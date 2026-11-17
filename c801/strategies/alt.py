"""Receipt-driven A787 lifecycle, with no exchange access or venue position view.

Source search is deliberately outside this book. Every parent carries its frozen
structural level and peer-turnover evidence. The complete batch (including gate
rejects) enters together. Confirmed protection is separate from requested
protection: unacknowledged amendments never release risk capacity.

Partial-fill handling extends the original all-or-nothing research venue: keep
the unfilled reservation until a terminal receipt, and only attempt a child
after the parent's entry quantity is final. This difference must be disclosed
in the final same-code execution replay.
"""
from copy import copy, deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal
import heapq
import math
from ..account import InvalidState
from ..market import CausalMarket
from .alt_risk import AltRisk, STEP

HOUR = 3_600_000
MINUTE = 60_000


def order_key(source):
    return (source['momentum_rank'], -source['known_quote24'], source['symbol'], source['id'])


def finite(value, *, positive=False):
    return type(value) in (int, float) and math.isfinite(value) and (not positive or value > 0)


@dataclass(frozen=True)
class AltCalendar:
    symbol: str
    known_ms: int
    no_new_ms: int
    exit_ms: int
    settlement_ms: int

    def validate(self):
        from ..account import PositionKey
        PositionKey(self.symbol, 'LONG')
        if any(type(t) is not int or t < 0 for t in (self.known_ms, self.no_new_ms, self.exit_ms, self.settlement_ms)):
            raise InvalidState('山寨下架日历时间无效')
        if not self.known_ms <= self.no_new_ms <= self.exit_ms < self.settlement_ms:
            raise InvalidState('山寨下架日历时序错误')

    def risk_record(self):
        return dict(modeled_schedule_known_ms=self.known_ms, no_new_position_ms=self.no_new_ms)


@dataclass(frozen=True)
class AltReceipt:
    event_id: str
    order_id: str
    kind: str
    event_ms: int
    received_ms: int
    available_ms: int
    quantity: float = 0.
    price: float = 0.
    fee: float = 0.
    amount: float = 0.  # funding only; fees and realized cash are derived
    interval_end_ms: int | None = None
    entry_finished: bool = False
    request_id: str | None = None
    stop: float = 0.
    target: float = 0.
    realized_pnl: float | None = None  # verified exchange cash; None retains the historical lot model
    protection_confirmed: bool | None = None  # live entry explicitly starts unconfirmed
    requested_stop: float | None = None
    requested_target: float | None = None
    covered_quantity: float | None = None

    def validate(self):
        if not self.event_id or not self.order_id or self.kind not in ('entry', 'exit', 'funding', 'entry_done', 'protection','initial_protection'):
            raise InvalidState('山寨回报标识或类型错误')
        if any(type(t) is not int for t in (self.event_ms, self.received_ms, self.available_ms)) or not 0 <= self.event_ms <= self.received_ms <= self.available_ms:
            raise InvalidState('山寨回报时间无效')
        if self.interval_end_ms is not None and (type(self.interval_end_ms) is not int or not self.event_ms <= self.interval_end_ms <= self.received_ms):
            raise InvalidState('山寨成交区间晚于回报到达')
        if any(not finite(x) for x in (self.quantity, self.price, self.fee, self.amount, self.stop, self.target)):
            raise InvalidState('山寨回报包含无效数值')
        if min(self.quantity, self.price) < 0 or type(self.entry_finished) is not bool:
            raise InvalidState('山寨回报数量、费用或终态错误')
        if self.realized_pnl is not None and (not finite(self.realized_pnl) or self.kind not in ('entry','exit') or self.kind=='entry' and self.realized_pnl!=0):
            raise InvalidState('真实已实现盈亏只能来自核实过的成交，开仓不能产生平仓盈亏')
        if self.kind in ('entry', 'exit'):
            if self.quantity <= 0 or self.price <= 0 or self.amount != 0:
                raise InvalidState('山寨成交缺少数量、价格或现金重复计入')
        elif self.quantity or self.price or self.fee:
            raise InvalidState('非成交回报不能携带成交数量或费用')
        if self.kind != 'funding' and self.amount != 0:
            raise InvalidState('非资金费回报不能直接修改现金')
        if self.kind in ('protection','initial_protection') and (not self.request_id or not 0 < self.stop < self.target):
            raise InvalidState('保护确认缺少对应请求或有效价格')
        if self.protection_confirmed is not None and (self.kind!='entry' or type(self.protection_confirmed) is not bool):raise InvalidState('开仓保护状态字段与回报类型不符')
        if (self.requested_stop is None)!=(self.requested_target is None):raise InvalidState('保护确认必须同时记录原请求的止损止盈')
        if self.requested_stop is not None and (self.kind not in ('protection','initial_protection') or not finite(self.requested_stop) or not finite(self.requested_target) or not 0<self.requested_stop<self.requested_target):raise InvalidState('保护确认的原计划价格无效')
        if self.covered_quantity is not None and (self.kind not in ('protection','initial_protection') or not finite(self.covered_quantity) or self.covered_quantity<=0):raise InvalidState('实际保护确认数量无效')


class AltBook:
    def __init__(self, *, start_ms, initial, cost=1):
        if type(start_ms) is not int or start_ms < 2 * STEP or start_ms % STEP:
            raise InvalidState('山寨账户起点错误')
        self.risk = AltRisk(initial, cost=cost)
        self.initial, self.cost = float(initial), cost
        self.known_cash = self.known_peak = self.cash_peak = float(initial)
        self.last_clock = start_ms - STEP
        self.orders, self.cycles, self.calendar = {}, {}, {}
        self.queue, self.receipts, self.deliveries = [], {}, []
        self.serial = 0
        self.requests, self.admissions, self.observations = [], [], []
        self.consumed_sources, self.add_attempted = set(), set()
        self.child_exit_requests, self.protection_requests, self.cancel_requested = {}, {}, set()
        self.calendar_exit_requested = set()

    def receive(self, receipt: AltReceipt):
        receipt.validate()
        if receipt.order_id not in self.orders:
            raise InvalidState('山寨回报无法对应已登记订单')
        payload = asdict(receipt)
        if receipt.event_id in self.receipts:
            if self.receipts[receipt.event_id] != payload:
                raise InvalidState('山寨同一回报标识的内容冲突')
            return False
        if receipt.available_ms <= self.last_clock:
            raise InvalidState('不能把晚到回报回填到已经决策的时刻')
        self.serial += 1
        heapq.heappush(self.queue, (receipt.available_ms, self.serial, payload))
        self.receipts[receipt.event_id] = payload
        return True

    def _deliver(self, r, now):
        o = self.orders[r['order_id']]
        p = self.cycles.get(o['id'])
        kind, q = r['kind'], r['quantity']
        if kind == 'entry':
            if o['entry_done'] or o['filled_quantity'] + q > o['quantity'] + 1e-10:
                raise InvalidState('山寨开仓回报超量或订单已经终结')
            if not o['intended_entry_ms'] <= r['event_ms'] < o['deadline_ms'] or r['price'] > o['fill_price_cap'] * (1 + 1e-12):
                raise InvalidState('山寨开仓回报超出委托时间或价格上限')
            if p is None:
                p = deepcopy(o)
                p.update(quantity=0., remaining=0., entry_fill=0., entry_ms=r['event_ms'], entry_received_ms=r['available_ms'],
                         initial_stop=o['stop'], peak_high=0., confirmed_stop=o['stop'], confirmed_target=o['tp2'],
                         closed=False, fees=0., realized=0., funding=0., exit_due_ms=None, conditional_exit_ms=None,protection_ready=True)
                self.cycles[o['id']] = p
            elif p.get('had_exit'):
                raise InvalidState('山寨减仓后又收到开仓成交，必须重新对账')
            if r.get('protection_confirmed') is not None:p['protection_ready']=r['protection_confirmed']
            total = p['quantity'] + q
            # Preserve a single fill exactly, including equality at the 1R/2R
            # boundaries. Decimal weighting avoids introducing q*p/q noise.
            if p['quantity'] == 0:
                p['entry_fill'] = r['price']
            else:
                oldq, newq = Decimal(str(p['quantity'])), Decimal(str(q))
                p['entry_fill'] = float((Decimal(str(p['entry_fill']))*oldq + Decimal(str(r['price']))*newq)/(oldq+newq))
            p['quantity'], p['remaining'] = total, p['remaining'] + q
            p['peak_high'] = max(p['peak_high'], r['price'])
            p['fees'] += r['fee']
            o['filled_quantity'] += q
            self.known_cash -= r['fee']
            if r['entry_finished']:
                o['entry_done'] = True
        elif kind == 'entry_done':
            o['entry_done'] = True
            if p is not None and p['remaining'] <= 1e-10:
                p['closed'] = True
        elif kind == 'exit':
            if p is None or p['closed'] or q > p['remaining'] + 1e-10 or r['event_ms'] < p['entry_ms']:
                raise InvalidState('山寨平仓回报无对应仓位或数量超限')
            realized = q * (r['price'] - p['entry_fill']) if r['realized_pnl'] is None else r['realized_pnl']
            p['remaining'] = max(0., p['remaining'] - q)
            p.update(had_exit=True, exit_ms=r['event_ms'], exit_interval_end_ms=r['interval_end_ms'] or r['event_ms'], exit_received_ms=r['available_ms'])
            p['fees'] += r['fee']
            p['realized'] += realized
            p['closed'] = p['remaining'] <= 1e-10 and o['entry_done']
            self.known_cash += realized - r['fee']
        elif kind == 'funding':
            if p is None or r['event_ms'] < p['entry_ms']:
                raise InvalidState('山寨资金费没有对应成交仓位')
            if p.get('closed') and r['event_ms'] > p['exit_interval_end_ms']:
                raise InvalidState('山寨仓位结束后发生新的资金费，需重新对账')
            p['funding'] += r['amount']
            self.known_cash += r['amount']
        elif kind in ('protection','initial_protection'):
            initial=kind=='initial_protection'
            request = dict(id=o['id'],decision_ms=o['decision_ms'],stop=o['stop'],target=o['tp2']) if initial else self.protection_requests.get(r['request_id'])
            if initial and r['request_id']!='initial:'+str(o['id']):raise InvalidState('初始保护确认的交易批次不符')
            if p is None or request is None or request['id'] != o['id'] or r['event_ms'] < request['decision_ms']:
                raise InvalidState('山寨保护确认没有对应请求')
            requested=(r.get('requested_stop'),r.get('requested_target'))
            if requested[0] is None:requested=(r['stop'],r['target'])
            if requested != (request['stop'], request['target']) or r['stop']<requested[0] or r['target']>requested[1]:
                raise InvalidState('山寨保护确认价格与请求不符')
            if p.get('protection_ready',True) and (r['stop'] < p['confirmed_stop'] or r['target'] < p['confirmed_target']):
                raise InvalidState('山寨保护确认发生倒退，需重新对账')
            p['confirmed_stop'], p['confirmed_target'] = r['stop'], r['target']
            p['protection_ready']=r.get('covered_quantity') is None or r['covered_quantity']+1e-10>=p['remaining']
            request['acknowledged_ms'] = now
        self.deliveries.append(dict(event_id=r['event_id'], id=o['id'], kind=kind, delivered_ms=now, known_cash=self.known_cash))

    def _bar(self, market, symbol, now, *, start_ms=None):
        end = now - STEP if start_ms is None else start_ms + STEP
        return market.window(symbol, STEP, end, 1, now_ms=now, lag_ms=STEP)[0]

    def snapshot(self, now, market):
        if type(now) is not int or now not in (self.last_clock,self.last_clock+STEP):
            raise InvalidState('山寨账户快照只能用于当前决策时钟')
        cycles, marks = deepcopy(self.cycles), {}
        effective_cash = self.known_cash
        for p in cycles.values():
            if p['remaining'] > 0:
                marks[p['symbol']] = float(self._bar(market, p['symbol'], now).close)
            # Desired protection is not exchange protection until acknowledged.
            p['stop'], p['tp2'] = p['confirmed_stop'], p['confirmed_target']
        pending = []
        fee, slip = .001 * self.cost, .002 * self.cost
        for cid, o in self.orders.items():
            if o['entry_done']:
                continue
            q = max(0., o['quantity'] - o['filled_quantity'])
            if not q:
                continue
            pending.append(cid)
            p = deepcopy(o)
            cap, fee_cash = o['fill_price_cap'], q * o['fill_price_cap'] * fee
            identity = cid if cid not in cycles else cid + '~unfilled'
            p.update(id=identity, quantity=q, remaining=q, entry_ms=o['decision_ms'], entry_fill=cap,
                     fees=fee_cash, realized=0., funding=0., closed=False, fee_estimate=fee/2, slip=slip/2)
            cycles[identity] = p
            effective_cash -= fee_cash
            marks[o['symbol']] = min(marks.get(o['symbol'], o['reference_price']), o['reference_price'])
        positions = [p for p in cycles.values() if not p['closed'] and p['remaining'] > 0]
        equity = effective_cash + sum(p['remaining'] * (marks[p['symbol']] - p['entry_fill']) for p in positions)
        self.known_peak = max(self.known_peak, equity)
        return dict(time_ms=now, known_cash=self.known_cash, effective_cash=effective_cash, equity=equity,
                    peak=self.known_peak, cycles=cycles, positions=positions, marks=marks, pending_ids=sorted(pending))

    def _request(self, kind, cid, now, **payload):
        row = dict(kind=kind, id=cid, request_id=f'A787:{len(self.requests)+1}:{cid}:{kind}', decision_ms=now, **payload)
        self.requests.append(row)
        return row

    def _parent_protection(self, now, market, *, symbols=None):
        for cid, p in self.cycles.items():
            if symbols is not None and p['symbol'] not in symbols:continue
            if p.get('is_add') or p['closed'] or p['remaining'] <= 0 or now-2*STEP < p['entry_ms']:
                continue
            b = self._bar(market, p['symbol'], now)
            old = p['stop'], p['tp2'], p['virtual_base_target']
            p['peak_high'] = max(p['peak_high'], float(b.high))
            distance = max(1e-12, p['entry_fill'] - p['initial_stop'])
            if p['peak_high'] >= p['entry_fill'] + distance:
                p['stop'] = max(p['stop'], p['entry_fill'])
                p['virtual_base_target'] = max(p['virtual_base_target'], p['entry_fill'] + 48*distance)
            p['tp2'] = p['virtual_base_target']
            if old != (p['stop'], p['tp2'], p['virtual_base_target']):
                row = self._request('PROTECTION_AMEND', cid, now, effective_ms=now, stop=p['stop'], target=p['tp2'], observed_bar_start_ms=b.start_ms)
                self.protection_requests[row['request_id']] = row

    def _trend(self, now, market, *, symbols=None):
        for cid, p in self.cycles.items():
            if symbols is not None and p['symbol'] not in symbols:continue
            if p.get('is_add') or p['closed'] or p['remaining'] <= 0 or p['conditional_exit_ms'] is not None:
                continue
            clocks = ((p['entry_ms'] + age*HOUR + STEP-1)//STEP*STEP for age in (48,72,96,120,144))
            if now not in clocks:
                continue
            recent = self._bar(market, p['symbol'], now)
            older = self._bar(market, p['symbol'], now, start_ms=now-2*STEP-24*HOUR)
            carry = recent.close > older.close and recent.quote_volume > 0 and older.quote_volume > 0
            self.observations.append(dict(kind='TREND', id=cid, now_ms=now, continue_holding=carry))
            if not carry:
                p['conditional_exit_ms'] = now
                self._request('EXIT_REQUEST', cid, now, effective_ms=now, reason='trend_checkpoint', priority=60)

    def _prepare_children(self, now, market, *, symbols=None):
        children = []
        for cid, p in self.cycles.items():
            if symbols is not None and p['symbol'] not in symbols:continue
            if p.get('is_add') or p['closed'] or p['remaining'] <= 0 or cid in self.add_attempted or now-2*STEP < p['entry_ms']:
                continue
            # A partially-filled parent's final quantity is not known yet.
            if not self.orders[cid]['entry_done'] or not p.get('protection_ready',True):
                continue
            distance = max(1e-12, p['entry_fill']-p['initial_stop'])
            if p['peak_high'] < p['entry_fill']+distance:
                continue
            self.add_attempted.add(cid)
            b = self._bar(market, p['symbol'], now)
            ref, stop = float(b.close), p['entry_fill']
            target = max(p['virtual_base_target'], p['entry_fill']+48*distance)
            event = self.calendar.get(p['symbol'])
            reason = ('announced_no_new_position_before_add_observation' if event and now >= event.no_new_ms
                      else 'no_time_for_add_before_parent_deadline' if now+STEP >= p['deadline_ms']
                      else 'invalid_add_geometry_or_zero_quote' if b.quote_volume <= 0 or not stop < ref < target else None)
            self.observations.append(dict(kind='ADD_ATTEMPT', id=cid, now_ms=now, reason=reason))
            if reason:
                continue
            children.append(dict(id=cid+'_ADD', parent_id=cid, is_add=True, symbol=p['symbol'], side=1,
                                 momentum_rank=p['momentum_rank'], leadership_event_hour=p['leadership_event_hour'], known_quote24=p['known_quote24'],
                                 signal_end_ms=now-STEP, decision_ms=now, intended_entry_ms=now+STEP, input_available_ms=now,
                                 reference_price=ref, stop=stop, tp2=target, virtual_base_target=target, fixed_T3=target,
                                 deadline_ms=p['deadline_ms'], original_parent_quantity=p['quantity'], target_arm='C0'))
        return children

    def _observe_exits(self, now, market, *, symbols=None):
        # Order matches A419 -> parent receipt -> A438 child floor -> A786 parent floor.
        for cid, p in self.cycles.items():
            if symbols is not None and p['symbol'] not in symbols:continue
            if p.get('is_add') or p['closed'] or p['remaining'] <= 0 or p['exit_due_ms'] is not None or now-2*STEP < p['entry_ms']:
                continue
            b = self._bar(market, p['symbol'], now)
            if float(b.close) <= p['structural_level']:
                self._latch_exit(p, now, 'breakout_failure')
        for cid, o in self.orders.items():
            if symbols is not None and o['symbol'] not in symbols:continue
            if not o.get('is_add') or cid in self.child_exit_requests:
                continue
            if self.cycles[o['parent_id']]['closed']:
                row = self._request('EXIT_REQUEST', cid, now, effective_ms=now+STEP, reason='parent_receipt_exit', priority=50)
                self.child_exit_requests[cid] = row
        for p in self.cycles.values():
            if symbols is not None and p['symbol'] not in symbols:continue
            if not p.get('is_add') or p['closed'] or p['remaining'] <= 0 or p['exit_due_ms'] is not None or now-2*STEP < p['entry_ms'] or now+STEP >= p['deadline_ms']:
                continue
            b = self._bar(market, p['symbol'], now)
            if b.quote_volume <= 0:
                continue
            entry, stop = Decimal(str(p['entry_fill'])), Decimal(str(p['initial_stop']))
            if p.get('child_floor_armed_ms') is None and b.high >= entry+(entry-stop):
                p['child_floor_armed_ms'] = now
            if p.get('child_floor_armed_ms') is not None and b.close <= entry:
                self._latch_exit(p, now, 'child_profit_floor')
        for p in self.cycles.values():
            if symbols is not None and p['symbol'] not in symbols:continue
            if p.get('is_add') or p['closed'] or p['remaining'] <= 0 or p['exit_due_ms'] is not None or p['conditional_exit_ms'] is not None or now+STEP >= p['deadline_ms']:
                continue
            # Preserve the original receipt delay, and also respect actual late arrival.
            earliest = max(p['entry_ms']//STEP*STEP+2*STEP, p['entry_received_ms'])
            if now-2*STEP < earliest:
                continue
            b = self._bar(market, p['symbol'], now)
            if b.quote_volume <= 0:
                continue
            entry, stop = Decimal(str(p['entry_fill'])), Decimal(str(p['initial_stop']))
            armed = p.get('parent_floor_armed_ms')
            if armed is None and b.close >= entry+2*(entry-stop):
                p['parent_floor_armed_ms'] = now
            elif armed is not None and now > armed and b.close <= entry+(entry-stop):
                self._latch_exit(p, now, 'parent_profit_floor')

    def _latch_exit(self, p, now, reason):
        p['exit_due_ms'] = now+STEP
        self._request('EXIT_REQUEST', p['id'], now, effective_ms=now+STEP, reason=reason, priority=40)

    def _validate_source(self, s, now):
        from ..account import PositionKey
        PositionKey(s['symbol'], 'LONG')
        if not isinstance(s['id'], str) or not s['id'] or '~' in s['id'] or s.get('is_add') or s.get('side',1) != 1:
            raise InvalidState('山寨母仓信号身份无效')
        if (s['decision_ms'], s['signal_end_ms'], s['intended_entry_ms']) != (now,now-STEP,now+STEP) or not s['signal_end_ms'] <= s['input_available_ms'] <= now:
            raise InvalidState('山寨信号未到决策时间')
        if any(not finite(s[k], positive=True) for k in ('reference_price','stop','tp2','virtual_base_target','fixed_T3','structural_level')):
            raise InvalidState('山寨信号价格无效')
        if not s['stop'] < s['reference_price'] < s['tp2'] or s.get('target_arm','C0') != 'C0' or s['tp2'] != s['virtual_base_target']:
            raise InvalidState('山寨母仓初始保护几何错误')
        if type(s['momentum_rank']) is not int or s['momentum_rank'] < 1 or type(s['leadership_event_hour']) is not int or not finite(s['known_quote24']) or s['known_quote24'] < 0:
            raise InvalidState('山寨信号排序或事件信息错误')
        if type(s['structural_available_ms']) is not int or s['structural_available_ms'] > now:
            raise InvalidState('结构失效价不能使用信号之后才知道的信息')
        gate = s['context']
        if gate['symbol'] != s['symbol'] or gate['decision_ms'] != now or gate['input_available_ms'] > now:
            raise InvalidState('山寨同伴成交额证据身份或时序错误')
        total, positive = gate['peer_total_quote24'], gate['peer_positive_quote24']
        if not finite(total) or not finite(positive) or not 0 <= positive <= total or type(gate['unknown']) is not bool:
            raise InvalidState('山寨同伴成交额证据无效')
        expected = not gate['unknown'] and total > 0 and 2*positive > total
        if type(gate['passed']) is not bool or gate['passed'] != expected:
            raise InvalidState('山寨同伴成交额门控与原始证据不符')
        audit = s.get('feature_audit', {})
        if audit.get('feature_last_close_ms', now-STEP-1) >= now-STEP:
            raise InvalidState('山寨信号含尚未结束的特征窗口')

    def _admit(self, s, now, market, members):
        snapshot = self.snapshot(now, market)
        event = self.calendar.get(s['symbol'])
        reason = ('announced_no_new_position_before_reservation' if event and now >= event.no_new_ms
                  else 'known_parent_closed_before_add_reservation' if s.get('is_add') and self.cycles[s['parent_id']]['closed']
                  else 'other_alt_turnover_not_majority' if not s.get('is_add') and not s['context']['passed'] else None)
        if reason:
            result = dict(reason=reason)
        else:
            result = self.risk.consider(s, snapshot, self._bar(market,s['symbol'],now), now_ms=now, cash_peak=self.cash_peak,
                                        simultaneous_ids=members, lifecycle=event.risk_record() if event else None)
        self.admissions.append(dict(id=s['id'], time_ms=now, result=deepcopy(result)))
        if result['reason'] is not None:
            return
        plan = result['plan']
        order = deepcopy(s)
        order.update(plan, quantity=result['quantity'], entry_ms=s['intended_entry_ms'], entry_done=False, filled_quantity=0.,
                     deadline_ms=s['deadline_ms'] if s.get('is_add') else s['intended_entry_ms']+168*HOUR,
                     fee_estimate=.0005*self.cost, slip=.001*self.cost, effective_risk_fraction=result['effective_risk_fraction'],
                     event_reserved_loss=result['quantity']*plan['worst_stop_loss_per_unit'])
        if order['id'] in self.orders:
            raise InvalidState('山寨母仓与加仓订单标识冲突')
        self.orders[order['id']] = order
        self._request('ENTRY_PLAN', order['id'], now, effective_ms=order['intended_entry_ms'], order=deepcopy(order))

    def _transaction_snapshot(self):
        """Copy mutable scalar rows, preserving immutable source evidence.

        All nested source/context and committed risk result payloads are frozen
        after admission. Protection acknowledgments do edit prior request rows;
        preserve their aliases in the request list and protection mapping.
        """
        state=self.__dict__.copy()
        tails={k:len(state[k]) for k in ('deliveries','admissions','observations')}
        for k in ('orders','cycles'):
            state[k]={identity:dict(row) for identity,row in state[k].items()}
        rows={id(r):dict(r) for r in self.requests}
        state['requests']=[rows[id(r)] for r in self.requests]
        for k in ('child_exit_requests','protection_requests'):
            state[k]={identity:rows.setdefault(id(r),dict(r)) for identity,r in state[k].items()}
        state['calendar']=dict(self.calendar);state['queue']=list(self.queue)
        for k in ('consumed_sources','add_attempted','cancel_requested','calendar_exit_requested'):
            state[k]=state[k].copy()
        risk=copy(self.risk)
        # Existing risk rows are immutable. consider() adds new rows or replaces
        # scalar commitments; it never edits an existing nested risk result.
        risk.__dict__={k:dict(v) if isinstance(v,dict) else v for k,v in self.risk.__dict__.items()}
        state['risk']=risk
        return state,tails

    def _transaction_restore(self,snapshot):
        state,tails=snapshot
        for k,n in tails.items():del state[k][n:]
        self.__dict__.clear();self.__dict__.update(state)

    def advance(self, now_ms, market: CausalMarket, *, sources=(), calendars=()):
        if type(now_ms) is not int or now_ms != self.last_clock+STEP:
            raise InvalidState('山寨控制时钟必须连续推进，重启需先重建缺失事件')
        sources = tuple(deepcopy(s) for s in sources)
        for s in sources:
            self._validate_source(s, now_ms)
        ids = [s['id'] for s in sources]
        if len(ids) != len(set(ids)) or set(ids) & self.consumed_sources or set(ids) & set(self.orders):
            raise InvalidState('山寨来源重复消费或身份冲突')
        before = self._transaction_snapshot()
        try:
            first_request = len(self.requests)
            for event in calendars:
                event.validate()
                if event.known_ms > now_ms:
                    raise InvalidState('山寨日历公告尚未收到')
                if event.symbol in self.calendar and self.calendar[event.symbol] != event:
                    raise InvalidState('山寨日历修订需要暂停并核对')
                self.calendar[event.symbol] = event
            while self.queue and self.queue[0][0] <= now_ms:
                self._deliver(heapq.heappop(self.queue)[2], now_ms)
            self.cash_peak = max(self.cash_peak, self.known_cash)
            for cid, o in self.orders.items():
                event = self.calendar.get(o['symbol'])
                if event and now_ms >= event.settlement_ms and (not o['entry_done'] or cid in self.cycles and not self.cycles[cid]['closed']):
                    raise InvalidState('山寨自动结算时仍有未核实仓位或订单，需对账')
                if event and now_ms >= event.no_new_ms and not o['entry_done'] and cid not in self.cancel_requested:
                    self._request('CANCEL_ENTRY', cid, now_ms, effective_ms=now_ms, reason='announced_no_new_position')
                    self.cancel_requested.add(cid)  # Reservation remains until a terminal receipt.
                if event and cid in self.cycles and self.cycles[cid]['remaining'] > 0 and cid not in self.calendar_exit_requested:
                    self._request('EXIT_REQUEST', cid, now_ms, effective_ms=max(now_ms,event.exit_ms),
                                  reason='announced_pre_settlement_exit', priority=30)
                    self.calendar_exit_requested.add(cid)
            self._parent_protection(now_ms, market)
            self._trend(now_ms, market)
            children = self._prepare_children(now_ms, market)
            self._observe_exits(now_ms, market)
            parents = sorted(sources, key=order_key)
            members = tuple(s['id'] for s in parents)
            for s in parents:
                self._admit(s, now_ms, market, members)
                self.consumed_sources.add(s['id'])
            for s in sorted(children, key=order_key):
                self._admit(s, now_ms, market, ())
            self.last_clock = now_ms
            return deepcopy(self.requests[first_request:])
        except BaseException:
            self._transaction_restore(before)
            raise

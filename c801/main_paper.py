"""Original M784 shadow venue; no independent supervisor or replay runner."""
from copy import deepcopy
import math
from decimal import Decimal as D
from .account import InvalidState
from .strategies.main import MainBook,MainReceipt,CLOCK,MINUTE,SYMS,HOLD


class MainPaperVenue:
    def __init__(self, book: MainBook):
        self.book = book
        self.actual_cash = book.initial
        self.orders = {}
        self.positions = {}
        self.trades = {}
        self.requests = []
        self.attempts = []
        self.funding_records = []
        self.equity = []
        self.last_minute = book.start_ms - MINUTE
        self.serial = 0

    def _entry_quantity(self,o,*,t,equity,limit,margin):
        return o['qty']

    def dispatch(self, actions):
        for action in actions:
            kind = action['action']
            if kind == 'ENTRY_PLAN':
                o = deepcopy(action['order'])
                if o['id'] in self.orders:
                    raise InvalidState('模拟订单被重复发送')
                o['status'] = 'pending'
                self.orders[o['id']] = o
            elif kind == 'STOP_AMEND':
                self.requests.append(dict(deepcopy(action), applied_ms=None))
            elif kind == 'ENTRY_AMEND':
                oid = action['order_id']
                if oid not in self.orders:
                    raise InvalidState('更新请求没有对应模拟订单')
                self.orders[oid]['refresh'] = deepcopy(action)
            else:
                raise InvalidState('模拟执行器收到未知动作')

    def _cash_state(self, bars, field):
        equity, gross, margin = self.actual_cash, 0., 0.
        for p in self.positions.values():
            price = float(getattr(bars[p['symbol']], field))
            equity += p['side']*p['qty']*(price-p['entry_fill'])
            gross += p['qty']*price
            margin += p['qty']*p['raw_entry']/p['lev']
        return equity, gross, margin

    def _receipt(self, oid, kind, event_ms, received_ms, available_ms, *, quantity=0., price=0., raw=0., delta=0., settled=False):
        self.serial += 1
        self.book.receive(MainReceipt(f'paper:{self.book.mode}:{self.serial}', oid, kind, event_ms,
                                      max(event_ms, received_ms), max(event_ms, received_ms, available_ms),
                                      quantity, price, raw, delta, kind == 'entry', settled))

    def _close(self, oid, t, price, reason, phase, bars, *, receipt_floor_ms=None):
        p = self.positions.pop(oid)
        fee, slip = .0005*self.book.cost, .0004*self.book.cost
        fill = price*(1-p['side']*slip)
        commission = p['qty']*fill*fee
        delta = p['qty']*p['side']*(fill-p['entry_fill'])-commission
        self.actual_cash += delta
        upper = t+MINUTE
        p.update(exit_ms=upper, exit_lower_ms=t, exit_upper_ms=upper, exit_fill=fill, raw_exit=price,
                 exit_fee=commission, exit_type=reason, exit_phase=phase, status='closed')
        self._receipt(oid, 'exit', upper, max(bars[p['symbol']].received_ms,receipt_floor_ms or 0), upper+CLOCK,
                      quantity=p['qty'], price=fill, raw=price, delta=delta, settled=True)

    def minute(self, t, bars, *, funding=(), received_ms=None, receipt_floor_ms=None):
        if t != self.last_minute+MINUTE or set(bars) != set(SYMS):
            raise InvalidState('模拟执行分钟或币种不连续')
        if receipt_floor_ms is not None and (type(receipt_floor_ms) is not int or type(received_ms) is not int
                or not t+MINUTE<=receipt_floor_ms<=received_ms):
            raise InvalidState('模拟恢复回报必须保留实际处理时间')
        for symbol, bar in bars.items():
            if bar.symbol != symbol or bar.start_ms != t or bar.period_ms != MINUTE:
                raise InvalidState('模拟执行行情时间不匹配')
        # Clock decisions must have happened before this minute's future OHLC is
        # made visible to the venue model; completed-bar events come afterwards.
        if (t-self.book.start_ms) % CLOCK == 0 and self.book.last_clock != t:
            raise InvalidState('必须先执行当时可知的策略决策，再处理本分钟行情')
        for event in funding:
            observed_mark = ('mark_event_ms' in event and event['mark_event_ms']==event['time_ms']
                and type(received_ms) is int and type(event.get('received_ms')) is int
                and event['time_ms']<=event['mark_known_ms']<=event['received_ms']<=received_ms)
            if (event['symbol'] not in SYMS or not t <= event['time_ms'] < t+MINUTE
                    or event['mark_known_ms'] > event['time_ms'] and not observed_mark):
                raise InvalidState('资金费时间或参考价尚不可知')
            if not (math.isfinite(event['rate']) and math.isfinite(event['mark']) and event['mark'] > 0):
                raise InvalidState('资金费数值无效')
        fee, slip = .0005*self.book.cost, .0004*self.book.cost
        observed = set(self.positions)
        for oid, p in list(self.positions.items()):
            bar = bars[p['symbol']]
            opening, printed, side = float(bar.open), bar.quote_volume > 0, p['side']
            if printed:
                if side*(opening-p['active_stop']) <= 0:
                    self._close(oid,t,opening,'profit_lock' if p['active_stop']!=p['initial_stop'] else 'stop','open',bars,receipt_floor_ms=receipt_floor_ms);continue
                if side*(opening-p['initial_target']) >= 0:
                    self._close(oid,t,p['initial_target'],'target','open',bars,receipt_floor_ms=receipt_floor_ms);continue
                if t >= p['planned_ms']+HOLD:
                    self._close(oid,t,opening,'time','open',bars,receipt_floor_ms=receipt_floor_ms);continue
            for request in self.requests:
                if request['order_id']==oid and request['applied_ms'] is None and request['effective_ms']<=t:
                    p['active_stop']=request['new_stop'];request['applied_ms']=t
            if printed and side*(opening-p['active_stop']) <= 0:
                self._close(oid,t,opening,'profit_lock','open',bars,receipt_floor_ms=receipt_floor_ms)
        for oid,o in self.orders.items():
            if o['status']!='pending' or o['planned_ms']>t or (t-self.book.start_ms)%CLOCK:
                continue
            if t>=o['expiry_ms']:
                o.update(status='rejected',reject_reason='limit_expired')
                self.attempts.append(dict(order=oid,time_ms=t,action='limit_expired'))
                self._receipt(oid,'entry_done',t,max(t,receipt_floor_ms or 0),t+CLOCK,settled=True)
                continue
            bar=bars[o['symbol']];raw=float(bar.open);side=o['side'];fill=raw*(1+side*slip)
            change=o.get('refresh')
            limit=change['new_limit'] if change and change['eligible'] and t>change['effect_ms'] else o['fixed_limit']
            equity,_,margin=self._cash_state(bars,'open')
            quantity=o['qty']
            favorable = (self.book.mode!='shadow' and side*(raw-o['reference'])<0 and side*(fill-limit)<=0
                         and side*(fill-o['initial_stop'])>0 and side*(o['initial_target']-fill)>0)
            reason=('unprinted_execution' if bar.quote_volume<=0
                    else 'gap_outside_precommitted_bracket' if side*(raw-o['initial_stop'])<=0 or side*(o['initial_target']-raw)<=0
                    else 'gap_exceeds_cap' if abs(raw-o['reference'])>o['entry_cap_distance'] and not favorable else None)
            if reason is None and side*(fill-limit)>0:
                self.attempts.append(dict(order=oid,time_ms=t,action='wait_limit'));continue
            if reason is None and o['family']=='weekly' and not o['initial_stop']<fill<o['initial_target']:
                reason='slipped_fill_outside_bracket'
            if reason is None:
                quantity=self._entry_quantity(o,t=t,equity=equity,limit=limit,margin=margin)
                if quantity<=0:reason='stop_risk_budget'
            commission=quantity*fill*fee
            if reason is None and (equity<=0 or margin+quantity*raw/o['lev']>equity-commission):
                reason='actual_margin'
            self.attempts.append(dict(order=oid,time_ms=t,action=reason or 'filled'))
            if reason:
                o.update(status='rejected',reject_reason=reason)
                self._receipt(oid,'entry_done',t+MINUTE,max(bar.received_ms,receipt_floor_ms or 0),t+MINUTE+CLOCK,settled=True)
                continue
            p=dict(deepcopy(o),qty=quantity,entry_ms=t,entry_upper_ms=t+MINUTE,entry_fill=fill,raw_entry=raw,
                   entry_fee=commission,active_stop=o['initial_stop'],funding=0.,status='open',exit_ms=None)
            self.positions[oid]=p;self.trades[oid]=p;o['status']='filled';observed.add(oid)
            self.actual_cash-=commission
            self._receipt(oid,'entry',t+MINUTE,max(bar.received_ms,receipt_floor_ms or 0),t+MINUTE+CLOCK,
                          quantity=quantity,price=fill,raw=raw,delta=-commission)
        for oid,p in list(self.positions.items()):
            bar=bars[p['symbol']]
            if bar.quote_volume<=0:continue
            low,high=float(bar.low),float(bar.high)
            if low<=p['active_stop'] if p['side']>0 else high>=p['active_stop']:
                self._close(oid,t,p['active_stop'],'profit_lock' if p['active_stop']!=p['initial_stop'] else 'stop','intrabar',bars,receipt_floor_ms=receipt_floor_ms)
            elif high>=p['initial_target'] if p['side']>0 else low<=p['initial_target']:
                self._close(oid,t,p['initial_target'],'target','intrabar',bars,receipt_floor_ms=receipt_floor_ms)
        for event in sorted(funding,key=lambda e:(e['time_ms'],e['symbol'])):
            ft,symbol,rate,mark=event['time_ms'],event['symbol'],event['rate'],event['mark']
            for oid in sorted(observed):
                p=self.trades[oid]
                if p['symbol']!=symbol or p['entry_ms']>ft or p['exit_ms'] is not None and ft>p['exit_upper_ms']:continue
                ambiguous=ft<p['entry_upper_ms'] or p.get('exit_lower_ms')==t
                signed=-p['side']*p['qty']*mark*rate
                flow=min(0.,signed) if ambiguous else signed
                p['funding']+=flow;self.actual_cash+=flow
                self._receipt(oid,'funding',ft,max(ft,bars[symbol].received_ms,event.get('received_ms',ft),receipt_floor_ms or 0),ft+CLOCK,delta=flow)
                self.funding_records.append(dict(order=oid,event_ms=ft,rate=rate,mark=mark,ambiguous=ambiguous,flow=flow))
        equity,gross,_=self._cash_state(bars,'close')
        if self.positions and equity<=.005*gross and all(bars[p['symbol']].quote_volume>0 for p in self.positions.values()):
            for oid,p in list(self.positions.items()):self._close(oid,t,float(bars[p['symbol']].close),'maintenance_proxy','close',bars,receipt_floor_ms=receipt_floor_ms)
        equity,_,_=self._cash_state(bars,'close')
        self.equity.append((t+MINUTE,equity));self.last_minute=t
        return dict(time_ms=t+MINUTE,equity=equity,actual_cash=self.actual_cash,open_positions=len(self.positions))

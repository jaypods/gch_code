"""Original strategy actions and actual exchange fills, without a supervisor.

Pure transformations only: this module cannot send an order. A durable sender
must freeze each returned request before sending it and query uncertain sends
under the same client identity. Quotes, account funds and exchange filters are
execution inputs; chart readiness and historical audit status are not inputs.
"""
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal as D, ROUND_FLOOR, ROUND_CEILING
import hashlib
import json

from .account import (InvalidState, PositionKey, number, positive, nonnegative,
                      sleeve_free_capital)

MINUTE = 60000
FIVE = 5 * MINUTE
KINDS = {
    'M784': {'ENTRY_PLAN', 'ENTRY_AMEND', 'STOP_AMEND'},
    'A787': {'ENTRY_PLAN', 'PROTECTION_AMEND', 'EXIT_REQUEST', 'CANCEL_ENTRY'},
    'S800': {'ENTRY_PLAN', 'PROTECT_HALF', 'EXIT_REQUEST', 'CANCEL_ENTRY'},
}


def stamp(value):
    if type(value) is not int or value < 0:
        raise InvalidState('时间必须是非负毫秒整数')
    return value


def identity(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
                     separators=(',', ':'), allow_nan=False,
                     default=lambda v: str(v) if isinstance(v, D) else _bad(v))
    return 'c801' + hashlib.sha256(raw.encode()).hexdigest()[:32]


def _bad(value):
    raise InvalidState('委托包含不支持的值：' + type(value).__name__)


def normalize_action(sleeve, action, *, now_ms, first_entry_ms=None):
    """Keep strategy times intact, including M784's strict refresh boundary."""
    stamp(now_ms)
    a = deepcopy(action)
    kind = a.get('action', a.get('kind'))
    if kind not in KINDS.get(sleeve, ()):
        raise InvalidState('未知策略动作')
    o = a.get('order', {})
    oid = o.get('id') if kind == 'ENTRY_PLAN' else a.get('order_id', a.get('id'))
    if sleeve == 'A787':
        if not isinstance(oid, str) or not oid:
            raise InvalidState('山寨批次缺少标识')
    elif type(oid) is not int or oid < (0 if sleeve == 'M784' else 1):
        raise InvalidState('策略批次标识错误')
    decision = stamp(a.get('decision_ms', a.get('request_ms', o.get('decision_ms'))))
    known = stamp(a.get('known_ms', decision))
    if not decision <= known <= now_ms:
        raise InvalidState('策略动作尚未产生或收到')
    result = dict(sleeve=sleeve, kind=kind, strategy_order_id=oid,
                  lot_id=sleeve + ':' + json.dumps(oid, ensure_ascii=False),
                  decision_ms=decision, known_ms=known, action=a)
    if kind == 'ENTRY_PLAN':
        PositionKey(o['symbol'], 'BOTH')
        if type(o.get('side')) is not int or o['side'] not in (-1, 1):
            raise InvalidState('策略方向错误')
        if sleeve == 'A787' and o['side'] != 1:
            raise InvalidState('原山寨策略仅做多')
        active = stamp(o.get('planned_ms', o.get('intended_entry_ms', o.get('active_ms'))))
        expiry = stamp(o.get('expiry_ms', o.get('deadline_ms')))
        if not decision < active < expiry or active % (MINUTE if sleeve == 'S800' else FIVE):
            raise InvalidState('原策略入场窗口错误')
        result.update(due_ms=active, expiry_ms=expiry,
                      current_limit=str(positive(o.get('fixed_limit', o.get('fill_price_cap', o.get('limit'))))))
    else:
        due = stamp(a['effect_ms']) + FIVE if kind == 'ENTRY_AMEND' else stamp(a.get('effective_ms', known))
        if due < decision:
            # The original stock deadline remains anchored to the first actual
            # fill, even if receipt delivery discovers it after that deadline.
            if not (sleeve == 'S800' and kind == 'EXIT_REQUEST' and a.get('reason') == 'deadline'
                    and first_entry_ms is not None and due == stamp(first_entry_ms) + 1440 * MINUTE):
                raise InvalidState('动作不能追溯到决策前生效')
            due = decision
        result['due_ms'] = max(due, known)
    result['id'] = identity({'sleeve': sleeve, 'action': a})
    return result


def entry_attempt(plan, due_ms):
    stamp(due_ms)
    if plan['kind'] != 'ENTRY_PLAN' or not plan['due_ms'] <= due_ms < plan['expiry_ms']:
        raise InvalidState('入场尝试不在原计划有效期')
    if due_ms != plan['due_ms'] and (plan['sleeve'] != 'M784' or (due_ms - plan['due_ms']) % FIVE):
        raise InvalidState('只有主流币可按原策略每五分钟重新检查限价')
    return dict(id=identity([plan['id'], 'ENTRY', due_ms]), due_ms=due_ms,
                expires_ms=min(due_ms + MINUTE, plan['expiry_ms']))


@dataclass(frozen=True)
class Quote:
    symbol: str
    event_ms: int
    received_ms: int
    bid: D
    ask: D
    request_ms: int | None = None

    def __post_init__(self):
        PositionKey(self.symbol, 'BOTH')
        if not stamp(self.event_ms) <= stamp(self.received_ms):
            raise InvalidState('报价时间倒退')
        if self.request_ms is not None and stamp(self.request_ms) > self.received_ms:
            raise InvalidState('报价查询晚于接收')
        for name in ('bid', 'ask'):
            object.__setattr__(self, name, positive(getattr(self, name)))
        if self.bid > self.ask:
            raise InvalidState('买卖报价交叉')

    @property
    def freshness_ms(self):
        return max(self.event_ms, self.request_ms or 0)


@dataclass(frozen=True)
class SleeveFunds:
    cash: D
    equity: D
    held: D

    def __post_init__(self):
        object.__setattr__(self, 'cash', number(self.cash))
        object.__setattr__(self, 'equity', number(self.equity))
        object.__setattr__(self, 'held', nonnegative(self.held))

    @property
    def available(self):
        return sleeve_free_capital(self.cash, self.equity, self.held)


def tick_round(value, rules, up):
    return rules.minimum_price + ((positive(value) - rules.minimum_price) / rules.tick).to_integral_value(
        rounding=ROUND_CEILING if up else ROUND_FLOOR) * rules.tick


def prepare_entry(plan, attempt, *, snapshot, funds, rules, quote,
                  installed_leverage, owned_positions, owned_orders,
                  now_ms, pending_margin=0, fee_rate=D('.002'), alt_positions=()):
    """Size down from the original plan using funds actually available now.

    pending_margin contains only local reservations not yet reflected by the
    account snapshot. Exchange margin is already subtracted by free_capital().
    Ownership is derived from actual fills, never inferred from an ID prefix.
    """
    stamp(now_ms)
    if attempt != entry_attempt(plan, attempt['due_ms']):
        raise InvalidState('入场尝试身份或有效期不匹配')
    if not max(attempt['due_ms'], plan['known_ms']) <= now_ms < attempt['expires_ms']:
        return dict(result='MISSED_WINDOW')
    o = plan['action']['order']; s = plan['sleeve']; side = o['side']
    key = PositionKey(o['symbol'], ('LONG' if side == 1 else 'SHORT') if snapshot.hedge_mode else 'BOTH')
    if not isinstance(quote, Quote) or quote.symbol != key.symbol or not (
            attempt['due_ms'] <= quote.freshness_ms <= quote.received_ms <= now_ms
            and now_ms - quote.freshness_ms <= 5000):
        raise InvalidState('需要本次入场窗口内的可成交报价')
    if not 0 <= now_ms - snapshot.received_ms <= 5000 or rules.key != key or not 0 <= now_ms - rules.observed_ms <= 3600000:
        raise InvalidState('账户或交易规则不是当前可用记录')
    if not snapshot.can_trade or snapshot.multi_asset_mode:
        return dict(result='ACCOUNT_MODE_UNSUPPORTED')
    actual = next((p.amount for p in snapshot.positions if p.key == key), D(0))
    if actual != number(owned_positions.get(key, 0)) or any(
            w.key == key and w.client_id not in owned_orders for w in snapshot.orders):
        return dict(result='SAME_POSITION_NOT_OWNED')
    if actual and side * actual < 0:
        return dict(result='OPPOSITE_POSITION')
    lev = o['lev'] if s == 'M784' else o['leverage'] if s == 'A787' else 3
    if type(lev) is not int or type(installed_leverage) is not int or installed_leverage != lev or not 1 <= lev <= rules.maximum_leverage:
        raise InvalidState('已设置杠杆与原计划不符')
    raw = quote.ask if side == 1 else quote.bid
    limit = tick_round(plan['current_limit'], rules, side == -1)
    stop = tick_round(o['initial_stop'] if s == 'M784' else o['stop'], rules, side == 1)
    target = tick_round(o['initial_target'] if s == 'M784' else o['tp2'] if s == 'A787' else o['target'], rules, side == -1)
    if not rules.tradable or not rules.entry_allowed:
        return dict(result='NOT_TRADABLE')
    if not all(p > 0 and rules.price_allowed(p) for p in (limit, stop, target)):
        return dict(result='PRICE_FILTER')
    if rules.entry_price_issues('BUY' if side == 1 else 'SELL', limit, now_ms=now_ms, max_age_ms=5000):
        return dict(result='EXCHANGE_ENTRY_FILTER')
    if min(side * (raw - stop), side * (target - raw), side * (limit - stop), side * (target - limit)) <= 0:
        return dict(result='BRACKET_BREACHED')
    if s == 'M784':
        ref, distance = positive(o['reference']), positive(o['entry_cap_distance'])
        if abs(raw - ref) > distance and side * (raw - ref) > 0:
            return dict(result='CAP_BREACHED')
        if side * (raw - limit) > 0:
            return dict(result='WAIT_LIMIT', next_due_ms=(attempt['due_ms'] + FIVE
                        if attempt['due_ms'] + FIVE < plan['expiry_ms'] else None))
    elif s == 'A787' and (raw > positive(o['raw_price_cap']) or raw > limit):
        return dict(result='CAP_BREACHED')
    requested = positive(o['qty'] if s == 'M784' else o['quantity'])
    available = min(funds.available, max(D(0), snapshot.free_capital() - nonnegative(pending_margin)))
    qty = rules.floor_quantity(requested, limit, available, lev, fee_rate)
    if not qty:
        return dict(result='BELOW_MINIMUM' if available else 'INSUFFICIENT_BUDGET')
    if s == 'A787':
        # This is original A787 admission, not the removed global supervisor.
        from .alt_paper import entry_check
        positions = list(alt_positions)
        opens = {}
        for p in positions:
            pkey = PositionKey(p['symbol'], 'LONG' if snapshot.hedge_mode else 'BOTH')
            observed = next((v for v in snapshot.positions if v.key == pkey and v.amount > 0), None)
            if observed is None or observed.amount != number(owned_positions.get(pkey, 0)):
                raise InvalidState('山寨风险计算缺少实际持仓对应关系')
            opens[p['symbol']] = float(observed.notional / observed.amount)
        for symbol in opens:
            pkey = PositionKey(symbol, 'LONG' if snapshot.hedge_mode else 'BOTH')
            if abs(sum(number(p['quantity']) for p in positions if p['symbol'] == symbol)
                   - number(owned_positions[pkey])) > D('1e-10') * max(D(1), abs(number(owned_positions[pkey]))):
                raise InvalidState('山寨实际持仓未完整传入风险计算')
        opens[o['symbol']] = float(raw)
        mark_pnl = sum(p['quantity'] * (opens[p['symbol']] - p['entry_fill']) for p in positions)
        resized = o | dict(quantity=float(qty), stop=float(stop), tp2=float(target))
        admission = entry_check(resized, float(raw), float(min(funds.cash, funds.equity)),
                                float(funds.equity) - mark_pnl, positions, opens,
                                o['known_quote24'], float(o['slip']) / .002)
        if admission['reason'] is not None:
            return dict(result='STRATEGY_ADMISSION', reason=admission['reason'])
    expires = min(attempt['expires_ms'], quote.freshness_ms + 5001)
    if now_ms >= expires:
        return dict(result='MISSED_WINDOW')
    args = dict(symbol=key.symbol, positionSide=key.side, side='BUY' if side == 1 else 'SELL',
                type='LIMIT', timeInForce='GTC' if s == 'S800' else 'IOC',
                quantity=str(qty), price=str(limit), newClientOrderId=attempt['id'], newOrderRespType='ACK')
    return dict(result='PREPARED', id=attempt['id'], sleeve=s, lot_id=plan['lot_id'],
                strategy_order_id=plan['strategy_order_id'], side=side, leverage=lev,
                arguments=args, quantity=str(qty), stop=str(stop), target=str(target),
                reserved_margin=str(qty * limit * (D(1) / lev + nonnegative(fee_rate))),
                created_ms=now_ms, expires_ms=expires, resized=qty < requested)


def protection_request(entry, *, role, quantity, trigger, rules, generation,
                       now_ms, previous_stop=None):
    """Protect only an actual filled quantity; no ACK is counted as a fill."""
    if role not in ('SL', 'TP', 'HALF_SL') or type(generation) is not int or generation < 0:
        raise InvalidState('保护委托角色或版本错误')
    stamp(now_ms)
    args = entry['arguments']; side = entry['side']
    if rules.key != PositionKey(args['symbol'], args['positionSide']):
        raise InvalidState('保护委托合约规则不匹配')
    q = (positive(quantity) / rules.step).to_integral_value(rounding=ROUND_FLOOR) * rules.step
    if q <= 0 or q > positive(entry['quantity']) or q > rules.maximum_quantity:
        raise InvalidState('保护数量必须来自本批实际成交且不可超过计划')
    price = tick_round(trigger, rules, (side == 1) == (role != 'TP'))
    if price <= 0 or not rules.price_allowed(price):
        raise InvalidState('保护触发价不符合交易所精度')
    if role != 'TP' and previous_stop is not None and side * (price - positive(previous_stop)) < 0:
        raise InvalidState('原策略不允许放宽已有止损')
    client = identity([entry['id'], role, generation, str(q), str(price)])
    body = dict(algoType='CONDITIONAL', symbol=args['symbol'], positionSide=args['positionSide'],
                side='SELL' if side == 1 else 'BUY', type='TAKE_PROFIT_MARKET' if role == 'TP' else 'STOP_MARKET',
                quantity=str(q), triggerPrice=str(price), workingType='CONTRACT_PRICE',
                priceProtect='false', closePosition='false', clientAlgoId=client)
    if args['positionSide'] == 'BOTH':
        body['reduceOnly'] = 'true'
    return dict(id=client, operation='SEND_PROTECTION', arguments=body,
                lot_id=entry['lot_id'], role=role, created_ms=now_ms)


def receipt_available(sleeve, kind, event_ms, received_ms, prior_available_ms=0):
    stamp(event_ms); stamp(received_ms); stamp(prior_available_ms)
    if event_ms > received_ms:
        raise InvalidState('不能提前收到未来成交')
    if sleeve == 'M784':
        bound = (event_ms // MINUTE * MINUTE + MINUTE + FIVE
                 if kind in ('entry', 'exit') else event_ms + FIVE)
    elif sleeve == 'A787':
        bound = event_ms // FIVE * FIVE + 2 * FIVE
    elif sleeve == 'S800':
        bound = event_ms // MINUTE * MINUTE + 2 * MINUTE
    else:
        raise InvalidState('回报策略错误')
    return max(received_ms, bound, prior_available_ms)


def fill_receipt(plan, fill, *, purpose, received_ms, prior_available_ms=0):
    """An actual user-trade record, never an order ACK or a candle estimate."""
    from .strategies.main import MainReceipt
    from .strategies.alt import AltReceipt
    from .strategies.stock import StockReceipt
    if purpose not in ('ENTRY', 'EXIT') or fill['symbol'] != plan['action']['order']['symbol']:
        raise InvalidState('成交用途或合约不匹配')
    side = plan['action']['order']['side'] * (1 if purpose == 'ENTRY' else -1)
    if fill['side'] != ('BUY' if side == 1 else 'SELL'):
        raise InvalidState('成交方向与批次用途不符')
    qty, price = positive(fill['quantity']), positive(fill['price'])
    fee, pnl = number(fill['fee']), number(fill['realized_pnl'])
    if fee and fill['fee_asset'] != 'USDT':
        raise InvalidState('非 USDT 手续费需要实际折算记录')
    if purpose == 'ENTRY' and pnl:
        raise InvalidState('开仓成交不能混入其他持仓平仓盈亏')
    if not str(fill['trade_id']) or not str(fill['order_id']):
        raise InvalidState('缺少交易所实际成交身份')
    s = plan['sleeve']; kind = purpose.lower(); event_ms = stamp(fill['time_ms'])
    fields = dict(event_id=identity([s, fill['symbol'], str(fill['trade_id'])]),
                  order_id=plan['strategy_order_id'], kind=kind, event_ms=event_ms,
                  received_ms=received_ms, available_ms=receipt_available(s, kind, event_ms, received_ms, prior_available_ms),
                  quantity=float(qty), price=float(price))
    if s == 'M784':
        receipt = MainReceipt(**fields, raw_price=float(price), cash_delta=float(pnl - fee))
    elif s == 'A787':
        receipt = AltReceipt(**fields, fee=float(fee), realized_pnl=float(pnl),
                             protection_confirmed=False if purpose == 'ENTRY' else None)
    else:
        receipt = StockReceipt(**fields, fee=float(fee), realized_pnl=float(pnl))
    receipt.validate()
    return receipt

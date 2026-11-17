"""Actual sleeve funds and fill ownership, without a global audit gate.

The original equal allocation happens once. Subsequent realized cash belongs
to the sleeve that traded it. Manual positions consume exchange free margin
but are not adopted, closed, or counted as strategy profits.
"""
from decimal import Decimal as D, localcontext, ROUND_DOWN
import json

from .account import InvalidState, PositionKey, SLEEVES, number, positive
from .simple_engine import initial_allocation
from .simple_orders import SleeveFunds, identity, stamp, receipt_available
from .simple_store import _json


def proportional(amount, quantities):
    """Conserve an actual USDT amount to the last exchange decimal place."""
    unit=D('.00000001');amount=number(amount)
    if amount % unit or not quantities or any(number(v)<=0 for v in quantities.values()):
        raise InvalidState('资金费金额精度或持仓数量错误')
    with localcontext() as ctx:
        ctx.prec=80
        count=abs(amount)/unit;total=sum(map(number,quantities.values()),D(0))
        exact={k:count*number(q)/total for k,q in quantities.items()}
        rounded={k:int(v.to_integral_value(rounding=ROUND_DOWN)) for k,v in exact.items()}
        for k in sorted(exact,key=lambda k:(-(exact[k]-rounded[k]),k))[:int(count)-sum(rounded.values())]: rounded[k]+=1
        return {k:D(v)*unit*(-1 if amount<0 else 1) for k,v in rounded.items()}


class SimpleCash:
    def __init__(self, store, broker):
        if broker.store is not store: raise InvalidState('资金和订单必须使用同一运行记录')
        self.store=store;self.broker=broker;self.db=store.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS simple_funding(
            transaction_id TEXT PRIMARY KEY, body TEXT NOT NULL,
            received_ms INTEGER NOT NULL, allocation TEXT NOT NULL)''')

    def initialize(self, snapshot, *, now_ms):
        stamp(now_ms)
        with self.store.transaction():
            previous=self.store.value('initial_capital')
            if previous is not None: return previous
            if not 0<=now_ms-snapshot.received_ms<=5000:
                raise InvalidState('首次分配需要刚收到的账户余额')
            if self.db.execute('SELECT 1 FROM fills WHERE sleeve IS NOT NULL').fetchone():
                raise InvalidState('已有实际策略成交，不能重设初始本金')
            amounts=initial_allocation(snapshot)
            result=dict(created_ms=now_ms,hedge_mode=snapshot.hedge_mode,
                        amounts={s:str(v) for s,v in amounts.items()},snapshot_version=snapshot.version)
            self.store.set_value('initial_capital',result)
        return result

    def inventory(self, *, through_ms=None):
        """Signed quantities by exact strategy lot and exchange position key."""
        rows=self.db.execute('SELECT body,sleeve FROM fills WHERE sleeve IS NOT NULL'+
                             ('' if through_ms is None else ' AND time_ms<?')+' ORDER BY time_ms,trade_id',
                             () if through_ms is None else (stamp(through_ms),))
        lots={};cash={s:D(0) for s in SLEEVES}
        for raw,sleeve in rows:
            f=json.loads(raw);key=PositionKey(f['symbol'],f['position_side']);lot=(sleeve,f['lot_id'],key)
            lots[lot]=lots.get(lot,D(0))+positive(f['quantity'])*(1 if f['side']=='BUY' else -1)
            if f['fee_asset']!='USDT' and number(f['fee']): raise InvalidState('非 USDT 手续费缺少实际兑换金额')
            cash[sleeve]+=number(f['realized_pnl'])-number(f['fee'])
        lots={k:q for k,q in lots.items() if q}
        for (s,lot,key),q in lots.items():
            side=self.broker.plan(lot)['action']['order']['side']
            if side*q<=0: raise InvalidState('批次净成交与原策略方向矛盾')
        return lots,cash

    def view(self, snapshot, *, now_ms):
        stamp(now_ms);initial=self.store.value('initial_capital')
        if initial is None: raise InvalidState('组合本金尚未初始化')
        if not 0<=now_ms-snapshot.received_ms<=5000 or snapshot.hedge_mode!=initial['hedge_mode']:
            raise InvalidState('账户余额过期或持仓模式已改变')
        lots,flows=self.inventory();owned={};unrealized={s:D(0) for s in SLEEVES};held={s:D(0) for s in SLEEVES}
        for (s,lot,key),q in lots.items(): owned[key]=owned.get(key,D(0))+q
        observed={p.key:p for p in snapshot.positions if p.amount}
        mismatches={k for k,q in owned.items() if k not in observed or observed[k].amount!=q}
        for key,q in owned.items():
            if key in mismatches: continue
            quantities={s:sum((abs(v) for (sl,lot,k),v in lots.items() if sl==s and k==key),D(0)) for s in SLEEVES}
            denominator=sum(quantities.values(),D(0));position=observed[key]
            for s,amount in quantities.items():
                held[s]+=position.initial_margin*amount/denominator
                unrealized[s]+=position.unrealized_pnl*amount/denominator
        for (raw,) in self.db.execute('SELECT allocation FROM simple_funding'):
            for a in json.loads(raw): flows[a['sleeve']]+=number(a['amount'])
        actual_orders={o.client_id:o for o in snapshot.orders};pending=D(0);owned_orders=set()
        for (rid,) in self.db.execute("SELECT id FROM simple_requests WHERE operation IN ('SEND_ORDER','SEND_PROTECTION','ALGO_CHILD')"):
            r=self.broker.request_record(rid);p=self.broker.plan(r['lot_id']);s=p['sleeve'];a=r['body']['arguments']
            cid=a.get('newClientOrderId',a.get('clientAlgoId'));key=PositionKey(a['symbol'],a['positionSide'])
            if cid in actual_orders:
                order=actual_orders[cid]
                if order.key!=key or order.remaining>positive(a['quantity']):
                    mismatches.add(key)
                elif r['exchange_id'] is not None and r['exchange_status'] is not None:
                    owned_orders.add(cid)
            if r['purpose']!='ENTRY' or r['state']=='NOT_SENT': continue
            total=positive(a['quantity']);filled=self.broker.filled_quantity(rid)
            if r['state']=='TERMINAL' and filled==number(r['cumulative']): continue
            remaining=max(D(0),total-filled)
            reserved=number(r['body']['reserved_margin'])*remaining/total
            held[s]+=reserved
            # Recognized exchange orders already consume available balance.
            # Only the unreflected part is additionally reserved globally.
            if cid not in owned_orders: pending+=reserved
        funds={s:SleeveFunds(number(initial['amounts'][s])+flows[s],
                            number(initial['amounts'][s])+flows[s]+unrealized[s],held[s]) for s in SLEEVES}
        return dict(funds=funds,owned_positions=owned,owned_orders=owned_orders,
                    pending_margin=pending,account_free=max(D(0),snapshot.free_capital()-pending),
                    position_mismatches=mismatches,unrealized=unrealized)

    def record_funding(self, raw, *, received_ms, owned_position_at_event):
        """Record actual funding only when its position attribution is known.

        owned_position_at_event is independently established from the account
        position and complete incremental trades, including manual activity.
        No rate*mark estimate or current position is substituted for that fact.
        Ambiguous same-ms fills or hedged directions remain unallocated.
        """
        from .strategies.main import MainReceipt
        from .strategies.alt import AltReceipt
        from .strategies.stock import StockReceipt
        stamp(received_ms)
        if raw.get('incomeType')!='FUNDING_FEE' or raw.get('asset')!='USDT':
            raise InvalidState('这里只接收实际 USDT 资金费流水')
        tid=str(raw['tranId']);at=stamp(raw['time']);symbol=raw['symbol'];PositionKey(symbol,'BOTH')
        amount=number(raw['income']);body=dict(transaction_id=tid,event_ms=at,symbol=symbol,amount=str(amount))
        if not tid.isdigit() or not at<=received_ms: raise InvalidState('资金费身份或时间错误')
        with self.store.transaction():
            old=self.db.execute('SELECT body FROM simple_funding WHERE transaction_id=?',(tid,)).fetchone()
            if old:
                if json.loads(old[0])!=body: raise InvalidState('同一资金费流水内容变化')
                return False
            initial=self.store.value('initial_capital')
            if initial is None or at<=initial['created_ms']: raise InvalidState('资金费不能重复计入初始化前的本金')
            if self.db.execute('SELECT 1 FROM fills WHERE symbol=? AND time_ms=?',(symbol,at)).fetchone():
                raise InvalidState('成交与资金费同毫秒，不能猜测归属顺序')
            lots,_=self.inventory(through_ms=at);lots={k:q for k,q in lots.items() if k[2].symbol==symbol}
            if not lots or len({q>0 for q in lots.values()})!=1:
                raise InvalidState('资金费没有可明确分配的同方向策略持仓')
            expected={}
            for (s,lot,key),q in lots.items():expected[key]=expected.get(key,D(0))+q
            if expected!={k:number(v) for k,v in owned_position_at_event.items() if k.symbol==symbol and number(v)}:
                raise InvalidState('资金费时点有未归属持仓，不能算成策略收益')
            quantities={lot:abs(q) for (s,lot,key),q in lots.items()}
            allocations=proportional(amount,quantities);rows=[]
            classes=dict(M784=MainReceipt,A787=AltReceipt,S800=StockReceipt)
            for (s,lot,key),q in lots.items():
                p=self.broker.plan(lot);value=allocations[lot]
                prior=self.db.execute('SELECT MAX(available_ms) FROM receipts WHERE lot_id=?',(lot,)).fetchone()[0] or 0
                fields={'cash_delta' if s=='M784' else 'amount':float(value)}
                receipt=classes[s](identity(['funding',tid,lot]),p['strategy_order_id'],'funding',at,received_ms,
                                    receipt_available(s,'funding',at,received_ms,prior),**fields)
                self.store.put_receipt(s,lot,receipt)
                rows.append(dict(sleeve=s,lot_id=lot,position_side=key.side,amount=str(value)))
            self.db.execute('INSERT INTO simple_funding VALUES(?,?,?,?)',(tid,_json(body),received_ms,_json(rows)))
        return True

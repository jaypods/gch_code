"""Receipt-driven S800 decisions. No venue state, native tape or network access.

One stock sleeve slot; 5% costed risk / 3x notional cap; last four fully received
same-symbol net-R outcomes. Source birth/expiry comes from the source observer.
Full reconciliation is an explicit receipt barrier, never guessed from a local
zero position or from a cancellation request. The historical venue can model
this barrier; the real broker must establish it from exchange evidence.
"""
from copy import deepcopy
from dataclasses import dataclass,asdict
from decimal import Decimal
import heapq
import math
from ..account import InvalidState,PositionKey
from ..market import Bar

MINUTE=60000
FIVE=5*MINUTE


def positive(x):return type(x) in (int,float) and math.isfinite(x) and x>0


def decision_time(frame_ms,decision_ms=None,*,last_ms=None):
    """Logical closed-bar boundary and actual knowledge cutoff stay separate.

    Orders retain their original next-minute activation boundary. If this
    minute's processing is already that late, halt instead of issuing an order
    whose modeled execution window has started before the decision existed.
    """
    actual=frame_ms if decision_ms is None else decision_ms
    if type(frame_ms) is not int or type(actual) is not int or not frame_ms<=actual<frame_ms+MINUTE or last_ms is not None and actual<=last_ms:
        raise InvalidState('实际决策时间早于收盘、已越过下一分钟或发生倒退')
    return actual


@dataclass(frozen=True)
class StockSource:
    source_id: str
    symbol: str
    side: int
    birth_ms: int
    available_ms: int
    anchor: float
    distance: float
    score: float
    expiry_ms: int
    ordinal: int

    def validate(self,now):
        PositionKey(self.symbol,'LONG' if self.side==1 else 'SHORT')
        if not self.source_id or self.side not in (-1,1) or type(self.ordinal) is not int or self.ordinal<0:
            raise InvalidState('股票信号身份或方向错误')
        if any(type(t) is not int for t in (self.birth_ms,self.available_ms,self.expiry_ms)) or self.birth_ms%FIVE or not 0<=self.birth_ms<=self.available_ms<=now or self.expiry_ms<=self.birth_ms:
            raise InvalidState('股票信号尚未收到或时间错误')
        if not positive(self.anchor) or not positive(self.distance) or not math.isfinite(self.score):
            raise InvalidState('股票信号价格、距离或排序值无效')

    @property
    def cap(self):return self.anchor+self.side*.1*self.distance

    @property
    def invalid(self):return self.anchor-self.side*self.distance


@dataclass(frozen=True)
class StockReceipt:
    event_id: str
    order_id: int
    kind: str
    event_ms: int
    received_ms: int
    available_ms: int
    quantity: float=0.
    price: float=0.
    fee: float=0.
    amount: float=0.
    # Broker barrier attests that entry terminal, exits and all attributable
    # cash/fee/funding receipts through this closed parent have been reconciled.
    reconciled: bool=False
    parent_net: float | None=None
    entry_quantity: float | None=None
    realized_pnl: float | None=None  # exchange-reported cash for a verified fill

    def validate(self):
        if not self.event_id or type(self.order_id) is not int or self.order_id<1 or self.kind not in ('entry','exit','funding','entry_done','settled'):
            raise InvalidState('股票成交回报身份错误')
        if any(type(t) is not int for t in (self.event_ms,self.received_ms,self.available_ms)) or not 0<=self.event_ms<=self.received_ms<=self.available_ms:
            raise InvalidState('股票成交回报时序错误')
        if any(not math.isfinite(x) for x in (self.quantity,self.price,self.fee,self.amount)) or min(self.quantity,self.price)<0:
            raise InvalidState('股票成交回报数值无效')
        if self.realized_pnl is not None and (not math.isfinite(self.realized_pnl) or self.kind not in ('entry','exit') or self.kind=='entry' and self.realized_pnl!=0):
            raise InvalidState('真实已实现盈亏缺少对应成交或开仓产生异常平仓盈亏')
        if self.kind in ('entry','exit'):
            if not positive(self.quantity) or not positive(self.price) or self.amount:
                raise InvalidState('股票成交量价缺失或现金重复计入')
        elif self.quantity or self.price or self.fee:
            raise InvalidState('股票非成交回报携带成交量价')
        if self.kind!='funding' and self.amount:raise InvalidState('股票现金仅能来自成交或资金费')
        if self.kind=='settled' and (self.reconciled is not True or self.parent_net is None or self.entry_quantity is None
                                      or not math.isfinite(self.parent_net) or not math.isfinite(self.entry_quantity) or self.entry_quantity<0):
            raise InvalidState('股票交易结算缺少完整对账依据')


@dataclass(frozen=True)
class StockCancellationView:
    source_close_ms: int
    available_ms: int
    source_ids: dict
    calendar_allowed: dict
    calendar_known_ms: int

    def validate(self,now,*,decision_ms=None):
        expected=(now-MINUTE)//FIVE*FIVE
        at=decision_time(now,decision_ms)
        if self.source_close_ms!=expected or not self.source_close_ms<=self.available_ms<=at or not 0<=self.calendar_known_ms<=at:
            raise InvalidState('股票撤单依据未满足原策略来源延迟或尚未收到')
        if any(type(x) is not bool for x in self.calendar_allowed.values()):
            raise InvalidState('股票交易日历状态必须明确')


class StockBook:
    def __init__(self,*,start_ms,initial,cost=1):
        if type(start_ms) is not int or start_ms<0 or start_ms%MINUTE or not positive(initial) or cost not in (1,2):
            raise InvalidState('股票策略起点、资金或成本错误')
        self.start_ms=start_ms;self.initial=float(initial);self.cost=cost;self.known_cash=float(initial)
        self.last_clock=start_ms-MINUTE;self.last_decision_ms=start_ms-MINUTE;self.orders={};self.sources={};self.consumed=set();self.cancelled_sources=set()
        self.queue=[];self.receipts={};self.serial=0;self.deliveries=[];self.actions=[];self.decisions=[];self.gates=[]
        self.recent_outcomes={};self.performance=[]

    def receive(self,r:StockReceipt):
        r.validate()
        if r.order_id not in self.orders:raise InvalidState('股票回报没有对应委托')
        payload=asdict(r)
        if r.event_id in self.receipts:
            if self.receipts[r.event_id]!=payload:raise InvalidState('股票回报标识冲突')
            return False
        if r.available_ms<=self.last_decision_ms:raise InvalidState('股票晚到回报不能回填到已决策时刻')
        self.serial+=1;heapq.heappush(self.queue,(r.available_ms,self.serial,payload));self.receipts[r.event_id]=payload
        return True

    def _action(self,kind,oid,now,**kw):
        row=dict(kind=kind,order_id=oid,request_id=f'S800:{len(self.actions)+1}:{oid}:{kind}',decision_ms=now,**kw)
        self.actions.append(row);return row

    def _deliver(self,r,now):
        p=self.orders[r['order_id']];kind=r['kind'];q=r['quantity'];delta=0.
        if p['settled']:raise InvalidState('股票已结算交易又收到新回报，必须重新对账')
        if kind=='entry':
            if p['entry_done'] or p['entered']+q>p['quantity']+1e-9 or p['side']*(r['price']-p['limit'])>1e-10:
                raise InvalidState('股票开仓回报与数量、终态或价格上限矛盾')
            if not p['active_ms']<=r['event_ms']<p['expiry_ms']:raise InvalidState('股票成交超出委托时间')
            p['entered']+=q;p['remaining']+=q;p['basis']+=q*r['price'];p['fees']+=r['fee'];delta=-r['fee']
            self.consumed.add(p['source_id'])
            if p['first_ms'] is None:
                p['first_ms']=r['event_ms']
                self._action('EXIT_REQUEST',p['id'],now,effective_ms=p['first_ms']+1440*MINUTE,reason='deadline')
        elif kind=='exit':
            if p['remaining']<=0 or q>p['remaining']+1e-9 or r['event_ms']<p['first_ms']:
                raise InvalidState('股票平仓回报无对应仓位或数量超限')
            average=p['basis']/p['remaining'];gross=p['side']*q*(r['price']-average) if r['realized_pnl'] is None else r['realized_pnl']
            p['basis']-=q*average;p['remaining']=max(0.,p['remaining']-q);p['fees']+=r['fee'];p['realized']+=gross
            delta=gross-r['fee'];p['last_exit_ms']=r['event_ms']
        elif kind=='entry_done':p['entry_done']=True
        elif kind=='funding':
            if p['first_ms'] is None or r['event_ms']<p['first_ms'] or p.get('last_exit_ms') is not None and p['remaining']<=0 and r['event_ms']>p['last_exit_ms']:
                raise InvalidState('股票资金费没有可对应的持仓时间')
            delta=r['amount'];p['funding']+=delta
        elif kind=='settled':
            if not p['entry_done'] or p['remaining']>1e-9 or not math.isclose(r['entry_quantity'],p['entered'],rel_tol=1e-12,abs_tol=1e-9) or not math.isclose(r['parent_net'],p['net'],rel_tol=1e-12,abs_tol=1e-8):
                raise InvalidState('股票结算回报与已收成交、费用或仓位不符')
            if r['event_ms']<max(p['active_ms'],p.get('last_exit_ms',0)):
                raise InvalidState('股票结算依据早于交易完成')
            p['settled']=True;p['settled_ms']=now
            if p['entered']>0:
                net_r=p['net']/(p['entered']*p['loss_per_unit'])
                recent=(self.recent_outcomes.get(p['symbol'],[])+[(p['id'],net_r)])[-4:]
                self.recent_outcomes[p['symbol']]=recent
                self.performance.append(dict(order_id=p['id'],symbol=p['symbol'],known_ms=now,net=p['net'],net_r=net_r,
                                             debt=max(0.,-sum(x for _,x in recent)),recent_parent_ids=[i for i,_ in recent]))
        self.known_cash+=delta;p['net']+=delta
        self.deliveries.append(dict(event_id=r['event_id'],order_id=p['id'],kind=kind,delivered_ms=now,known_cash=self.known_cash))

    def _cancel(self,p,now,reason,*,effective_ms=None):
        if p['entry_done'] or p.get('cancel_requested'):return
        p['cancel_requested']=True
        self._action('CANCEL_ENTRY',p['id'],now,effective_ms=now if effective_ms is None else effective_ms,reason=reason)

    def _submit(self,s,price,now,*,decision_ms=None):
        at=decision_time(now,decision_ms)
        capital=self.known_cash;slip=.001*self.cost;fee=.0005*self.cost
        limit=s.cap;worst=limit if s.side==1 else s.invalid*(1-slip);loss=s.distance+worst*(2*fee+2*slip)
        reason='invalid_bracket' if min(limit-s.side*s.distance,limit+s.side*8*s.distance)<=0 else None
        if not positive(worst) or not positive(loss):reason='invalid_risk_geometry'
        recent=self.recent_outcomes.get(s.symbol,[]);debt=max(0.,-sum(r for _,r in recent))
        factor=1. if debt<2 else .5 if debt<4 else .25
        base=min(.05*capital/loss,3*capital/worst) if reason is None else 0.
        q=base*factor
        row=dict(source_id=s.source_id,symbol=s.symbol,decision_ms=at,why=reason,quantity=q,base_quantity=base,
                 quantity_multiplier=factor,debt=debt,received_window_parent_ids=[i for i,_ in recent],known_cash=capital,
                 limit=limit,worst_price=worst,loss_per_unit=loss,original_signal_close=price)
        self.decisions.append(row)
        if reason:return
        oid=len(self.orders)+1
        p=dict(row,id=oid,side=s.side,active_ms=now+MINUTE,expiry_ms=s.expiry_ms,distance=s.distance,invalid=s.invalid,
               stop=limit-s.side*s.distance,target=limit+s.side*8*s.distance,entered=0.,remaining=0.,basis=0.,fees=0.,
               realized=0.,funding=0.,net=0.,entry_done=False,settled=False,first_ms=None,protection=None)
        self.orders[oid]=p;row['order_id']=oid
        self._action('ENTRY_PLAN',oid,at,effective_ms=now+MINUTE,order=deepcopy(p))

    def _manage_order(self,p,now,at,minute_bars,cancellation):
        if at>=p['expiry_ms']:self._cancel(p,at,'expiry')
        elif now>=p['active_ms'] and not p['entry_done']:
            if cancellation is None or p['symbol'] not in cancellation.source_ids or p['symbol'] not in cancellation.calendar_allowed:
                raise InvalidState('股票在途委托缺少已知来源或交易日历状态')
            if cancellation.source_ids[p['symbol']]!=p['source_id']:self._cancel(p,at,'source_replaced')
            elif not cancellation.calendar_allowed[p['symbol']]:self._cancel(p,at,'calendar')
        if p['remaining']>0:
            if at-p['first_ms']>168*60*MINUTE:raise InvalidState('股票退出超过 168 小时，必须核查未完成成交')
            if p['protection'] is None:
                b=minute_bars.get(p['symbol'])
                if b is None:raise InvalidState('股票已收仓位缺少完整分钟观察')
                if b.volume>0 and b.quote_volume>0 and p['side']*(float(b.close)-p['limit'])>=2*p['distance']:
                    protection=dict(effective_ms=now+MINUTE,quantity=p['remaining']/2,stop=p['limit']+p['side']*.5*p['distance'])
                    p['protection']=protection
                    self._action('PROTECT_HALF',p['id'],at,**protection)
                    self._cancel(p,at,'protection_activation',effective_ms=now+MINUTE)

    def _select(self,now,at,sources,decision_bars):
        if now%FIVE==0 and not any(not p['settled'] for p in self.orders.values()) and self.known_cash>0:
            candidates=[]
            for s in sources:
                if s.source_id in self.consumed or s.source_id in self.cancelled_sources or now+MINUTE>=s.expiry_ms:continue
                b=decision_bars.get(s.symbol)
                if b is None:raise InvalidState('股票当前候选缺少决策收盘行情')
                if b.volume<=0 or b.quote_volume<=0:continue
                price=float(b.close)
                if s.side*(price-s.invalid)<=0:
                    self.cancelled_sources.add(s.source_id);continue
                kept=Decimal(s.side)*(Decimal(str(price))-Decimal(str(s.anchor)))>=0
                self.gates.append(dict(source_id=s.source_id,symbol=s.symbol,decision_ms=at,kept=kept,price=price,anchor=s.anchor))
                if not kept:self.cancelled_sources.add(s.source_id);continue
                candidates.append((s,price))
            candidates.sort(key=lambda pair:(-pair[0].score,pair[0].ordinal))
            if candidates:self._submit(*candidates[0],now,decision_ms=at)

    def advance(self,now,*,sources=(),decision_bars=None,minute_bars=None,cancellation:StockCancellationView|None=None,decision_ms=None):
        if type(now) is not int or now!=self.last_clock+MINUTE:raise InvalidState('股票控制时钟必须连续推进')
        at=decision_time(now,decision_ms,last_ms=self.last_decision_ms)
        sources=tuple(sources);decision_bars=decision_bars or {};minute_bars=minute_bars or {}
        if sources and now%FIVE:raise InvalidState('股票只在完整五分钟时钟选择新来源')
        for s in sources:
            s.validate(at)
            if s.source_id in self.sources and self.sources[s.source_id]!=s:raise InvalidState('股票来源标识发生修订')
        if len({s.symbol for s in sources})!=len(sources) or len({s.source_id for s in sources})!=len(sources):
            raise InvalidState('股票当前来源出现重复币种或标识')
        for bars,period in ((decision_bars,FIVE),(minute_bars,MINUTE)):
            for symbol,b in bars.items():
                if not isinstance(b,Bar) or b.symbol!=symbol or b.period_ms!=period or b.end_ms!=now or b.received_ms>at:
                    raise InvalidState('股票控制器使用了未收盘、未收到或错位的行情')
        if cancellation is not None:cancellation.validate(now,decision_ms=at)
        # Closed parents and historical logs are immutable. Copy only live
        # state, then roll back append-only tails on failure; full-history
        # deepcopy at every minute would make a long replay quadratic.
        old_ids=set(self.orders);old_sources=set(self.sources)
        live={k:deepcopy(p) for k,p in self.orders.items() if not p['settled']}
        before=(self.known_cash,self.last_clock,self.last_decision_ms,list(self.queue),set(self.consumed),set(self.cancelled_sources),deepcopy(self.recent_outcomes))
        lengths={k:len(getattr(self,k)) for k in ('actions','deliveries','decisions','gates','performance')}
        try:
            first=len(self.actions)
            while self.queue and self.queue[0][0]<=at:self._deliver(heapq.heappop(self.queue)[2],at)
            for s in sources:self.sources.setdefault(s.source_id,s)
            for p in self.orders.values():
                if not p['settled']:self._manage_order(p,now,at,minute_bars,cancellation)
            self._select(now,at,sources,decision_bars)
            self.last_clock=now;self.last_decision_ms=at
            return deepcopy(self.actions[first:])
        except BaseException:
            self.known_cash,self.last_clock,self.last_decision_ms,self.queue,self.consumed,self.cancelled_sources,self.recent_outcomes=before
            for k in set(self.orders)-old_ids:del self.orders[k]
            self.orders.update(live)
            for k in set(self.sources)-old_sources:del self.sources[k]
            for k,n in lengths.items():del getattr(self,k)[n:]
            raise

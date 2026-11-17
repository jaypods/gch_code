"""Streaming S800 mother sources from received, complete 5m observations.

The 25-close momentum window includes the current close; ATR and 20-day
turnover deliberately stop one bar earlier. A missing/zero-volume/quarantined
bar breaks feature continuity. Identity/quality metadata must be known at the
observation time; retrospectively reconstructed metadata is conditional only.
"""
from collections import deque
from dataclasses import dataclass,field
import math
from ..account import InvalidState,PositionKey
from ..market import Bar
from .stock import StockSource,StockCancellationView,FIVE,MINUTE,decision_time

LIQUIDITY_BARS=5760


@dataclass(frozen=True)
class StockObservation:
    bar:Bar|None
    status:str
    metadata_known_ms:int

    def validate(self,symbol,now,conditional_history,*,known_ms=None):
        at=now if known_ms is None else known_ms
        if type(at) is not int or at<now:raise InvalidState('股票来源的实际接收时间早于收盘')
        if self.status not in ('present','not_listed','quarantined','missing') or type(self.metadata_known_ms) is not int or not 0<=self.metadata_known_ms<=at:
            raise InvalidState('股票身份、质量状态或其可知时间无效')
        if self.status=='missing' and not conditional_history:raise InvalidState('股票行情缺失，不能伪装成无信号')
        if self.status=='present':
            b=self.bar
            if not isinstance(b,Bar) or b.symbol!=symbol or b.period_ms!=FIVE or b.end_ms!=now or b.received_ms>at:
                raise InvalidState('股票信号包含错位、未收盘或尚未收到的行情')
            if any(not math.isfinite(float(x)) for x in b.content()):raise InvalidState('股票行情超过有限计算范围')
        elif self.bar is not None:raise InvalidState('股票非有效状态不能混入行情')


def kahan(total,comp,value):
    y=value-comp;result=total+y
    return result,(result-total)-y


@dataclass
class FeatureState:
    count:int=0
    atr:float=math.nan
    previous_close:float|None=None
    quotes:deque=field(default_factory=deque)
    total:float=0.
    add_comp:float=0.
    remove_comp:float=0.
    closes:deque=field(default_factory=lambda:deque(maxlen=25))

    def preview(self,observation):
        b=observation.bar
        valid=observation.status=='present' and b.volume>0 and b.quote_volume>0
        price=float(b.close) if valid else None
        closes=list(self.closes)[-24:]+[price]
        lag_atr=self.atr if self.count>=14 else math.nan
        lag_liquidity=self.total/20. if len(self.quotes)==LIQUIDITY_BARS else 0.
        momentum=price/closes[0]-1 if len(closes)==25 and price is not None and closes[0] is not None else math.nan
        eligible=(len(closes)==25 and all(x is not None for x in closes) and math.isfinite(momentum)
                  and math.isfinite(lag_atr) and lag_atr>0 and lag_liquidity>=1e7 and abs(momentum)>.03)
        row=dict(valid=valid,close=price,atr=lag_atr,liquidity=lag_liquidity,momentum=momentum,eligible=eligible)
        if not valid:return row,dict(reset=True,price=price)
        high,low=float(b.high),float(b.low)
        tr=max(high-low,abs(high-self.previous_close),abs(low-self.previous_close)) if self.previous_close is not None else high-low
        # pandas ewm(adjust=False) preserves identical consecutive values.
        atr=tr if self.count==0 else self.atr if self.atr==tr else (1-1/14)*self.atr+(1/14)*tr
        total,add,remove=self.total,self.add_comp,self.remove_comp
        if len(self.quotes)==LIQUIDITY_BARS:total,remove=kahan(total,remove,-self.quotes[0])
        quote=float(b.quote_volume);total,add=kahan(total,add,quote)
        if not math.isfinite(atr) or not math.isfinite(total):raise InvalidState('股票特征计算溢出')
        return row,dict(reset=False,price=price,atr=atr,quote=quote,total=total,add_comp=add,remove_comp=remove)

    def commit(self,p):
        self.closes.append(p['price'])
        if p['reset']:
            self.count=0;self.atr=math.nan;self.previous_close=None;self.quotes.clear()
            self.total=self.add_comp=self.remove_comp=0.;return
        self.count+=1;self.atr=p['atr'];self.previous_close=p['price']
        if len(self.quotes)==LIQUIDITY_BARS:self.quotes.popleft()
        self.quotes.append(p['quote']);self.total=p['total'];self.add_comp=p['add_comp'];self.remove_comp=p['remove_comp']


class StockSourceObserver:
    def __init__(self,*,symbols,start_ms,conditional_history=False,warmup_start_ms=None):
        self.symbols=tuple(symbols)
        if not self.symbols or len(set(self.symbols))!=len(self.symbols) or type(start_ms) is not int or start_ms<FIVE or start_ms%FIVE:
            raise InvalidState('股票信号观察范围或起点无效')
        for s in self.symbols:PositionKey(s,'LONG')
        if warmup_start_ms is None:warmup_start_ms=start_ms
        if type(warmup_start_ms) is not int or not FIVE<=warmup_start_ms<=start_ms or warmup_start_ms%FIVE:
            raise InvalidState('股票预热起点错误')
        self.start_ms=start_ms;self.last_clock=warmup_start_ms-FIVE;self.conditional_history=bool(conditional_history)
        self.features={s:FeatureState() for s in self.symbols};self.previous={s:0 for s in self.symbols};self.active={}
        self.births=[];self.lifecycle=[];self.history={};self.validity_history={};self.known_history={};self.last_known_ms=0
        self.frames=0;self.warmup_frames=0;self.missing_observations=0

    def advance(self,now,observations,*,universe_known_ms,universe_complete,known_ms=None):
        if type(now) is not int or now!=self.last_clock+FIVE:raise InvalidState('股票信号时钟必须逐根连续推进')
        at=now if known_ms is None else known_ms
        if type(at) is not int or at<now or at<self.last_known_ms:raise InvalidState('股票来源的实际可知时间倒退或早于收盘')
        if type(universe_known_ms) is not int or not 0<=universe_known_ms<=at or type(universe_complete) is not bool or not universe_complete and not self.conditional_history:
            raise InvalidState('股票观察范围不完整或尚未可知')
        if set(observations)!=set(self.symbols):raise InvalidState('股票完整观察范围存在缺项或多项')
        for s in self.symbols:
            o=observations[s]
            if not isinstance(o,StockObservation):raise InvalidState('股票观察记录类型错误')
            o.validate(s,now,self.conditional_history,known_ms=at)
        # Preview all numeric changes before mutation; no large warmup buffers
        # need to be copied at every 5m clock.
        previews={s:self.features[s].preview(observations[s]) for s in self.symbols}
        active=dict(self.active);previous={};births=[];life=[];warmup=now<self.start_ms
        for j,s in enumerate(self.symbols):
            row,_=previews[s];state=(1 if row['momentum']>0 else -1) if row['eligible'] and not warmup else 0
            previous[s]=state
            if state and state!=self.previous[s]:
                if s in active:life.append(dict(time_ms=now,source_id=active[s].source_id,symbol=s,reason='replacement',price=row['close']))
                seq=len(self.births)+len(births)+1
                # Original record expiry i+3 mapped to native-minute exclusive
                # expiry t0+(5*(i+3)+1)*minute: birth+11 minutes, not a future
                # horizon-dependent clipped deadline.
                mother=StockSource(f'S800:{seq:06d}',s,state,now,at,row['close'],6*row['atr'],abs(row['momentum']),now+2*FIVE+MINUTE,j)
                mother.validate(at);births.append(mother);active[s]=mother
            if s in active:
                p=active[s]
                reason=('invalid_observation' if not row['valid'] else 'source_expiry' if now>=p.birth_ms+3*FIVE
                        else 'source_stop' if p.side*(row['close']-p.anchor)<=-p.distance else None)
                if reason:
                    life.append(dict(time_ms=now,source_id=p.source_id,symbol=s,reason=reason,price=row['close']));del active[s]
        for s in self.symbols:
            self.features[s].commit(previews[s][1])
            if warmup:self.features[s].closes.clear()
        self.active=active;self.previous=previous;self.births.extend(births);self.lifecycle.extend(life)
        self.last_clock=now;self.last_known_ms=at
        if warmup:self.warmup_frames+=1
        else:self.frames+=1
        self.missing_observations+=sum(o.status=='missing' for o in observations.values())
        ids={s:active[s].source_id if s in active else None for s in self.symbols}
        self.history[now]=ids
        self.validity_history[now]={s:previews[s][0]['valid'] for s in self.symbols}
        self.known_history[now]=at
        for t in tuple(self.history):
            if t<now-FIVE:
                del self.history[t];del self.validity_history[t];del self.known_history[t]
        return dict(sources=tuple(active[s] for s in self.symbols if s in active),births=tuple(births),source_ids=dict(ids),
                    features={s:previews[s][0] for s in self.symbols},known_ms=at)

    def cancellation(self,now,*,calendar_allowed,calendar_known_ms,decision_ms=None):
        expected=(now-MINUTE)//FIVE*FIVE
        if expected not in self.history:raise InvalidState('股票撤单缺少原策略延迟对应的来源快照')
        if set(calendar_allowed)!=set(self.symbols):raise InvalidState('股票交易日历未覆盖观察范围')
        view=StockCancellationView(expected,self.known_history[expected],dict(self.history[expected]),dict(calendar_allowed),calendar_known_ms)
        view.validate(now,decision_ms=decision_ms);return view

    def execution_view(self,now,*,current_quality,quality_known_ms,decision_ms=None):
        """Original allowed[k] = prior printed bar AND prior/current quality.

        This inherited gate is called 'calendar' by the old execution driver;
        it is not a NYSE/RTH restriction. Current candle final volume is never
        consulted. Source replacement retains its separate strict-before lag.
        """
        end=now//FIVE*FIVE
        at=decision_time(now,decision_ms)
        if end not in self.validity_history or set(current_quality)!=set(self.symbols) or any(type(x) is not bool for x in current_quality.values()):
            raise InvalidState('股票执行资格缺少已完成行情或明确合约状态')
        if self.known_history[end]>at:raise InvalidState('股票可成交资格快照尚未实际收到')
        from ..stock_admission import validity_at
        allowed={s:validity_at(self,end,s) and current_quality[s] for s in self.symbols}
        return self.cancellation(now,calendar_allowed=allowed,calendar_known_ms=quality_known_ms,decision_ms=at)

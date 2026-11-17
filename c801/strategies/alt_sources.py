"""Causal A787 source observer: original ratio cohort plus A502 conjunction.

No fills, returns after entry, saved selected IDs, or account outcomes enter this
module. A caller supplies a complete known universe, a 193-hour prefix, and
only the five-minute features requested for price-qualified mothers. Missing
history is explicit; certified runtime mode refuses undocumented gaps.
"""
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal, localcontext
import math
import numpy as np
from ..account import InvalidState, PositionKey
from ..market import Bar

STEP=300_000
HOUR=3_600_000
EXCLUDED=('BTCUSDT','ETHUSDT','GIGADEVUSDT','CBRSUSDT','QNTXUSDT')


@dataclass(frozen=True)
class HourlyWindow:
    end_ms: int
    known_ms: int
    names: tuple[str,...]
    close: np.ndarray
    high: np.ndarray
    low: np.ndarray
    quote: np.ndarray
    received_ms: np.ndarray
    # 0=present, 1=not yet listed/known ineligible, 2=unexplained missing history.
    absence: np.ndarray
    universe_complete: bool
    source_names: tuple[str,...] | None = None

    def validate(self,now,conditional):
        if type(self.end_ms) is not int or self.end_ms%HOUR or now!=self.end_ms+STEP or not 0<=self.known_ms<=now:
            raise InvalidState('山寨小时窗口或币种池观察时间错误')
        if not self.names or len(self.names)!=len(set(self.names)) or not {'BTCUSDT','ETHUSDT'}<=set(self.names):
            raise InvalidState('山寨市场名单重复或缺少 BTC/ETH')
        for s in self.names:PositionKey(s,'LONG')
        if self.source_names is not None and (len(self.source_names)!=len(set(self.source_names)) or not set(self.source_names)<=set(self.names)):
            raise InvalidState('山寨信号排名名单与上下文名单不一致')
        shape=(193,len(self.names))
        for a in (self.close,self.high,self.low,self.quote,self.received_ms,self.absence):
            if not isinstance(a,np.ndarray) or a.shape!=shape:
                raise InvalidState('山寨小时窗口必须包含连续 193 小时')
        if not np.isin(self.absence,(0,1,2)).all():
            raise InvalidState('山寨缺失行情没有明确分类')
        present=self.absence==0
        values=(self.close,self.high,self.low,self.quote)
        if any(not np.isfinite(a[present]).all() for a in values):
            raise InvalidState('山寨已标记收到的行情包含非有限值')
        if any(not (a[present]>0).all() for a in values[:3]) or not (self.quote[present]>=0).all():
            raise InvalidState('山寨已收到行情价格或成交额无效')
        if not ((self.low<=self.close)&(self.close<=self.high))[present].all():
            raise InvalidState('山寨小时高低收盘价矛盾')
        ends=np.arange(self.end_ms-192*HOUR,self.end_ms+HOUR,HOUR)[:,None]
        if not np.issubdtype(self.received_ms.dtype,np.integer) or not ((self.received_ms>=ends)&(self.received_ms<=now))[present].all():
            raise InvalidState('山寨小时行情尚未收到或被回填到收盘之前')
        if any(not np.isnan(a[~present]).all() for a in values):
            raise InvalidState('山寨缺失行情不得包含插值或未来填补价格')
        if type(self.universe_complete) is not bool or not conditional and (not self.universe_complete or np.any(self.absence==2)):
            raise InvalidState('山寨市场名单或行情存在未经解释的缺口，不能据此开仓')


@dataclass(frozen=True)
class FlowBar:
    bar: Bar
    buy_quote: Decimal

    def __post_init__(self):
        amount=Decimal(str(self.buy_quote))
        if not amount.is_finite() or not 0<=amount<=self.bar.quote_volume or self.bar.period_ms!=STEP:
            raise InvalidState('主动买入成交额或周期无效')
        object.__setattr__(self,'buy_quote',amount)


@dataclass(frozen=True)
class FlowWindow:
    perpetual_symbol: str
    spot_symbol: str
    mapping_known_ms: int
    spot: tuple[FlowBar,...]
    perpetual: tuple[FlowBar,...]


def ranked(w,k,*,names=None,lexical=False):
    columns=[w.names.index(s) for s in (w.names if names is None else names)]
    hist=w.close[k-168:k+1]
    quotes=w.quote[k-23:k+1]
    valid=np.isfinite(hist).all(axis=0)&(hist>0).all(axis=0)&np.isfinite(quotes).all(axis=0)&(quotes>=0).all(axis=0)
    q24=quotes.sum(axis=0)
    with np.errstate(invalid='ignore',divide='ignore'):returns=w.close[k]/w.close[k-24]-1
    eligible=[j for j in columns if valid[j] and q24[j]>=1e7 and math.isfinite(returns[j]) and w.names[j] not in EXCLUDED]
    # Original mother ranking preserves cohort order on ties. A501 rotation
    # deliberately uses lexical ties; conflating the two changes the strategy.
    ordering=sorted(eligible,key=(lambda j:(-float(returns[j]),w.names[j])) if lexical else (lambda j:-float(returns[j])))
    return eligible,ordering[:3],returns,q24


def turnover_context(w,j,now):
    eligible,_,returns,q24=ranked(w,192)
    peers=[k for k in eligible if k!=j]
    total=float(q24[peers].sum())
    up=float(q24[[k for k in peers if returns[k]>0]].sum())
    return dict(symbol=w.names[j],decision_ms=now,input_available_ms=now,unknown=False,
                peer_total_quote24=total,peer_positive_quote24=up,passed=bool(total>0 and 2*up>total),
                peer_count=len(peers),peer_positive_turnover_fraction=up/total if total>0 else None)


def rotation(w,j):
    _,old,old_returns,_=ranked(w,168,names=w.source_names,lexical=True)
    _,new,_,_=ranked(w,192,names=w.source_names,lexical=True)
    os,ns=[w.names[k] for k in old],[w.names[k] for k in new]
    target=w.names[j]
    reason=('no_eligible_previous_leader' if not old else 'previous_leader_not_positive' if old_returns[old[0]]<=0
            else 'target_already_previous_top3' if target in os else 'target_not_current_top3' if target not in ns
            else 'previous_leader_still_current_top3' if os[0] in ns else None)
    known=True
    if reason is None:
        k=old[0];values=[w.close[192,k],w.close[186,k],w.quote[192,k],w.close[192,j],w.close[186,j],w.quote[192,j]]
        if not all(math.isfinite(x) and x>0 for x in values):
            known=False;reason='required_transition_observation_missing_or_unprinted'
        elif values[0]>=values[1]:reason='previous_leader_not_falling_over6h'
        elif values[3]<=values[4]:reason='new_leader_not_rising_over6h'
    return dict(known=known,passed=known and reason is None,reason=reason,old_top3=os,current_top3=ns)


def signed_flow(window,*,symbol,end_ms,now_ms,context):
    if window is None:return dict(known=False,passed=False,reason='missing_hour')
    if window.perpetual_symbol!=symbol or not window.spot_symbol or not 0<=window.mapping_known_ms<=end_ms-HOUR:
        return dict(known=False,passed=False,reason='spot_mapping_not_known_before_window')
    if len(window.spot)!=12 or len(window.perpetual)!=12:
        return dict(known=False,passed=False,reason='missing_hour')
    for bars,name in ((window.spot,window.spot_symbol),(window.perpetual,symbol)):
        for k,f in enumerate(bars):
            b=f.bar
            if b.symbol!=name or b.start_ms!=end_ms-HOUR+k*STEP or b.end_ms!=b.start_ms+STEP or b.received_ms>now_ms:
                return dict(known=False,passed=False,reason='flow_window_not_complete_or_received')
    with localcontext() as c:
        c.prec=50
        sq=sum((f.bar.quote_volume for f in window.spot),Decimal(0));sb=sum((f.buy_quote for f in window.spot),Decimal(0))
        pq=sum((f.bar.quote_volume for f in window.perpetual),Decimal(0));pb=sum((f.buy_quote for f in window.perpetual),Decimal(0))
        sd,pd=2*sb-sq,2*pb-pq
    reason=('zero_hour_turnover' if sq<=0 or pq<=0 else 'peer_turnover_not_majority' if not context['passed']
            else 'spot_buy_not_dominant' if sd<=0 else 'perp_sell_not_dominant' if pd>=0 else None)
    return dict(known=True,passed=reason is None,reason=reason,spot_quote=str(sq),spot_buy_quote=str(sb),spot_signed_quote=str(sd),
                perp_quote=str(pq),perp_buy_quote=str(pb),perp_signed_quote=str(pd))


def known_feature(source,bars,now):
    end=source['signal_end_ms']
    if bars is None or len(bars)!=13:
        return None,'missing_contiguous13bars'
    for i,b in enumerate(bars):
        if b.symbol!=source['symbol'] or b.period_ms!=STEP or b.start_ms!=end-13*STEP+i*STEP or b.received_ms>now:
            return None,'invalid_required_known13bars'
    hi=np.array([float(b.high) for b in bars]);lo=np.array([float(b.low) for b in bars]);cl=np.array([float(b.close) for b in bars])
    tr=np.maximum(hi[1:]-lo[1:],np.maximum(abs(hi[1:]-cl[:-1]),abs(lo[1:]-cl[:-1])))
    return dict(range12_pct=float(np.mean(hi[1:]-lo[1:])/source['reference_price']),atr12_pct=float(np.mean(tr)/source['reference_price']),
                ret1h=float(cl[-1]/float(bars[1].open)-1),feature_last_close_ms=end-1,window_start_ms=end-HOUR,bars=12,contiguous=True),None


def birth(source,feature,error):
    p=source['reference_price'];distance=(p-source['stop'])*(.20 if source['momentum_rank']==1 else .78)
    stop,target,fixed=p-distance,p+48*distance,p+3*distance
    reason='missing_known_feature' if error else 'signal_ATR12_above_12pct' if feature['atr12_pct']>.12 else None
    if not all(math.isfinite(x) for x in (p,distance,stop,target,fixed)) or not 0<stop<p<fixed<=target:
        reason=reason or 'invalid_birth_geometry'
    if reason:return None,reason
    result=dict(source,stop=stop,tp2=target,virtual_base_target=target,fixed_T3=fixed,target_arm='C0',feature_audit=feature,
                structural_level=source['breakout_level'],structural_available_ms=source['signal_end_ms']-HOUR+STEP,
                input_available_ms=source['decision_ms'],horizon_hours=168)
    return result,None


class AltSourceObserver:
    def __init__(self,*,start_ms,conditional_history=False):
        if type(start_ms) is not int or start_ms%HOUR!=STEP or type(conditional_history) is not bool:
            raise InvalidState('山寨来源控制器起始时钟错误')
        self.last_clock=start_ms-HOUR
        self.conditional_history=conditional_history
        self.last_leader=None;self.events=[];self.last_mother={};self.mother_count=0
        self.decisions=[];self.rank_checks=[];self.states=[];self.source_evidence=[]

    def advance(self,now,window:HourlyWindow,*,five_minute,flow):
        if now!=self.last_clock+HOUR:
            raise InvalidState('山寨信号时钟必须连续，不能跳过冷却或领先事件')
        window.validate(now,self.conditional_history)
        # Only the current hour mutates state; historical logs are append-only.
        # Retaining lengths avoids quadratic copies over a multi-year replay.
        before=(self.last_clock,self.last_leader,len(self.events),self.last_mother.copy(),self.mother_count,
                len(self.decisions),len(self.rank_checks),len(self.states),len(self.source_evidence))
        try:
            result=self._advance(now,window,five_minute,flow)
            self.last_clock=now
            return result
        except BaseException:
            self.last_clock,self.last_leader,events,self.last_mother,self.mother_count,decisions,ranks,states,evidence=before
            del self.events[events:];del self.decisions[decisions:];del self.rank_checks[ranks:]
            del self.states[states:];del self.source_evidence[evidence:]
            raise

    def _advance(self,now,w,five_minute,flow):
        end=w.end_ms;btc=w.names.index('BTCUSDT');eth=w.names.index('ETHUSDT')
        with np.errstate(invalid='ignore',divide='ignore'):
            ratios=w.close[-169:,eth]/w.close[-169:,btc]
            br=w.close[-1,btc]/w.close[-25,btc]-1;er=w.close[-1,eth]/w.close[-25,eth]-1
        valid_ratio=np.isfinite(ratios).all() and (ratios>0).all()
        condition=bool(valid_ratio and ratios[-1]>max(ratios[:-1]) and br>0 and er>0)
        event=condition and (self.last_leader is None or end-self.last_leader>=24*HOUR)
        if event:
            self.last_leader=end;self.events.append(end)
        previous=[t for t in self.events if HOUR<=end-t<=24*HOUR]
        eligible,rank,returns,q24=ranked(w,192,names=w.source_names)
        positive=sum(returns[j]>0 for j in eligible)
        self.states.append(dict(signal_end_ms=end,decision_ms=now,leader_condition=condition,event=event,
                                previous_event_ms=previous[-1] if previous else None,eligible_count=len(eligible),positive_count=int(positive),
                                ranked_symbols=[w.names[j] for j in rank],unexplained_missing=int(np.count_nonzero(w.absence==2)),
                                universe_complete=w.universe_complete,conditional_history=self.conditional_history))
        output=[]
        for position,j in enumerate(rank,1):
            symbol=w.names[j];hist_hi=w.high[-7:-1,j]
            high6=float(max(hist_hi)) if np.isfinite(hist_hi).all() else float('nan')
            reason=('nonpositive_own_return' if returns[j]<=0 else 'no_six_hour_breakout' if not math.isfinite(high6) or w.close[-1,j]<=high6
                    else 'mother_cooldown' if end-self.last_mother.get(symbol,-10**18)<24*HOUR else None)
            self.rank_checks.append(dict(symbol=symbol,signal_end_ms=end,momentum_rank=position,first_rejection=reason))
            if reason:continue
            self.last_mother[symbol]=end;self.mother_count+=1
            mother_id='A'+str(self.mother_count).zfill(5)
            row=dict(symbol=symbol,signal_end_ms=end,decision_ms=now,momentum_rank=position,mother_id=mother_id,
                     leadership_pass=bool(previous),first_rejection=None)
            self.decisions.append(row)
            # Mother cooldown is consumed BEFORE leader/breadth/price/flow gates.
            if positive*2<=len(eligible):
                row['first_rejection']='not_majority_positive';continue
            hi,lo,cl=w.high[-32:,j],w.low[-32:,j],w.close[-32:,j]
            if not all(np.isfinite(a).all() and (a>0).all() for a in (hi,lo,cl)):
                row['first_rejection']='missing_32_hour_prices';continue
            p,s,high=float(cl[-1]),float(lo[-1]),float(hi[-1]);s6=float(min(lo[-6:]));level=float(max(hi[-25:-1]))
            reason=('no_24_hour_breakout' if p<=level else 'confirmation_not_upper_half' if high<=s or 2*p<high+s
                    else 'known_cost_over_0p2R' if any(stop<=0 or p<=stop or (p*.001+stop*.001+.0005*(p*1.001+stop*.999))/(p-stop)>.2 for stop in (s6,s)) else None)
            if reason:row['first_rejection']=reason;continue
            context=turnover_context(w,j,now)
            extra=not previous
            if extra:
                rot=rotation(w,j)
                sf=signed_flow(flow(symbol,end,now),symbol=symbol,end_ms=end,now_ms=now,context=context) if rot['known'] and rot['passed'] else dict(known=False,passed=False,reason='rotation_not_selected')
                reason='rotation_unknown' if not rot['known'] else 'outside_rotation' if not rot['passed'] else 'flow_unknown' if not sf['known'] else 'flow_rejected' if not sf['passed'] else None
                self.source_evidence.append(dict(symbol=symbol,signal_end_ms=end,rotation=rot,flow=sf))
                if reason:row['first_rejection']=reason;continue
            identity=f'A275_{end}_{symbol}' if extra else f'A787_ORIGINAL_{end}_{symbol}'
            source=dict(id=identity,symbol=symbol,side=1,momentum_rank=position,leadership_event_hour=-(end//HOUR+1) if extra else previous[-1]//HOUR,
                        known_quote24=float(q24[j]),signal_end_ms=end,decision_ms=now,intended_entry_ms=now+STEP,reference_price=p,stop=s,
                        tp2=p+3*(p-s6),six_hour_stop=s6,breakout_level=level,context=context,source_kind='rotation_spot' if extra else 'ratio_leader',native_mother_id=mother_id)
            features,error=known_feature(source,five_minute(symbol,end,now),now)
            emitted,rejection=birth(source,features,error)
            row.update(id=identity,first_rejection=rejection,feature_error=error)
            if emitted is not None:output.append(emitted)
        return output

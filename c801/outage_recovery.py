"""Source indicators after missed minutes; no journal or replay service."""
from copy import deepcopy
import hashlib
from .account import InvalidState
from .state_codec import encode
from .strategies.stock import FIVE,MINUTE
from .strategies.stock_sources import StockSourceObserver,StockObservation
from .stock_admission import admit_stock_roster


def recover_stock_frame(observer,frame_ms,observations,*,known_ms,
                        universe_known_ms,universe_complete,roster_admission=None):
    """Continue S800 features through ONE missed 5m source observation.

    A sign transition still invalidates a previous mother, but its replacement
    is recorded as missed instead of born. Retaining the new direction prevents
    a continuing old move from masquerading as a fresh transition after restart.
    Existing mothers can only survive their original stop/expiry; their price,
    birth, availability and lifetime are never rebased to recovery time.

    No earlier financial decision is replayed. This is preparation on a staged
    observer, independent of the funded book, receipt queues and exchange state.
    All validation/numeric previews finish before any existing state is changed.
    """
    if not isinstance(observer,StockSourceObserver):raise InvalidState('恢复需要原股票来源观察器')
    if roster_admission is not None:
        # Admission is rare: isolate the entire transition so a bad next frame
        # cannot partially change an old feature, rank or admission record.
        staged=deepcopy(observer)
        admission=admit_stock_roster(staged,roster_admission,known_ms=known_ms)
        evidence=recover_stock_frame(staged,frame_ms,observations,known_ms=known_ms,
            universe_known_ms=universe_known_ms,universe_complete=universe_complete)
        evidence['roster_admission']=admission
        observer.__class__=staged.__class__;observer.__dict__=staged.__dict__
        return evidence
    if (type(frame_ms) is not int or frame_ms%FIVE or frame_ms<observer.start_ms
            or frame_ms!=observer.last_clock+FIVE):
        raise InvalidState('恢复只能连续续接已启动的股票来源时钟')
    if (type(known_ms) is not int or known_ms<frame_ms+MINUTE
            or known_ms<observer.last_known_ms):
        raise InvalidState('恢复必须记录错过决策窗口后的实际处理时间')
    if (type(universe_known_ms) is not int or not 0<=universe_known_ms<=known_ms
            or universe_complete is not True):
        raise InvalidState('股票恢复必须使用实际已知的完整观察范围')
    if not isinstance(observations,dict) or set(observations)!=set(observer.symbols):
        raise InvalidState('股票恢复观察范围不能缺项或静默迁移')
    for symbol in observer.symbols:
        observation=observations[symbol]
        if not isinstance(observation,StockObservation):raise InvalidState('股票恢复观察类型错误')
        # An outage does not permit the historical research missing-data mode.
        observation.validate(symbol,frame_ms,False,known_ms=known_ms)
    previews={s:observer.features[s].preview(observations[s]) for s in observer.symbols}
    active=dict(observer.active);previous={};retired=[];missed=[]
    for symbol in observer.symbols:
        row,_=previews[symbol]
        direction=(1 if row['momentum']>0 else -1) if row['eligible'] else 0
        previous[symbol]=direction
        if direction and direction!=observer.previous[symbol]:
            old=active.pop(symbol,None)
            if old is not None:
                retired.append(dict(time_ms=frame_ms,known_ms=known_ms,source_id=old.source_id,
                    symbol=symbol,reason='outage_source_replaced',price=row['close']))
            missed.append(dict(symbol=symbol,side=direction,frame_ms=frame_ms,known_ms=known_ms,
                reference=row['close'],distance=6*row['atr'],score=abs(row['momentum']),
                reason='MISSED_DECISION_WINDOW'))
        if symbol in active:
            mother=active[symbol]
            reason=('invalid_observation' if not row['valid'] else 'source_expiry' if frame_ms>=mother.birth_ms+3*FIVE
                else 'source_stop' if mother.side*(row['close']-mother.anchor)<=-mother.distance else None)
            if reason:
                retired.append(dict(time_ms=frame_ms,known_ms=known_ms,source_id=mother.source_id,
                    symbol=symbol,reason=reason,price=row['close']))
                del active[symbol]
    # Include original Bar.received_ms and metadata cutoffs in the digest. Never
    # replace them with the old frame's nominal time to pass a timely-input test.
    body=dict(frame_ms=frame_ms,known_ms=known_ms,universe_known_ms=universe_known_ms,
              universe_complete=universe_complete,observations=observations)
    evidence=dict(kind='STOCK_MISSED_SOURCE_FRAME',frame_ms=frame_ms,known_ms=known_ms,
        previous_frame_ms=observer.last_clock,previous_known_ms=observer.last_known_ms,
        input_sha256=hashlib.sha256(encode(body,profile='live')).hexdigest(),
        symbols=tuple(observer.symbols),missed=tuple(missed),retired=tuple(retired),
        created_sources=0,created_orders=0)
    # FeatureState.commit has no external operations. Keep only the bounded
    # feature states as rollback data; immutable births/history are not recopied.
    saved_features={s:deepcopy(observer.features[s]) for s in observer.symbols}
    try:
        for symbol in observer.symbols:observer.features[symbol].commit(previews[symbol][1])
    except BaseException:
        observer.features=saved_features
        raise
    observer.active=active;observer.previous=previous
    observer.lifecycle.extend(deepcopy(retired))
    observer.last_clock=frame_ms;observer.last_known_ms=known_ms;observer.frames+=1
    observer.history[frame_ms]={s:active[s].source_id if s in active else None for s in observer.symbols}
    observer.validity_history[frame_ms]={s:previews[s][0]['valid'] for s in observer.symbols}
    observer.known_history[frame_ms]=known_ms
    for t in tuple(observer.history):
        if t<frame_ms-FIVE:
            del observer.history[t];del observer.validity_history[t];del observer.known_history[t]
    return evidence


def recover_alt_frame(observer,frame_ms,window,*,known_ms):
    """Continue A787 leader/mother cooldown state without creating entry sources.

    A missed price-qualified mother consumes its original 24-hour cooldown,
    just as the ordinary observer consumes it before later price/flow gates.
    Those later gates are NOT evaluated or falsely described as passed here.
    """
    import math
    import numpy as np
    from .strategies.alt_sources import AltSourceObserver,HourlyWindow,HOUR,STEP,ranked
    from .account import PositionKey
    if (not isinstance(observer,AltSourceObserver) or type(frame_ms) is not int
            or frame_ms!=observer.last_clock+HOUR or frame_ms%HOUR!=STEP):
        raise InvalidState('山寨恢复必须连续续接原小时来源时钟')
    prior_known=observer.states[-1].get('known_ms',observer.states[-1]['decision_ms']) if observer.states else 0
    if type(known_ms) is not int or known_ms<frame_ms+MINUTE or known_ms<prior_known:
        raise InvalidState('山寨恢复处理时间倒退或尚未错过决策窗口')
    if (not isinstance(window,HourlyWindow) or type(window.end_ms) is not int
            or window.end_ms!=frame_ms-STEP or type(window.known_ms) is not int
            or not 0<=window.known_ms<=known_ms or window.universe_complete is not True):
        raise InvalidState('山寨恢复必须使用实际已知的完整小时观察')
    w=window
    if not w.names or len(set(w.names))!=len(w.names) or not {'BTCUSDT','ETHUSDT'}<=set(w.names):
        raise InvalidState('山寨恢复名单重复或缺少 BTC/ETH')
    for symbol in w.names:PositionKey(symbol,'LONG')
    if w.source_names is not None and (len(set(w.source_names))!=len(w.source_names) or not set(w.source_names)<=set(w.names)):
        raise InvalidState('山寨恢复信号名单不属于上下文名单')
    shape=(193,len(w.names))
    if any(not isinstance(a,np.ndarray) or a.shape!=shape for a in (w.close,w.high,w.low,w.quote,w.received_ms,w.absence)):
        raise InvalidState('山寨恢复必须包含连续 193 小时')
    if not np.isin(w.absence,(0,1)).all():raise InvalidState('山寨恢复不能把未知行情缺口当作无信号')
    present=w.absence==0;values=(w.close,w.high,w.low,w.quote)
    if any(not np.isfinite(a[present]).all() for a in values) or any(not (a[present]>0).all() for a in values[:3]) or not (w.quote[present]>=0).all():
        raise InvalidState('山寨恢复价格或成交额无效')
    if not ((w.low<=w.close)&(w.close<=w.high))[present].all() or any(not np.isnan(a[~present]).all() for a in values):
        raise InvalidState('山寨恢复行情矛盾或缺失价格被填补')
    ends=np.arange(w.end_ms-192*HOUR,w.end_ms+HOUR,HOUR)[:,None]
    if not np.issubdtype(w.received_ms.dtype,np.integer) or not ((w.received_ms>=ends)&(w.received_ms<=known_ms))[present].all():
        raise InvalidState('山寨恢复行情尚未收到或被回填到收盘之前')
    btc,eth=w.names.index('BTCUSDT'),w.names.index('ETHUSDT')
    with np.errstate(invalid='ignore',divide='ignore'):
        ratios=w.close[-169:,eth]/w.close[-169:,btc]
        br=w.close[-1,btc]/w.close[-25,btc]-1;er=w.close[-1,eth]/w.close[-25,eth]-1
    condition=bool(np.isfinite(ratios).all() and (ratios>0).all() and ratios[-1]>max(ratios[:-1]) and br>0 and er>0)
    event=condition and (observer.last_leader is None or w.end_ms-observer.last_leader>=24*HOUR)
    events=observer.events+([w.end_ms] if event else [])
    previous_events=[t for t in events if HOUR<=w.end_ms-t<=24*HOUR]
    eligible,rank,returns,q24=ranked(w,192,names=w.source_names)
    positive=sum(returns[j]>0 for j in eligible);last_mother=dict(observer.last_mother)
    count=observer.mother_count;checks=[];missed=[]
    for position,j in enumerate(rank,1):
        symbol=w.names[j];hist_hi=w.high[-7:-1,j]
        high6=float(max(hist_hi)) if np.isfinite(hist_hi).all() else float('nan')
        reason=('nonpositive_own_return' if returns[j]<=0 else 'no_six_hour_breakout' if not math.isfinite(high6) or w.close[-1,j]<=high6
            else 'mother_cooldown' if w.end_ms-last_mother.get(symbol,-10**18)<24*HOUR else None)
        checks.append(dict(symbol=symbol,signal_end_ms=w.end_ms,momentum_rank=position,
            first_rejection=reason or 'MISSED_DECISION_WINDOW',known_ms=known_ms,recovery=True))
        if reason:continue
        last_mother[symbol]=w.end_ms;count+=1
        missed.append(dict(symbol=symbol,signal_end_ms=w.end_ms,source_frame_ms=frame_ms,
            decision_ms=known_ms,known_ms=known_ms,momentum_rank=position,mother_id='A'+str(count).zfill(5),
            leadership_pass=bool(previous_events),first_rejection='MISSED_DECISION_WINDOW',
            later_entry_gates_evaluated=False,recovery=True))
    input_body=dict(frame_ms=frame_ms,known_ms=known_ms,window=w)
    evidence=dict(kind='ALT_MISSED_SOURCE_FRAME',frame_ms=frame_ms,known_ms=known_ms,
        previous_frame_ms=observer.last_clock,previous_known_ms=prior_known,
        input_sha256=hashlib.sha256(encode(input_body,profile='live')).hexdigest(),
        symbols=tuple(w.names),missed=tuple(missed),created_sources=0,created_orders=0,
        leader_event_ms=w.end_ms if event else None,later_entry_gates_evaluated=False)
    state=dict(signal_end_ms=w.end_ms,source_frame_ms=frame_ms,decision_ms=known_ms,known_ms=known_ms,
        leader_condition=condition,event=event,previous_event_ms=previous_events[-1] if previous_events else None,
        eligible_count=len(eligible),positive_count=int(positive),ranked_symbols=[w.names[j] for j in rank],
        unexplained_missing=0,universe_complete=True,conditional_history=observer.conditional_history,recovery=True)
    # All numeric/data validation is complete; append only missed observations.
    observer.last_clock=frame_ms
    if event:observer.last_leader=w.end_ms;observer.events.append(w.end_ms)
    observer.last_mother=last_mother;observer.mother_count=count
    observer.rank_checks.extend(checks);observer.decisions.extend(deepcopy(missed));observer.states.append(state)
    observer.source_evidence.append(dict(kind=evidence['kind'],frame_ms=frame_ms,known_ms=known_ms,
        input_sha256=evidence['input_sha256'],created_sources=0,later_entry_gates_evaluated=False))
    return evidence

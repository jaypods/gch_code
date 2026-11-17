"""Bounded continuation of missed source observations, no historic orders.

Uses the original stock/alt source math and original shadow venue. No audit
database, independent watchdog, financial replay or release proof is opened.
"""
from copy import deepcopy
import heapq

from .account import InvalidState
from .main_shadow import MainShadowModel
from .main_paper import MainPaperVenue
from .market import Bar
from .market_warmup import CandleNeed
from .main_warmup import main_needs, prepare_main
from .outage_recovery import recover_stock_frame, recover_alt_frame
from .strategies.main import MINUTE, CLOCK, SYMS
from .strategies.alt_sources import HOUR, STEP


def advance_missed_shadow(model, frame_ms, bars, *, known_ms, funding):
    if (not isinstance(model,MainShadowModel) or model.book.mode!='shadow' or model.book.initial!=1000.
            or frame_ms%MINUTE or frame_ms!=model.last_minute+2*MINUTE or known_ms<frame_ms+MINUTE):
        raise InvalidState('模型续接只处理尚未记录的已结束分钟')
    t=frame_ms-MINUTE;clock=t//CLOCK*CLOCK
    if (set(bars)!=set(SYMS) or any(not isinstance(b,Bar) or b.symbol!=s or b.start_ms!=t
            or b.period_ms!=MINUTE or b.received_ms>known_ms for s,b in bars.items())):
        raise InvalidState('模型续接缺少实际收到的已收盘行情')
    if model.book.last_clock not in ((clock,clock-CLOCK) if t%CLOCK==0 else (clock,)):
        raise InvalidState('模型时钟存在未处理的间隔')
    for event in funding:
        if not t<=event['time_ms']<frame_ms or not event['time_ms']<=event['received_ms']<=known_ms:
            raise InvalidState('模型资金费不属于该分钟或尚未收到')
    before=deepcopy(model.__dict__);book=model.book;orders=set(model.orders)
    def deliver():
        while book.queue and book.queue[0][0]<=known_ms:
            book._deliver(heapq.heappop(book.queue)[2],known_ms)
        book._settle_groups(known_ms)
    try:
        deliver();book.last_clock=clock
        MainPaperVenue.minute(model,t,bars,funding=funding,received_ms=known_ms,receipt_floor_ms=known_ms)
        deliver()
        if set(model.orders)!=orders:
            raise InvalidState('离线期间不能产生新的模型委托')
        del model.equity[:-2]
    except BaseException:
        saved_book=before.pop('book');book.__dict__.clear();book.__dict__.update(saved_book.__dict__)
        model.__dict__.clear();model.__dict__.update(before);model.book=book
        raise


class SimpleResume:
    def __init__(self, controller, revision, *, target_ms):
        if controller.cycles<1 or target_ms%MINUTE or target_ms<controller.last_clock+2*MINUTE:
            raise InvalidState('续接需要已运行的原状态和新的当前时点')
        self.revision=revision;self.target_ms=target_ms
        self.sources=dict(M784=deepcopy(controller.shadow_model),A787=deepcopy(controller.alt_observer),
                          S800=deepcopy(controller.stock.observer))
        self.main=None

    def retarget(self, target_ms):
        if target_ms<self.target_ms or target_ms%MINUTE:
            raise InvalidState('当前运行目标时点不能倒退')
        if (target_ms+STEP-1)//STEP != (self.target_ms+STEP-1)//STEP:
            self.main=None
        self.target_ms=target_ms

    def needs(self, inputs):
        sources=inputs.sources;cursor=self.target_ms-MINUTE
        five=cursor//STEP*STEP;hour=(cursor-STEP)//HOUR*HOUR+STEP
        needs=list(main_needs((self.target_ms+STEP-1)//STEP*STEP))
        first=self.sources['M784'].last_minute+MINUTE
        if first<cursor:
            needs.extend(CandleNeed('perpetual',s,'1m',first,cursor) for s in SYMS)
        observer=self.sources['S800']
        needs.extend(sources.stock_admission_needs(observer.symbols,through_ms=observer.last_clock))
        if observer.last_clock<five:
            for symbol in sources.stock_symbols:
                c=sources.contracts[symbol]
                begin=max(observer.last_clock,c.listed_ms//STEP*STEP)
                end=min(five,c.delivery_ms//STEP*STEP)
                if begin<end:needs.append(CandleNeed('perpetual',symbol,'5m',begin,end))
        alt=self.sources['A787']
        for end in (alt.last_clock+HOUR-STEP,hour-STEP):
            if end+STEP<=hour:needs.extend(sources.alt_hourly_needs(end))
        merged={}
        for n in needs:
            key=n.venue,n.symbol,n.interval
            old=merged.get(key)
            merged[key]=n if old is None else CandleNeed(*key,min(n.start_ms,old.start_ms),max(n.end_ms,old.end_ms))
        return tuple(merged.values())

    def step(self, inputs, data, *, now_ms):
        """One model/source frame or one current M784 warmup, then yield."""
        cursor=self.target_ms-MINUTE;five=cursor//STEP*STEP;hour=(cursor-STEP)//HOUR*HOUR+STEP
        sources=inputs.sources;market=inputs.store
        model=self.sources['M784'];frame=model.last_minute+2*MINUTE
        if frame<=cursor and now_ms>=frame+MINUTE:
            bars=inputs._bars('perpetual',SYMS,'1m',frame,now_ms)
            funding=data.funding(start_ms=frame-MINUTE,end_ms=frame,now_ms=now_ms)
            advance_missed_shadow(model,frame,bars,known_ms=now_ms,funding=funding)
            return 'MODEL_MINUTE'
        observer=self.sources['S800'];frame=observer.last_clock+STEP
        if frame<=five and now_ms>=frame+MINUTE:
            admission=sources.stock_admission(observer.symbols,through_ms=observer.last_clock,known_ms=now_ms)
            recover_stock_frame(observer,frame,sources.stock_frame(frame,known_ms=now_ms),known_ms=now_ms,
                universe_known_ms=sources.universe.received_ms,universe_complete=True,roster_admission=admission)
            return 'STOCK_FRAME'
        alt=self.sources['A787'];frame=alt.last_clock+HOUR
        if frame<=hour and now_ms>=frame+MINUTE:
            recover_alt_frame(alt,frame,sources.outage_hourly(frame-STEP,known_ms=now_ms),known_ms=now_ms)
            return 'ALT_FRAME'
        next_main=(self.target_ms+STEP-1)//STEP*STEP
        if self.main is None and now_ms<next_main:
            self.main=prepare_main(market,start_ms=next_main,now_ms=now_ms)
            return 'MAIN_WINDOW'
        return 'READY' if self.ready else 'WAIT_BOUNDARY'

    @property
    def ready(self):
        cursor=self.target_ms-MINUTE
        return (self.main is not None and self.sources['M784'].last_minute+MINUTE==cursor
                and self.sources['S800'].last_clock==cursor//STEP*STEP
                and self.sources['A787'].last_clock==(cursor-STEP)//HOUR*HOUR+STEP)

    def finish(self, engine, inputs, data, *, now_ms, request_current=None):
        if not self.ready or engine.revision!=self.revision:
            raise InvalidState('续接状态与当前策略版本不一致')
        def packet(c):
            if request_current is not None:
                plan=inputs.tick_plan(c,frame_ms=self.target_ms,now_ms=now_ms)
                request_current(plan['needs'],(self.target_ms-MINUTE,self.target_ms))
            return data.packet(inputs,c,frame_ms=self.target_ms,now_ms=now_ms)
        return engine.resume(self.sources,packet,
            frame_ms=self.target_ms,now_ms=now_ms,
            perpetual_bars=tuple(self.main.perpetual._bars.values()),spot_bars=tuple(self.main.spot._bars.values()),
            weekly_closes=tuple(self.main.weekly.completed.values()))

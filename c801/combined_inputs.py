"""Three-sleeve bootstrap and per-minute market packets for OwnerRuntime.

Financial capital is supplied from the reconciled original allocation and is
checked again by OwnerRuntime.initialize. No activation, credential access,
orders, inferred funding, invented calendar or backdated tick is performed.
The continuous owner scheduler still supplies certified funding/calendar input.
"""
from .account import InvalidState
from .combined_controller import CombinedController,MINUTE
from .main_warmup import main_needs,prepare_main
from .strategies.alt_sources import STEP,HOUR
from .market_warmup import CandleNeed
from .strategies.stock_sources import StockSourceObserver,LIQUIDITY_BARS


class CombinedPreparation:
    """Yield between bounded feature batches so account maintenance can run."""
    def __init__(self,inputs,*,start_ms,initial,cost=1):
        if type(start_ms) is not int or start_ms%STEP:raise InvalidState('组合预热起点错误')
        self.inputs=inputs;self.start_ms=start_ms;self.initial=dict(initial);self.cost=cost
        self.alt_needs=inputs.sources.alt_hourly_needs((start_ms-STEP)//HOUR*HOUR)
        self.alt_cursor=0;self.stock_cursor=start_ms-STEP-LIQUIDITY_BARS*STEP
        self.main=None;self.observer=None;self.controller=None;self.failed=False
        self.stock_begin=self.stock_cursor

    @property
    def ready(self):return self.controller is not None and not self.failed

    def retarget(self,start_ms):
        """Move a never-started bootstrap forward without repeating old features."""
        if self.failed or type(start_ms) is not int or start_ms%STEP or start_ms<self.start_ms:
            raise InvalidState('未启动策略的下一周期不能倒退')
        if start_ms==self.start_ms:return
        if self.observer is not None:
            if self.observer.births or self.observer.active:
                raise InvalidState('只能接续尚未产生历史入场信号的预热')
            # An old first observation becomes feature-only history. Clear its
            # momentum seed exactly as the original observer does in warmup.
            self.observer.start_ms=start_ms-STEP
            self.observer.warmup_frames+=self.observer.frames;self.observer.frames=0
            for feature in self.observer.features.values():feature.closes.clear()
            self.observer.previous={s:0 for s in self.observer.symbols}
        completed=set(self.alt_needs[:self.alt_cursor])
        fresh=self.inputs.sources.alt_hourly_needs((start_ms-STEP)//HOUR*HOUR)
        done=[n for n in fresh if n in completed];pending=[n for n in fresh if n not in completed]
        self.alt_needs=tuple(done+pending);self.alt_cursor=len(done)
        self.start_ms=start_ms;self.main=None;self.controller=None

    def pending_needs(self,*,stock_frames=None,alt_symbols=None):
        """Only unfinished windows; optional bounds match one computation batch."""
        from .market_warmup import CandleNeed
        if self.ready:return ()
        alt=self.alt_needs[self.alt_cursor:]
        if alt_symbols is not None and alt:return tuple(alt[:alt_symbols])
        needs=list(alt)
        if self.main is None:
            needs.extend(main_needs(self.start_ms))
            if stock_frames is not None:return tuple(needs)
        end=self.start_ms-STEP
        if stock_frames is not None:end=min(end,self.stock_cursor+(stock_frames-1)*STEP)
        for symbol in self.inputs.sources.stock_symbols:
            c=self.inputs.sources.contracts[symbol]
            begin=max(self.stock_cursor-STEP,c.listed_ms//STEP*STEP)
            last=min(end,c.delivery_ms//STEP*STEP)
            if begin<last:needs.append(CandleNeed('perpetual',symbol,'5m',begin,last))
        return tuple(needs)

    def progress(self):
        total=(self.start_ms-STEP-self.stock_begin)//STEP+1
        return dict(alt_complete=self.alt_cursor,alt_total=len(self.alt_needs),main_complete=self.main is not None,
                    stock_complete=min(total,(self.stock_cursor-self.stock_begin)//STEP),stock_total=total,ready=self.ready)

    def step(self,*,now_ms,stock_frames=32,alt_symbols=16):
        if (self.failed or type(now_ms) is not int or not self.start_ms-STEP<=now_ms<self.start_ms
                or type(stock_frames) is not int or not 1<=stock_frames<=64
                or type(alt_symbols) is not int or not 1<=alt_symbols<=64):
            raise InvalidState('分段预热时钟、批量或状态无效')
        if self.ready:return dict(outcome='CONTROLLER_READY')
        sources=self.inputs.sources
        try:
            sources._current(now_ms)
            if self.alt_cursor<len(self.alt_needs):
                stop=min(len(self.alt_needs),self.alt_cursor+alt_symbols)
                for n in self.alt_needs[self.alt_cursor:stop]:
                    self.inputs.store.window(n.venue,n.symbol,n.interval,start_ms=n.start_ms,end_ms=n.end_ms,now_ms=now_ms)
                self.alt_cursor=stop
                return dict(outcome='ALT_FEATURE_HISTORY_CHECKED',complete=stop,total=len(self.alt_needs))
            if self.main is None:
                self.main=prepare_main(self.inputs.store,start_ms=self.start_ms,now_ms=now_ms)
                return dict(outcome='MAIN_FEATURE_HISTORY_READY')
            if self.observer is None:
                self.observer=StockSourceObserver(symbols=sources.stock_symbols,start_ms=self.start_ms-STEP,warmup_start_ms=self.stock_cursor)
            for _ in range(stock_frames):
                if self.stock_cursor>self.start_ms-STEP:break
                self.observer.advance(self.stock_cursor,sources.stock_frame(self.stock_cursor,known_ms=now_ms),
                    universe_known_ms=sources.universe.received_ms,universe_complete=True,known_ms=now_ms)
                self.stock_cursor+=STEP
            if self.stock_cursor<=self.start_ms-STEP:
                return dict(outcome='STOCK_FEATURE_HISTORY_BUILDING',next_frame_ms=self.stock_cursor)
            if self.observer.births:raise InvalidState('预热意外产生历史入场来源')
            self.controller=CombinedController(start_ms=self.start_ms,initial=self.initial,perpetual=self.main.perpetual,
                spot=self.main.spot,stock_observer=self.observer,cost=self.cost)
            self.main.install(self.controller)
            return dict(outcome='CONTROLLER_READY')
        except Exception:
            self.failed=True;self.controller=None;raise


class CombinedInputs:
    def __init__(self,sources):self.sources=sources;self.store=sources.store

    def warmup_needs(self,start_ms):
        main=main_needs(start_ms);stock=self.sources.stock_needs(start_ms)
        end=(start_ms-STEP)//HOUR*HOUR
        alt=self.sources.alt_hourly_needs(end)
        return tuple(dict.fromkeys((*main,*stock,*alt)))

    def prepare(self,*,start_ms,now_ms,initial,cost=1):
        self.sources._current(now_ms)
        # Alt's old hourly data only warms a future window. Do not execute a
        # historical source decision at an invented early receipt timestamp.
        for need in self.sources.alt_hourly_needs((start_ms-STEP)//HOUR*HOUR):
            self.store.window(need.venue,need.symbol,need.interval,start_ms=need.start_ms,end_ms=need.end_ms,now_ms=now_ms)
        main=prepare_main(self.store,start_ms=start_ms,now_ms=now_ms)
        observer=self.sources.prepare_stock(start_ms=start_ms,now_ms=now_ms)
        controller=CombinedController(start_ms=start_ms,initial=initial,perpetual=main.perpetual,
            spot=main.spot,stock_observer=observer,cost=cost)
        main.install(controller)
        return controller

    def _bars(self,venue,symbols,interval,end,at):
        rows,missing=self.store.frame(venue,symbols,interval,end_ms=end,now_ms=at)
        if missing:raise InvalidState('组合必需行情未完整收到：'+','.join(missing))
        return {s:rows[s].bar for s in symbols}

    def tick_plan(self,controller,*,frame_ms,now_ms):
        """Missing-only prefetch for the next live clock, with no decisions.

        Stock uses the newly closed frame; M/A deliberately retain their
        original five-minute lag. Hourly ranking is derived only after its
        entire window is actually present, never from the eventual winners.
        """
        if (not isinstance(controller,CombinedController) or frame_ms!=controller.last_clock+MINUTE
                or type(now_ms) is not int or now_ms<0):
            raise InvalidState('行情预取必须对应下一个组合分钟')
        self.sources._current(now_ms)
        needs=list(self.sources.stock_admission_needs(controller.stock.observer.symbols,
            through_ms=controller.stock.observer.last_clock))
        def stock(interval,p):
            for symbol in self.sources.stock_symbols:
                c=self.sources.contracts[symbol]
                if frame_ms<=c.listed_ms or frame_ms-p>=c.delivery_ms:continue
                needs.append(CandleNeed('perpetual',symbol,interval,frame_ms-p,frame_ms))
        stock('1m',MINUTE)
        if frame_ms>controller.start_ms:
            needs.extend(CandleNeed('perpetual',s,'1m',frame_ms-MINUTE,frame_ms) for s in ('BTCUSDT','ETHUSDT'))
        resolved=True
        if frame_ms%STEP==0:
            end=frame_ms-STEP;stock('5m',STEP)
            needs.extend(CandleNeed(v,s,'5m',end-STEP,end) for v in ('perpetual','spot') for s in ('BTCUSDT','ETHUSDT'))
            active={o['symbol'] for oid,o in controller.alt.orders.items()
                if not o['entry_done'] or oid in controller.alt.cycles and not controller.alt.cycles[oid]['closed']}
            needs.extend(CandleNeed('perpetual',s,'5m',end-STEP,end) for s in sorted(active))
            if frame_ms==controller.alt_observer.last_clock+HOUR:
                hourly=self.sources.alt_hourly_needs(end);needs.extend(hourly)
                resolved=end<=now_ms and all(not self.store.missing(n.venue,n.symbol,n.interval,
                    start_ms=n.start_ms,end_ms=n.end_ms,now_ms=now_ms) for n in hourly)
                if resolved:needs.extend(self.sources.alt_feature_needs(end,known_ms=now_ms))
        return dict(frame_ms=frame_ms,needs=tuple(dict.fromkeys(needs)),features_resolved=resolved)

    def observed_packet(self,controller,*,frame_ms,received_ms,funding,lifecycle):
        from .funding_inputs import FundingInputs
        from .lifecycle_inputs import LifecycleInputs
        if not isinstance(funding,FundingInputs) or not isinstance(lifecycle,LifecycleInputs):
            raise InvalidState('运行输入必须绑定实际接收的资金费与下架日历记录')
        if lifecycle.source_digest!=self.sources.universe.source_digest:
            raise InvalidState('行情与下架日历并非同一份合约目录')
        if funding.start_ms!=controller.start_ms:raise InvalidState('资金费覆盖起点与组合启动时间不同')
        funding._healthy()
        # Check the catalog on every minute, even between alt strategy clocks.
        names=tuple(s for s in self.sources.crypto_names if s not in ('BTCUSDT','ETHUSDT'))
        calendars=lifecycle.calendars(names,now_ms=received_ms) if names else ()
        events=None if frame_ms==controller.start_ms else funding.seal(start_ms=frame_ms-MINUTE,end_ms=frame_ms,now_ms=received_ms)
        return self.packet(controller,frame_ms=frame_ms,received_ms=received_ms,
            shadow_funding=events,alt_calendars=calendars if frame_ms%STEP==0 else ())

    def packet(self,controller,*,frame_ms,received_ms,shadow_funding,alt_calendars):
        if (not isinstance(controller,CombinedController) or frame_ms!=controller.last_clock+MINUTE
                or type(received_ms) is not int or not frame_ms<=received_ms<frame_ms+MINUTE):
            raise InvalidState('组合输入不是下一个及时到达的分钟或股票观察范围变化')
        self.sources._current(received_ms)
        packet=dict(stock_minute_bars=self.sources.stock_minutes(frame_ms,known_ms=received_ms),
            current_stock_quality=self.sources.stock_quality(now_ms=received_ms),
            stock_quality_known_ms=self.sources.universe.received_ms)
        admission=self.sources.stock_admission(controller.stock.observer.symbols,
            through_ms=controller.stock.observer.last_clock,known_ms=received_ms)
        if admission is not None:packet['stock_roster_admission']=admission
        if frame_ms>controller.start_ms:
            if shadow_funding is None:raise InvalidState('影子模型的该分钟资金费覆盖尚未提供')
            packet.update(shadow_bars=self._bars('perpetual',('BTCUSDT','ETHUSDT'),'1m',frame_ms,received_ms),
                shadow_funding=tuple(shadow_funding))
        elif shadow_funding is not None:raise InvalidState('组合起点不能处理开始前的影子资金费')
        if frame_ms%STEP==0:
            if alt_calendars is None:raise InvalidState('山寨公告日历输入尚未提供')
            end=frame_ms-STEP;perpetual=self._bars('perpetual',('BTCUSDT','ETHUSDT'),'5m',end,received_ms)
            packet.update(spot_bars=tuple(self._bars('spot',('BTCUSDT','ETHUSDT'),'5m',end,received_ms).values()),
                stock_observations=self.sources.stock_frame(frame_ms,known_ms=received_ms),
                stock_universe_known_ms=self.sources.universe.received_ms,stock_universe_complete=True,
                alt_calendars=tuple(alt_calendars))
            needed={o['symbol'] for oid,o in controller.alt.orders.items()
                if not o['entry_done'] or oid in controller.alt.cycles and not controller.alt.cycles[oid]['closed']}
            if needed:perpetual.update(self._bars('perpetual',tuple(sorted(needed)),'5m',end,received_ms))
            packet['perpetual_bars']=list(perpetual.values())
            if frame_ms==controller.alt_observer.last_clock+HOUR:
                source=self.sources.alt_packet(end,known_ms=received_ms);packet.update(source)
                # Admission and the book's quote/risk checks see the same raw
                # prefix used to derive the source. Every duplicate is checked
                # by CausalMarket instead of silently replacing earlier bars.
                for bars in source['alt_five_minute'].values():packet['perpetual_bars'].extend(bars)
        elif alt_calendars:raise InvalidState('山寨五分钟时钟之外不能提交新日历')
        return packet

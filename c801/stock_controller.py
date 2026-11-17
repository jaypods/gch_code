"""One-minute S800 control loop shared by replay and owner-run adapters.

Only received prior bars and current known contract status enter the decision
loop. No native venue inventory, tape from the current minute or future account
outcomes are accepted. A failed cycle latches halted; the integration must
recover from the last durable checkpoint and reconciled broker evidence.
"""
from .account import InvalidState
from .market import Bar
from .strategies.stock import StockBook,StockReceipt,MINUTE,FIVE,decision_time
from .strategies.stock_sources import StockSourceObserver


class StockController:
    def __init__(self,observer:StockSourceObserver,*,start_ms,initial,cost=1):
        if start_ms%FIVE or observer.last_clock!=start_ms-FIVE or observer.start_ms>start_ms:
            raise InvalidState('股票控制器启动需要完整的上一根来源快照和特征预热')
        self.observer=observer;self.book=StockBook(start_ms=start_ms,initial=initial,cost=cost)
        self.halt_reason=None;self.source_audit=[]

    def receive(self,receipt:StockReceipt):
        try:return self.book.receive(receipt)
        except Exception as e:
            self.halt_reason=f'receipt:{type(e).__name__}:{e}';raise

    def advance(self,now,*,minute_bars,current_quality,quality_known_ms,source_observations=None,
                universe_known_ms=None,universe_complete=None,decision_ms=None,roster_admission=None):
        if self.halt_reason:raise InvalidState('股票控制器已暂停，需要从持久化检查点恢复并完成对账')
        try:
            if type(now) is not int or now!=self.book.last_clock+MINUTE:raise InvalidState('股票控制器分钟时钟必须连续')
            at=decision_time(now,decision_ms,last_ms=self.book.last_decision_ms)
            if roster_admission is not None:
                from .stock_admission import admit_stock_roster
                admit_stock_roster(self.observer,roster_admission,known_ms=at)
            if set(current_quality)!=set(self.observer.symbols) or any(type(x) is not bool for x in current_quality.values()) or type(quality_known_ms) is not int or not 0<=quality_known_ms<=at:
                raise InvalidState('股票控制器当前合约资格缺项或尚未可知')
            for s,b in minute_bars.items():
                if not isinstance(b,Bar) or b.symbol!=s or b.period_ms!=MINUTE or b.end_ms!=now or b.received_ms>at:
                    raise InvalidState('股票控制器收到未完成或错位分钟行情')
            out=None
            if now%FIVE==0:
                if source_observations is None:raise InvalidState('股票控制器缺少五分钟完整来源观察')
                out=self.observer.advance(now,source_observations,universe_known_ms=universe_known_ms,universe_complete=universe_complete,known_ms=at)
            elif source_observations is not None:raise InvalidState('股票控制器不能在五分钟未收盘时更新来源')
            view=self.observer.execution_view(now,current_quality=current_quality,quality_known_ms=quality_known_ms,decision_ms=at)
            # The inherited minute driver starts at an opening boundary. Its
            # first admission is after the first full 5m bar, not at startup.
            sources=out['sources'] if out is not None and now>self.book.start_ms else ()
            decision_bars={s:source_observations[s].bar for s in self.observer.symbols if source_observations[s].bar is not None} if sources else {}
            actions=self.book.advance(now,sources=sources,decision_bars=decision_bars,minute_bars=minute_bars,cancellation=view,decision_ms=at)
            if out is not None:
                self.source_audit.append(dict(time_ms=now,decision_ms=at,symbols=len(self.observer.symbols),active_sources=len(out['sources']),
                                              born_sources=[p.source_id for p in out['births']],startup_admission_suppressed=now==self.book.start_ms))
            return actions
        except Exception as e:
            # A source frame may have advanced before a downstream fault. Never
            # retry on that partial in-memory state or release staged actions.
            self.halt_reason=f'cycle:{type(e).__name__}:{e}';raise

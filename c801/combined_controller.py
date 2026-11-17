"""One causal minute clock for the three independent C801 strategy books.

Received market/receipt packets are inputs; broker and simulated venue state are
never exposed to decision logic. Funded actions and shadow-model actions remain
separate. The caller journals inputs and atomically checkpoints state + outbox
before dispatch. A partial failed cycle cannot be retried on this instance.
"""
from .account import InvalidState,SLEEVES,positive
from .market import Bar,CausalMarket
from .main_controller import MainController
from .main_shadow import MainShadowModel
from .stock_controller import StockController
from .strategies.main import MainReceipt
from .strategies.alt import AltBook,AltReceipt
from .strategies.alt_sources import AltSourceObserver,HourlyWindow,HOUR,STEP
from .strategies.stock import StockReceipt,decision_time
from .strategies.stock_sources import StockSourceObserver

MINUTE=60000


class CombinedController:
    def __init__(self,*,start_ms,initial,perpetual,spot,stock_observer,cost=1,conditional_history=False,stock_start_ms=None):
        if type(start_ms) is not int or start_ms<2*STEP or start_ms%STEP or set(initial)!=set(SLEEVES):raise InvalidState('组合起点或三策略本金不完整')
        stock_start_ms=start_ms if stock_start_ms is None else stock_start_ms
        if type(stock_start_ms) is not int or stock_start_ms<start_ms or stock_start_ms%STEP:
            raise InvalidState('股票覆盖起点必须是组合起点之后的五分钟边界')
        deferred=stock_start_ms>start_ms
        if deferred and (conditional_history is not True or stock_observer is not None):
            raise InvalidState('延后股票起点仅用于明确的历史覆盖，不能预装未来股票状态')
        if not isinstance(perpetual,CausalMarket) or not isinstance(spot,CausalMarket) or not deferred and not isinstance(stock_observer,StockSourceObserver):raise InvalidState('组合缺少已预热的行情或股票来源')
        if not deferred and (start_ms-STEP not in stock_observer.history or start_ms-STEP not in stock_observer.validity_history):
            raise InvalidState('组合启动缺少股票上一根来源与可成交资格快照，必须先预热')
        amounts={s:float(positive(initial[s])) for s in SLEEVES}
        self.main=MainController(start_ms=start_ms,initial=amounts['M784'],cost=cost)
        self.shadow_model=MainShadowModel(self.main.shadow)
        self.alt=AltBook(start_ms=start_ms,initial=amounts['A787'],cost=cost)
        next_hour=((start_ms-STEP+HOUR-1)//HOUR)*HOUR+STEP
        self.alt_observer=AltSourceObserver(start_ms=next_hour,conditional_history=conditional_history)
        self.stock_start_ms=stock_start_ms;self.stock_initial=amounts['S800'];self.stock_cost=cost
        self.stock=None if deferred else StockController(stock_observer,start_ms=start_ms,initial=amounts['S800'],cost=cost)
        self.perpetual=perpetual;self.spot=spot;self.start_ms=start_ms;self.last_clock=start_ms-MINUTE;self.last_decision_ms=start_ms-MINUTE
        self.halt_reason=None;self.cycles=0

    def activate_stock(self,observer,*,now_ms,decision_ms=None):
        """Attach S800 only when its historical coverage actually starts.

        Before then its allocation is idle cash, with no observer, indicators,
        orders or fake not-listed observations in the controller graph.
        This is not a live pause/resume or permission to skip missing minutes.
        """
        if (self.halt_reason or self.stock is not None or now_ms!=self.stock_start_ms
                or self.last_clock!=now_ms-MINUTE or not self.alt_observer.conditional_history):
            raise InvalidState('股票历史启动时点、状态或覆盖模式不符')
        at=decision_time(now_ms,decision_ms,last_ms=self.last_decision_ms)
        if not isinstance(observer,StockSourceObserver) or observer.last_known_ms>at:
            raise InvalidState('股票启动包含尚未可知的来源状态')
        from .state_codec import validate_time
        validate_time(observer,clock_ms=now_ms-MINUTE,received_ms=at)
        self.stock=StockController(observer,start_ms=now_ms,initial=self.stock_initial,cost=self.stock_cost)
        return self.stock

    def receive(self,sleeve,receipt,*,received_ms):
        if self.halt_reason:raise InvalidState('组合已暂停，先恢复检查点并补齐回报')
        try:
            if not isinstance(receipt,(MainReceipt,AltReceipt,StockReceipt)):raise InvalidState('组合回报类型错误')
            if type(received_ms) is not int or receipt.received_ms!=received_ms or receipt.available_ms<=self.last_decision_ms:
                raise InvalidState('组合回报实际接收时间不符或试图回填到已提交决策')
            if sleeve=='M784' and isinstance(receipt,MainReceipt):return self.main.funded.receive(receipt)
            if sleeve=='A787' and isinstance(receipt,AltReceipt):return self.alt.receive(receipt)
            if sleeve=='S800' and isinstance(receipt,StockReceipt):
                if self.stock is None:raise InvalidState('股票尚未启动，不能接收股票成交')
                return self.stock.receive(receipt)
            raise InvalidState('组合回报类型与策略归属不符')
        except Exception as exc:
            self.halt_reason='receipt:'+type(exc).__name__;raise

    def _compact_observations(self):
        """Bound working observations, never cash, receipts or source history.

        M784 needs 291 completed five-minute bars plus its one-bar lag;
        A787 uses a 13-bar feature packet and the current lagged quote. Weekly
        closes live independently in WeeklyReferences. Keep 300 periods for
        both markets, including the predecessor at the next decision boundary.
        The durable input journal and public archive retain original arrivals.
        """
        if self.last_clock%STEP==0:
            cutoff=self.last_clock-300*STEP
            for market in (self.perpetual,self.spot):
                market._bars={k:b for k,b in market._bars.items() if b.end_ms>=cutoff}
        # This curve is diagnostic only. Its cash, trades, positions, model
        # receipt queue and peak-dependent strategy books remain untouched.
        del self.shadow_model.equity[:-2]

    def advance(self,now,*,perpetual_bars=(),spot_bars=(),stock_minute_bars,current_stock_quality,stock_quality_known_ms,
                stock_observations=None,stock_universe_known_ms=None,stock_universe_complete=None,
                stock_roster_admission=None,
                alt_hourly=None,alt_five_minute=None,alt_flow=None,alt_calendars=(),shadow_bars=None,shadow_funding=(),decision_ms=None):
        if self.halt_reason:raise InvalidState('组合已暂停，禁止在部分更新的状态上重试')
        try:
            if type(now) is not int or now!=self.last_clock+MINUTE:raise InvalidState('组合分钟时钟必须连续')
            at=decision_time(now,decision_ms,last_ms=self.last_decision_ms)
            if self.stock is None:
                if now>=self.stock_start_ms:raise InvalidState('股票覆盖起点已到达，必须完成股票预热后才能推进')
                if (stock_minute_bars or current_stock_quality or stock_observations is not None or stock_roster_admission is not None
                        or stock_universe_known_ms is not None or stock_universe_complete is not None
                        or type(stock_quality_known_ms) is not int or not 0<=stock_quality_known_ms<=at):
                    raise InvalidState('股票覆盖开始前不能输入未来股票状态')
            if now>self.start_ms:
                if shadow_bars is None:raise InvalidState('组合缺少影子账户已收盘分钟，不能跳过机会占用时钟')
                self.shadow_model.observe_closed_minute(now-MINUTE,shadow_bars,received_ms=at,funding=shadow_funding)
            elif shadow_bars is not None or shadow_funding:raise InvalidState('启动时不能处理策略开始前的影子成交')
            for market,bars in ((self.perpetual,perpetual_bars),(self.spot,spot_bars)):
                for b in bars:
                    if not isinstance(b,Bar) or b.received_ms>at:raise InvalidState('组合收到尚未在当前时刻接收的行情')
                    market.add(b)
            actions={s:[] for s in SLEEVES};shadow=[]
            if now%STEP==0:
                main=self.main.advance(now,self.perpetual,self.spot)
                actions['M784']=main['funded_actions'];shadow=main['shadow_actions']
                self.shadow_model.dispatch(shadow)  # Audit return only; never goes to a broker.
                due=now==self.alt_observer.last_clock+HOUR
                sources=()
                if due:
                    if not isinstance(alt_hourly,HourlyWindow) or not isinstance(alt_five_minute,dict) or not isinstance(alt_flow,dict):raise InvalidState('组合缺少当前山寨小时完整观察及特征输入')
                    def five(symbol,end,clock):return alt_five_minute.get(symbol)
                    def flow(symbol,end,clock):return alt_flow.get(symbol)
                    sources=self.alt_observer.advance(now,alt_hourly,five_minute=five,flow=flow)
                elif alt_hourly is not None or alt_five_minute is not None or alt_flow is not None:raise InvalidState('组合不能在山寨小时决策时刻之外输入新来源')
                actions['A787']=self.alt.advance(now,self.perpetual,sources=sources,calendars=alt_calendars)
            elif alt_hourly is not None or alt_five_minute is not None or alt_flow is not None or alt_calendars:raise InvalidState('山寨五分钟时钟尚未到达')
            if self.stock is not None:
                actions['S800']=self.stock.advance(now,minute_bars=stock_minute_bars,current_quality=current_stock_quality,
                    quality_known_ms=stock_quality_known_ms,source_observations=stock_observations,
                    universe_known_ms=stock_universe_known_ms,universe_complete=stock_universe_complete,decision_ms=at,
                    roster_admission=stock_roster_admission)
            # Model frame timestamps remain explicit; no action is published
            # before the actual decision cutoff, including the lagged M/A books.
            for rows in actions.values():
                for action in rows:action.update(cycle_ms=now,known_ms=at)
            self.last_clock=now;self.last_decision_ms=at;self.cycles+=1
            self._compact_observations()
            return dict(clock_ms=now,decision_ms=at,strategy_actions=actions,shadow_actions=shadow)
        except Exception as exc:
            # Nothing is returned for dispatch from a failed cycle. The caller
            # restores the LAST COMMITTED graph and replays journaled inputs.
            self.halt_reason='cycle:'+type(exc).__name__;raise

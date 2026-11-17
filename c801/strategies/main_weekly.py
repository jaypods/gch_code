"""M784 union of weekly downcross and reclaim signals, with known-time inputs.

Both legs trade the same futures symbol. 'spot' is the source of one limit
reference, not an instruction to buy a separate spot asset or borrow its funds.
"""
from dataclasses import dataclass
from fractions import Fraction
import math
from ..account import InvalidState
from ..market import CausalMarket
from .main import CLOCK, SYMS, MainSource

ORIGIN = 1704067200000  # Monday 2024-01-01 UTC, the original weekly grid.
WEEK = 7 * 24 * 3_600_000


@dataclass(frozen=True)
class WeeklyClose:
    symbol: str
    market: str
    week_start_ms: int
    price: float
    received_ms: int

    def validate(self):
        if self.symbol not in SYMS or self.market not in ('perpetual', 'spot'):
            raise InvalidState('周线价格来源错误')
        if (self.week_start_ms - ORIGIN) % WEEK or self.received_ms < self.week_start_ms + WEEK:
            raise InvalidState('周线尚未收盘或网格错误')
        if not math.isfinite(self.price) or self.price <= 0:
            raise InvalidState('周收盘价无效')


class WeeklyReferences:
    def __init__(self):
        self.completed = {}

    def add(self, close: WeeklyClose):
        close.validate()
        key = (close.symbol, close.market, close.week_start_ms)
        previous = self.completed.get(key)
        if previous is not None:
            if previous.price != close.price or close.received_ms < previous.received_ms:
                raise InvalidState('已收到的周线被修改或回填了更早可用时间')
            return False
        self.completed[key] = close
        return True

    def observe_completed_week(self,perpetual,spot,*,week_end_ms,now_ms):
        """Roll a completed week from its actually received final 5m close.

        Daily warmup does not invent intraday prices. Once running, the final
        closed 5m bar supplies that same weekly close, after the original lag.
        Build and validate all four inputs before mutating any reference.
        """
        if (type(week_end_ms) is not int or type(now_ms) is not int
                or (week_end_ms-ORIGIN)%WEEK or week_end_ms+CLOCK>now_ms):
            raise InvalidState('完整周线尚未满足原策略发布时间')
        week=week_end_ms-WEEK;closes=[]
        for kind,market in (('perpetual',perpetual),('spot',spot)):
            for symbol in SYMS:
                if (symbol,kind,week) in self.completed:continue
                b=market.window(symbol,CLOCK,week_end_ms,1,now_ms=now_ms,lag_ms=CLOCK)[0]
                if b.quote_volume<=0:raise InvalidState('周收盘参考缺少真实成交')
                close=WeeklyClose(symbol,kind,week,float(b.close),max(b.received_ms,week_end_ms+CLOCK))
                close.validate();closes.append(close)
        for close in closes:self.add(close)
        return len(closes)

    def live30(self, symbol, kind, bar, now_ms):
        if symbol != bar.symbol or bar.period_ms != CLOCK or bar.received_ms > now_ms:
            raise InvalidState('当前五分钟价格尚未收到或周期错误')
        week = ORIGIN + ((bar.start_ms - ORIGIN) // WEEK) * WEEK
        history = []
        for prior in range(week - 29 * WEEK, week, WEEK):
            observation = self.completed.get((symbol, kind, prior))
            if observation is None or observation.received_ms > now_ms:
                raise InvalidState('缺少当时已知的二十九根完成周线')
            history.append(observation)
        values = [r.price for r in history]
        price = float(bar.close)
        level = (math.fsum(values) + price) / 30
        exact = (sum((Fraction(str(v)) for v in values), Fraction(0)) + Fraction(str(price))) / 30
        if not math.isfinite(level) or level <= 0:
            raise InvalidState('动态周线参考无效')
        # A rounding disagreement at a crossing is a data/precision issue to
        # inspect, not permission to choose the profitable classification.
        if (price > level) != (Fraction(str(price)) > exact):
            raise InvalidState('周线穿越判断在精度边界上不一致')
        return level, max(bar.received_ms, *(r.received_ms for r in history))

    def sources(self, perpetual: CausalMarket, spot: CausalMarket, *, source_end_ms, now_ms):
        if source_end_ms % CLOCK or now_ms != source_end_ms + CLOCK:
            raise InvalidState('周线信号必须在原定五分钟延迟后处理')
        if source_end_ms <= ORIGIN + CLOCK:
            return ()  # The original stream has no previous bar at its first cell.
        signals = []
        for symbol in SYMS:
            previous, current = perpetual.window(symbol, CLOCK, source_end_ms, 2, now_ms=now_ms, lag_ms=CLOCK)
            previous_level, previous_known = self.live30(symbol, 'perpetual', previous, now_ms)
            level, known = self.live30(symbol, 'perpetual', current, now_ms)
            above_before, above_now = float(previous.close) > previous_level, float(current.close) > level
            if above_before == above_now:
                continue
            if previous.quote_volume <= 0 or current.quote_volume <= 0:
                raise InvalidState('周线穿越来源缺少真实合约成交')
            spot_current = spot.window(symbol, CLOCK, source_end_ms, 1, now_ms=now_ms, lag_ms=CLOCK)[0]
            spot_level, spot_known = self.live30(symbol, 'spot', spot_current, now_ms)
            family = 'weekly_reclaim' if above_now else 'weekly_downcross'
            signals.append(MainSource(symbol, 1, source_end_ms, max(source_end_ms+CLOCK, previous_known, known, spot_known),
                                      float(current.close), level, f'{symbol}:{source_end_ms}:{family}', family, spot_level))
        return tuple(signals)

"""Arrival-time aware OHLCV inputs shared by historical replay and streaming.

There is no forward fill, interpolation, partial-bar lookahead or silent revision.
REST backfill receives its real local arrival time, not its old candle timestamp.
"""
from dataclasses import dataclass
from decimal import Decimal
from .account import InvalidState, PositionKey, nonnegative, positive

MINUTE = 60_000


@dataclass(frozen=True)
class Bar:
    symbol: str
    start_ms: int
    end_ms: int  # exclusive boundary
    received_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal

    def __post_init__(self):
        PositionKey(self.symbol, "BOTH")
        for field in ("start_ms", "end_ms", "received_ms"):
            if type(getattr(self, field)) is not int:
                raise InvalidState("行情时间必须为毫秒整数")
        if not 0 <= self.start_ms < self.end_ms <= self.received_ms:
            raise InvalidState("行情尚未收盘或时间错误")
        period = self.end_ms - self.start_ms
        if period % MINUTE or self.start_ms % period:
            raise InvalidState("行情周期或起点未对齐")
        for field in ("open", "high", "low", "close"):
            object.__setattr__(self, field, positive(getattr(self, field)))
        for field in ("volume", "quote_volume"):
            object.__setattr__(self, field, nonnegative(getattr(self, field)))
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise InvalidState("行情高低价矛盾")
        if (self.volume == 0) != (self.quote_volume == 0):
            raise InvalidState("成交量与成交额是否为零不一致")

    @property
    def period_ms(self):
        return self.end_ms - self.start_ms

    def content(self):
        return (self.open, self.high, self.low, self.close, self.volume, self.quote_volume)


class CausalMarket:
    def __init__(self):
        self._bars = {}

    def add(self, bar: Bar):
        key = (bar.symbol, bar.period_ms, bar.end_ms)
        old = self._bars.get(key)
        if old is not None:
            if old.content() != bar.content():
                raise InvalidState("同一根已收到 K 线出现修订，需要暂停并核查来源")
            if bar.received_ms < old.received_ms:
                raise InvalidState("不能把补到的数据伪装成更早收到")
            return False
        self._bars[key] = bar
        return True

    def window(self, symbol, period_ms, end_ms, count, *, now_ms, lag_ms=0):
        if type(count) is not int or count <= 0 or period_ms <= 0 or period_ms % MINUTE or end_ms % period_ms or lag_ms < 0:
            raise InvalidState("行情窗口参数错误")
        if end_ms + lag_ms > now_ms:
            raise InvalidState("行情收盘或策略延迟尚未到达")
        result = []
        for end in range(end_ms - (count - 1) * period_ms, end_ms + 1, period_ms):
            bar = self._bars.get((symbol, period_ms, end))
            if bar is None or bar.received_ms > now_ms:
                raise InvalidState("行情存在缺口或尚未收到")
            result.append(bar)
        return tuple(result)

    def aggregate(self, symbol, source_period_ms, target_period_ms, end_ms, *, now_ms):
        if target_period_ms <= source_period_ms or target_period_ms % source_period_ms or end_ms % target_period_ms:
            raise InvalidState("聚合周期错误")
        parts = self.window(symbol, source_period_ms, end_ms, target_period_ms // source_period_ms, now_ms=now_ms)
        # Empty-minute prices are archival placeholders, not traded extremes.
        # Keep every interval for continuity/arrival checks; price aggregation
        # uses prints only when at least one constituent actually traded.
        printed = tuple(b for b in parts if b.quote_volume > 0) or parts
        return Bar(symbol, end_ms - target_period_ms, end_ms, max(b.received_ms for b in parts),
                   printed[0].open, max(b.high for b in printed), min(b.low for b in printed), printed[-1].close,
                   sum((b.volume for b in parts), Decimal(0)), sum((b.quote_volume for b in parts), Decimal(0)))

"""Strict public-candle intake for live warmup, replay provenance and charts.

REST range is closed before sending, and each bar retains actual arrival time.
Missing intervals are explicit; not one is forward-filled. Partial chart candles
must use a separate future streaming view, never this strategy input container.
Official REST shapes/weights checked against Binance documentation 2026-11-10.
"""
from dataclasses import dataclass
from decimal import Decimal
from .account import InvalidState, nonnegative
from .binance_transport import CANDLE_PERIODS, WireReply, validate_parameters
from .market import Bar


@dataclass(frozen=True)
class CandleObservation:
    bar: Bar
    trades: int
    buy_volume: Decimal
    buy_quote: Decimal

    def content(self):
        return self.bar.content()+(self.trades,self.buy_volume,self.buy_quote)


@dataclass(frozen=True)
class CandlePage:
    symbol: str
    interval: str
    start_ms: int
    end_ms: int  # exclusive, unlike Binance's inclusive endTime
    sent_ms: int
    received_ms: int
    rows: tuple[CandleObservation,...]
    missing_starts: tuple[int,...]

    @property
    def complete(self):return not self.missing_starts


def parse_candles(reply, *, symbol, interval, start_ms, end_ms, limit):
    validate_parameters('QUERY_CANDLES',dict(symbol=symbol,interval=interval,
        startTime=start_ms,endTime=end_ms-1,limit=limit))
    if not isinstance(reply,WireReply) or not end_ms<=reply.sent_ms<=reply.received_ms or type(reply.body) is not list:
        raise InvalidState('K 线读取尚未结束、时钟异常或回复格式错误')
    period=CANDLE_PERIODS[interval];expected=set(range(start_ms,end_ms,period));rows=[];prior=-1
    if len(reply.body)>limit:raise InvalidState('K 线回复超过请求条数')
    for row in reply.body:
        if type(row) is not list or len(row)!=12:raise InvalidState('K 线字段数量错误')
        if any(type(row[i]) is not int for i in (0,6,8)) or row[8]<0:
            raise InvalidState('K 线时间或成交笔数错误')
        if row[0] not in expected or row[0]<=prior or row[6]+1!=row[0]+period:
            raise InvalidState('K 线重复、越界、倒序或周期错误')
        b=Bar(symbol,row[0],row[6]+1,reply.received_ms,*row[1:6],row[7])
        buy_volume,buy_quote=nonnegative(row[9]),nonnegative(row[10])
        if buy_volume>b.volume or buy_quote>b.quote_volume or (buy_volume==0)!=(buy_quote==0):
            raise InvalidState('主动买入量超出总成交量或金额矛盾')
        if (row[8]==0)!=(b.volume==0):raise InvalidState('成交笔数与成交量矛盾')
        rows.append(CandleObservation(b,row[8],buy_volume,buy_quote));prior=row[0]
    missing=tuple(sorted(expected-{r.bar.start_ms for r in rows}))
    return CandlePage(symbol,interval,start_ms,end_ms,reply.sent_ms,reply.received_ms,tuple(rows),missing)


class PublicMarket:
    """Shares the very same transport quota/clock as account and broker work."""
    def __init__(self,transport):self.transport=transport

    def candles(self,symbol,interval,*,start_ms,end_ms):
        if interval not in CANDLE_PERIODS:raise InvalidState('不支持该 K 线周期')
        period=CANDLE_PERIODS[interval];count=(end_ms-start_ms)//period
        params=dict(symbol=symbol,interval=interval,startTime=start_ms,endTime=end_ms-1,limit=count)
        validate_parameters('QUERY_CANDLES',params)
        reply=self.transport.request('QUERY_CANDLES',params)
        return parse_candles(reply,symbol=symbol,interval=interval,start_ms=start_ms,end_ms=end_ms,limit=count)

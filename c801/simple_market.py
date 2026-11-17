"""Bounded candle access for the unchanged strategy input builders.

Only requested windows are read. Backfill requests cover actual missing closed
candles, and never run an audit scan or change an original arrival timestamp.
"""
from decimal import Decimal as D
from pathlib import Path
import hashlib
import json
import sqlite3

from .account import InvalidState, PositionKey
from .market import Bar, CausalMarket
from .public_market import CandleObservation, parse_candles
from .simple_store import SimpleStore, PERIODS


class SimpleMarket:
    def __init__(self, store):
        if not isinstance(store, SimpleStore):
            raise InvalidState('行情需要精简运行存储')
        self.store = store

    @staticmethod
    def _range(venue, symbol, interval, start_ms, end_ms, now_ms):
        PositionKey(symbol, 'BOTH')
        period = PERIODS.get(interval)
        if (venue not in ('spot','perpetual') or period is None
                or any(type(v) is not int for v in (start_ms,end_ms,now_ms))
                or not 0 <= start_ms < end_ms <= now_ms
                or start_ms % period or end_ms % period):
            raise InvalidState('所需K线范围必须对齐且已经收盘')
        return period

    @staticmethod
    def _observation(row):
        if any(name not in row for name in ('trades','buy_volume','buy_quote')):
            raise InvalidState('K线缺少策略需要的真实成交量字段')
        bar = Bar(row['symbol'],row['start_ms'],row['end_ms'],row['received_ms'],
                  *(D(row[k]) for k in ('open','high','low','close','volume','quote_volume')))
        count=int(row['trades']);buy_volume=D(row['buy_volume']);buy_quote=D(row['buy_quote'])
        if (count < 0 or not 0 <= buy_volume <= bar.volume
                or not 0 <= buy_quote <= bar.quote_volume
                or (buy_volume == 0) != (buy_quote == 0)
                or (count == 0) != (bar.volume == 0)):
            raise InvalidState('K线成交量字段矛盾')
        return CandleObservation(bar,count,buy_volume,buy_quote)

    def window(self, venue, symbol, interval, *, start_ms, end_ms, now_ms, require_complete=True):
        period=self._range(venue,symbol,interval,start_ms,end_ms,now_ms)
        rows=self.store.candles(venue,symbol,interval,start_ms=start_ms,end_ms=end_ms,asof_ms=now_ms)
        observed=tuple(self._observation(row) for row in rows)
        present={o.bar.start_ms for o in observed}
        missing=tuple(t for t in range(start_ms,end_ms,period) if t not in present)
        if require_complete and missing:
            raise InvalidState(f'{symbol} {interval} 缺少 {len(missing)} 根已收盘K线')
        return observed,missing

    def missing(self, venue, symbol, interval, *, start_ms, end_ms, now_ms):
        period=self._range(venue,symbol,interval,start_ms,end_ms,now_ms)
        # Intake already validates candle contents. A gap query needs only
        # indexed timestamps, not a decode/verification of every old OHLC row.
        present={r[0] for r in self.store.db.execute('''SELECT DISTINCT start_ms FROM candles
            WHERE venue=? AND symbol=? AND interval=? AND start_ms>=? AND start_ms<? AND end_ms<=?
            AND end_ms<=? AND received_ms<=?''',(venue,symbol,interval,start_ms,end_ms,end_ms,now_ms,now_ms))}
        return tuple(t for t in range(start_ms,end_ms,period) if t not in present)

    def frame(self, venue, symbols, interval, *, end_ms, now_ms):
        symbols=tuple(symbols)
        if not symbols or len(set(symbols)) != len(symbols) or interval not in PERIODS:
            raise InvalidState('行情品种或周期错误')
        result={}
        for symbol in symbols:
            rows,_=self.window(venue,symbol,interval,start_ms=end_ms-PERIODS[interval],
                               end_ms=end_ms,now_ms=now_ms,require_complete=False)
            if rows: result[symbol]=rows[0]
        return result,tuple(s for s in symbols if s not in result)

    def load_into(self, market, venue, symbol, interval, *, start_ms, end_ms, now_ms):
        if not isinstance(market,CausalMarket):
            raise InvalidState('目标必须是策略行情容器')
        rows,_=self.window(venue,symbol,interval,start_ms=start_ms,end_ms=end_ms,now_ms=now_ms)
        for row in rows: market.add(row.bar)
        return len(rows)

    def verify_windows(self, needs, *, now_ms):
        # Compatibility with original feature warmup: check only its requested
        # candles. This is explicitly not an audit-chain or release certificate.
        digest=hashlib.sha256();count=0
        for need in needs:
            rows,_=self.window(need.venue,need.symbol,need.interval,
                start_ms=need.start_ms,end_ms=need.end_ms,now_ms=now_ms)
            for o in rows:
                key=(need.venue,need.symbol,need.interval,o.bar.start_ms,o.bar.received_ms,
                     tuple(map(str,o.content())))
                digest.update(json.dumps(key,separators=(',',':')).encode());count+=1
        return dict(events=count,digest=digest.hexdigest(),faulted=False,scope='requested_candles_only')

    def import_windows(self, legacy_path, needs, *, now_ms):
        """Read only indexed recent windows from the previous public cache."""
        legacy_path=Path(legacy_path).resolve()
        if not legacy_path.is_file():return 0
        db=sqlite3.connect(legacy_path.as_uri()+'?mode=ro',uri=True,timeout=3)
        db.execute('PRAGMA query_only=ON');imported=0
        try:
            for need in needs:
                p=self._range(need.venue,need.symbol,need.interval,need.start_ms,need.end_ms,now_ms)
                missing=set(self.missing(need.venue,need.symbol,need.interval,
                    start_ms=need.start_ms,end_ms=need.end_ms,now_ms=now_ms))
                if not missing:continue
                observations=[]
                rows=db.execute('''SELECT start_ms,available_ms,body FROM market_candles
                    WHERE venue=? AND symbol=? AND interval=? AND start_ms>=? AND start_ms<?
                        AND available_ms<=? ORDER BY start_ms''',
                    (need.venue,need.symbol,need.interval,need.start_ms,need.end_ms,now_ms))
                for stamp,available,raw in rows:
                    if stamp not in missing:continue
                    b=json.loads(raw)
                    if len(b)!=12 or b[:3]!=[stamp,stamp+p,available]:
                        raise InvalidState('已有K线索引与内容不一致')
                    row=dict(symbol=need.symbol,start_ms=stamp,end_ms=stamp+p,received_ms=available,
                        **dict(zip(('open','high','low','close','volume','quote_volume','trades','buy_volume','buy_quote'),b[3:])))
                    observations.append(self._observation(row))
                self.store.put_candles(need.venue,need.interval,observations)
                imported+=len(observations)
            return imported
        finally:
            db.close()

    def fetch_missing_page(self, transport, need):
        """One bounded public request; return remaining holes explicitly."""
        now=transport.now_ms()
        missing=self.missing(need.venue,need.symbol,need.interval,
            start_ms=need.start_ms,end_ms=need.end_ms,now_ms=now)
        if not missing:return dict(requested=0,received=0,remaining=0)
        period=PERIODS[need.interval];start=missing[0];end=start+period
        wanted=set(missing)
        while end in wanted and (end-start)//period<499:end+=period
        count=(end-start)//period
        operation='QUERY_SPOT_CANDLES' if need.venue=='spot' else 'QUERY_CANDLES'
        reply=transport.request(operation,dict(symbol=need.symbol,interval=need.interval,
            startTime=start,endTime=end-1,limit=count))
        page=parse_candles(reply,symbol=need.symbol,interval=need.interval,
            start_ms=start,end_ms=end,limit=count)
        self.store.put_candles(need.venue,need.interval,page.rows)
        remaining=self.missing(need.venue,need.symbol,need.interval,
            start_ms=need.start_ms,end_ms=need.end_ms,now_ms=reply.received_ms)
        return dict(requested=count,received=len(page.rows),remaining=len(remaining))

"""Public market input for the direct runtime, independent of the website.

Only current subscriptions and explicitly requested indicator windows are read.
Already received candles are reused. A missing candle is never filled with a
made-up price, and no historical trading/audit replay is started here.
"""
import time

from .account import InvalidState, PositionKey
from .binance_transport import WireReply
from .market_metadata import parse_exchange_info
from .market_stream import MarketStream, KlineEvent
from .market_fleet import subscription_plan
from .simple_market import SimpleMarket
from .spot_mapping import SpotPair


class SimpleMappings:
    def __init__(self, store):
        self.store = store

    def record(self, futures, spot):
        universe = parse_exchange_info(futures)
        if (not isinstance(spot, WireReply) or spot.http_status != 200
                or not 0 <= spot.sent_ms <= spot.received_ms
                or not isinstance(spot.body, dict)
                or not isinstance(spot.body.get('symbols'), list) or not spot.body['symbols']):
            raise InvalidState('完整现货目录尚未收到')
        now = max(futures.received_ms, spot.received_ms)
        if now - min(futures.received_ms, spot.received_ms) > 300000:
            raise InvalidState('现货和合约目录接收时间相隔过久')
        seen, assets = set(), {}
        for row in spot.body['symbols']:
            if not isinstance(row, dict) or not isinstance(row.get('symbol'), str) or row['symbol'] in seen:
                raise InvalidState('现货目录品种无效或重复')
            seen.add(row['symbol'])
            if row.get('quoteAsset') != 'USDT' or row.get('status') != 'TRADING' or row.get('isSpotTradingAllowed') is not True:
                continue
            symbol, asset = row['symbol'], row.get('baseAsset')
            PositionKey(symbol, 'BOTH')
            if not isinstance(asset, str) or not asset.isalnum() or symbol != asset + 'USDT' or asset in assets:
                raise InvalidState('现货资产身份不明确')
            assets[asset] = symbol
        old = self.store.value('spot_mappings', {})
        if old and now < old['received_ms']:
            raise InvalidState('目录接收时间不能倒退')
        previous = {r['perpetual_symbol']: r for r in old.get('identities', ())}
        active = {c.symbol for c in universe.contracts if c.category in ('main', 'alt') and c.tradable(now_ms=now)}
        identities, unmatched = [], []
        for row in sorted(futures.body['symbols'], key=lambda r: r['symbol']):
            symbol = row['symbol']
            if symbol not in active:
                continue
            asset = row.get('baseAsset')
            if not isinstance(asset, str) or not asset.isalnum():
                raise InvalidState('合约缺少实际基础资产')
            if asset not in assets:
                unmatched.append(symbol)
                continue
            prior = previous.get(symbol)
            known = prior['known_ms'] if prior and (prior['asset'], prior['spot_symbol']) == (asset, assets[asset]) else now
            identities.append(dict(perpetual_symbol=symbol, spot_symbol=assets[asset], asset=asset, known_ms=known))
        self.store.set_value('spot_mappings', dict(identities=identities, unmatched=unmatched,
            received_ms=now, futures_received_ms=futures.received_ms, spot_received_ms=spot.received_ms))
        return universe

    def current(self, *, now_ms):
        record = self.store.value('spot_mappings')
        if record is None or any(not 0 <= now_ms - record[k] <= 300000 for k in ('futures_received_ms', 'spot_received_ms')):
            raise InvalidState('需要当前现货与合约目录')
        return {r['perpetual_symbol']: SpotPair(**r) for r in record['identities']}


class SimpleFeed:
    def __init__(self, transport, store, *, legacy_path=None, stream_factory=MarketStream, monotonic=time.monotonic):
        if (getattr(transport, '_credentials', None) is not None or getattr(transport, 'credentials', None) is not None
                or getattr(transport, 'mutations_enabled', False)):
            raise InvalidState('行情线程只使用公开接口')
        self.transport, self.store = transport, store
        self.market, self.mappings = SimpleMarket(store), SimpleMappings(store)
        self.legacy_path = legacy_path
        self.stream_factory, self.monotonic = stream_factory, monotonic
        self.peers, self.subscriptions = [], set()
        self.cursor = 0
        self.closed = False
        self._imported = set()
        self._retry = {}

    def refresh_catalog(self):
        futures = self.transport.request('QUERY_MARKET_RULES', {})
        spot = self.transport.request('QUERY_SPOT_MARKETS', dict(permissions='SPOT',showPermissionSets='false',symbolStatus='TRADING'))
        universe=self.mappings.record(futures, spot)
        self.store.set_value('market_catalog',dict(body=futures.body,sent_ms=futures.sent_ms,received_ms=futures.received_ms))
        return universe

    def subscribe(self, sources, *, now_ms):
        if self.closed:
            raise InvalidState('行情输入已关闭')
        plan = subscription_plan(sources, now_ms=now_ms)
        # Add newly listed contracts without disconnecting working streams.
        added = set(plan.subscriptions) - self.subscriptions
        for venue in ('perpetual', 'spot'):
            pairs = sorted((s, i) for v, s, i in added if v == venue)
            for start in range(0, len(pairs), 200):
                self.peers.append(dict(stream=self.stream_factory(self.transport, self, tuple(pairs[start:start+200]), venue=venue),
                                       retry_ms=0, failures=0))
        self.subscriptions.update(added)
        return len(added)

    def put_closed(self, venue, event):
        if not isinstance(event, KlineEvent) or not event.closed or event.venue != venue:
            raise InvalidState('只有实际收盘消息可以进入策略行情')
        self.store.put_candles(venue, event.interval, (event.observation,))

    def poll(self, *, budget_ms=30, maximum_events=1000):
        """Fair, bounded drain. At most one reconnect attempt per invocation."""
        if self.closed:
            return dict(events=0, closed=0, connected=0)
        if budget_ms <= 0 or maximum_events < 1:
            raise InvalidState('行情轮询预算错误')
        stop = self.monotonic() + budget_ms / 1000
        events = closes = idle = 0
        attempted = False
        while self.peers and events < maximum_events and self.monotonic() < stop and idle < len(self.peers):
            peer = self.peers[self.cursor % len(self.peers)]
            self.cursor += 1
            stream = peer['stream']
            if stream._socket is None:
                now = self.transport.now_ms()
                if not attempted and now >= peer['retry_ms']:
                    attempted = True
                    try:
                        stream.start()
                    except Exception:
                        stream.close('CONNECTION_FAILED')
                    peer['failures'] += 1
                    peer['retry_ms'] = self.transport.now_ms() + min(60000, 1000 * 2 ** min(peer['failures'], 6))
                idle += 1
                continue
            result = stream.poll()
            if isinstance(result, KlineEvent):
                events += 1
                closes += int(result.closed)
                idle = 0
                peer['failures'] = 0
            else:
                idle += 1
        return dict(events=events, closed=closes, connected=sum(p['stream']._socket is not None for p in self.peers))

    def fill_one(self, needs):
        """Import a requested cache window once, then fetch one missing page.

        The caller can service orders between pages. Repeated empty venue
        replies remain explicit missing data, never a full-history restart.
        """
        now = self.transport.now_ms()
        blocked = None
        needs=tuple(needs)
        previous=getattr(self,'_fill_after',None)
        identities=[(n.venue,n.symbol,n.interval) for n in needs]
        if previous in identities:
            start=identities.index(previous)+1
            needs=needs[start:]+needs[:start]
        for need in needs:
            key = (need.venue, need.symbol, need.interval, need.start_ms, need.end_ms)
            if self.legacy_path is not None and key not in self._imported:
                self.market.import_windows(self.legacy_path, (need,), now_ms=now)
                self._imported.add(key)
            missing=self.market.missing(need.venue, need.symbol, need.interval, start_ms=need.start_ms, end_ms=need.end_ms, now_ms=now)
            if missing:
                gap=(need.venue,need.symbol,need.interval,missing[0])
                if self._retry.get(gap,0)>now:
                    blocked=dict(complete=False,symbol=need.symbol,interval=need.interval,requested=0,received=0,remaining=len(missing))
                    continue
                self._fill_after=(need.venue,need.symbol,need.interval)
                try:
                    result = self.market.fetch_missing_page(self.transport, need)
                except Exception:
                    self._retry[gap]=self.transport.now_ms()+30000
                    raise
                if not result['received']:self._retry[gap]=self.transport.now_ms()+30000
                else:self._retry.pop(gap,None)
                return dict(complete=False, symbol=need.symbol, interval=need.interval, **result)
        return blocked or dict(complete=True, requested=0, received=0, remaining=0)

    def close(self):
        self.closed = True
        for peer in self.peers:
            peer['stream'].close('OWNER_STOPPED')

"""Complete observed-universe public subscriptions and bounded fair polling.

No order/account reads or writes, no automatic reconnect, no invented coverage.
Opening a socket is only transport liveness; MarketStore still certifies each
required closed window. Start/recovery/backfill are explicit owner-scheduler
steps and cannot silently discard symbols, mappings or unreceived intervals.
"""
from dataclasses import dataclass
from collections import Counter
import hashlib,time
from .account import InvalidState,PositionKey
from .audit import canonical
from .market_stream import MarketStream,KlineEvent


@dataclass(frozen=True)
class SubscriptionPlan:
    subscriptions:tuple
    stock_symbols:tuple
    crypto_symbols:tuple
    unmapped_symbols:tuple
    catalog_digest:str
    mappings_digest:str
    known_ms:int

    def chunks(self,size=20):
        if type(size) is not int or not 1<=size<=200:raise InvalidState('行情连接分组大小无效')
        result=[]
        for venue in ('perpetual','spot'):
            pairs=tuple((s,i) for v,s,i in self.subscriptions if v==venue)
            result.extend((venue,pairs[start:start+size]) for start in range(0,len(pairs),size))
        return tuple(result)

    @property
    def digest(self):
        return hashlib.sha256(canonical(dict(subscriptions=self.subscriptions,stocks=self.stock_symbols,
            cryptos=self.crypto_symbols,unmapped=self.unmapped_symbols,catalog=self.catalog_digest,
            mappings=self.mappings_digest,known_ms=self.known_ms)).encode()).hexdigest()


def subscription_plan(sources,*,now_ms):
    from .live_source_inputs import LiveSourceInputs
    if not isinstance(sources,LiveSourceInputs):raise InvalidState('订阅计划必须使用完整的实际市场范围')
    sources._current(now_ms);mappings=sources.mappings.current(now_ms=now_ms)
    rows=set();stocks=sources.stock_symbols;cryptos=sources.crypto_names
    if not stocks or not {'BTCUSDT','ETHUSDT'}<=set(cryptos):raise InvalidState('组合订阅范围不完整')
    for s in stocks:
        for interval in ('1m','5m'):rows.add(('perpetual',s,interval))
    for s in cryptos:
        for interval in ('5m','1h'):rows.add(('perpetual',s,interval))
    for s in ('BTCUSDT','ETHUSDT'):
        rows.add(('perpetual',s,'1m'));rows.add(('spot',s,'5m'))
    mapped={s:mappings[s] for s in cryptos if s in mappings}
    for pair in mapped.values():rows.add(('spot',pair.spot_symbol,'5m'))
    binding={s:dict(spot_symbol=p.spot_symbol,asset=p.asset,known_ms=p.known_ms) for s,p in sorted(mapped.items())}
    return SubscriptionPlan(tuple(sorted(rows)),stocks,cryptos,tuple(s for s in cryptos if s not in mapped),
        sources.universe.source_digest,hashlib.sha256(canonical(binding).encode()).hexdigest(),now_ms)


class MarketFleet:
    @staticmethod
    def _validate_plan(plan):
        if not isinstance(plan,SubscriptionPlan):raise InvalidState('实时行情缺少完整订阅计划')
        if len(plan.subscriptions)!=len(set(plan.subscriptions)) or not plan.subscriptions:raise InvalidState('订阅重复或为空')
        for venue,symbol,interval in plan.subscriptions:
            PositionKey(symbol,'BOTH')
            if venue not in ('spot','perpetual') or interval not in ('1m','5m','1h'):raise InvalidState('组合实时周期或市场不受支持')

    def __init__(self,transport,store,plan,*,connector=None,chunk_size=20,monotonic=None):
        self._validate_plan(plan)
        self.transport,self.store,self.plan=transport,store,plan
        self._connector=connector;self._chunk_size=chunk_size;self._pending_additions=set()
        self.streams=tuple(MarketStream(transport,store,pairs,connector=connector,venue=venue) for venue,pairs in plan.chunks(chunk_size))
        self._cursor=0;self._attempted=set();self._mono=monotonic or time.monotonic
        self.closed=False;self.counts=Counter();self.last_poll_ms=None

    def expand(self,plan):
        """Append new peers without disconnecting/repacking any existing stream."""
        self._validate_plan(plan)
        if self.closed or plan.known_ms<self.plan.known_ms:raise InvalidState('订阅扩展已关闭或目录时间倒退')
        prior=self.plan;old=set(prior.subscriptions);added=tuple(s for s in plan.subscriptions if s not in old)
        merged=SubscriptionPlan(tuple(sorted(old|set(plan.subscriptions))),
            prior.stock_symbols+tuple(s for s in plan.stock_symbols if s not in prior.stock_symbols),
            tuple(sorted(set(prior.crypto_symbols)|set(plan.crypto_symbols))),plan.unmapped_symbols,
            plan.catalog_digest,plan.mappings_digest,plan.known_ms)
        delta=SubscriptionPlan(added,(),(),(),plan.catalog_digest,plan.mappings_digest,plan.known_ms)
        streams=tuple(MarketStream(self.transport,self.store,pairs,connector=self._connector,venue=venue)
            for venue,pairs in delta.chunks(self._chunk_size))
        first=len(self.streams)
        self.streams+=streams;self._pending_additions.update(range(first,len(self.streams)));self.plan=merged
        return dict(previous_plan_digest=prior.digest,plan_digest=merged.digest,added_subscriptions=len(added),
            added_connections=len(streams),coverage_certified=False)

    def start_one(self):
        if self.closed:raise InvalidState('行情组已关闭，不能隐式重开')
        for index,stream in enumerate(self.streams):
            if index in self._attempted:continue
            self._attempted.add(index)
            self._pending_additions.discard(index)
            stream.start()
            return dict(outcome='STREAM_OPENED_BACKFILL_REQUIRED',index=index,venue=stream.venue,
                subscriptions=len(stream.subscriptions),connections_open=sum(s._socket is not None for s in self.streams))
        return dict(outcome='ALL_CONNECTIONS_ATTEMPTED',connections_open=sum(s._socket is not None for s in self.streams))

    def reconnect(self,index):
        # Explicit recovery request; never clear MarketStore's revision halt.
        if self.closed or type(index) is not int or not 0<=index<len(self.streams):raise InvalidState('行情重连目标无效')
        if self.store.faulted():raise InvalidState('行情内容冲突尚未核对，拒绝重连')
        self._attempted.add(index)
        self._pending_additions.discard(index)
        return self.streams[index].start()

    def poll(self,*,max_events=1000,max_ms=100,fair_sweep=False):
        if self.closed:raise InvalidState('行情组已关闭')
        if (type(max_events) is not int or not 1<=max_events<=10000
                or type(max_ms) is not int or not 1<=max_ms<=1000
                or type(fair_sweep) is not bool
                or fair_sweep and max_events<len(self.streams)):raise InvalidState('行情轮询预算无效')
        start=self._mono();events=[];idle=set();polled=0;n=len(self.streams)
        # A round-robin cursor survives this budget. A hot socket cannot starve
        # a quiet peer's heartbeat or another sleeve's minute close. This is a
        # soft budget between messages, not a real-time deadline: the 20ms
        # socket timeout plus one decode/atomic database write can overrun it.
        # Slow account work can outlast several ordinary slices. Owner loops
        # explicitly request one real visit to every connection before yielding
        # to it. This bounded first sweep may exceed max_ms, but never exceeds
        # max_events, changes a receipt clock, or bypasses a stream's stall guard.
        try:
            with self.store.stream_batch():
                while (len(events)<max_events and len(idle)<n
                        and (fair_sweep and polled<n or (self._mono()-start)*1000<max_ms)):
                    index=self._cursor;self._cursor=(index+1)%n
                    if index in idle:continue
                    stream=self.streams[index];result=stream.poll();polled+=1
                    if isinstance(result,KlineEvent):
                        events.append(result)
                    elif result in ('IDLE','DISCONNECTED','WAIT_CLOCK_LAG','WAIT_PARTIAL_FRAME','CLOCK_CALIBRATION_EXPIRED'):
                        idle.add(index)
                    if self.store.faulted():raise InvalidState('MARKET_CONTENT_CONFLICT')
        except BaseException:
            # No returned events or in-memory chart may outlive a failed
            # durable commit. Keep the fleet available for explicit recovery.
            for stream in self.streams:stream.close('MARKET_BATCH_FAILED')
            raise
        for event in events:self.counts[event.venue,'closed' if event.closed else 'partial']+=1
        self.last_poll_ms=self.transport.now_ms()
        return dict(events=tuple(events),polled=polled,elapsed_ms=(self._mono()-start)*1000,
            connected=sum(s._socket is not None for s in self.streams),
            disconnected=tuple(i for i,s in enumerate(self.streams) if s._socket is None),
            requires_window_validation=True)

    def chart(self,venue,symbol,interval,*,now_ms):
        for stream in self.streams:
            if stream.venue==venue and (symbol,interval) in stream.subscriptions:
                return stream.chart(symbol,interval,now_ms=now_ms)
        return None

    def status(self):
        return dict(plan_digest=self.plan.digest,subscriptions=len(self.plan.subscriptions),
            stocks=len(self.plan.stock_symbols),cryptos=len(self.plan.crypto_symbols),
            unmatched_spot=len(self.plan.unmapped_symbols),last_poll_ms=self.last_poll_ms,
            connections=[dict(index=i,venue=s.venue,subscriptions=len(s.subscriptions),
                connected=s._socket is not None,reason=s.reason) for i,s in enumerate(self.streams)],
            counts={v+':'+k:n for (v,k),n in self.counts.items()},coverage_certified=False)

    def close(self):
        for stream in self.streams:stream.close()
        self.closed=True

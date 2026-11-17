"""Public USD-M/spot kline streams. Partial candles are chart-only observations.

No implicit reconnect, no trading, no claim that a connected socket fills gaps.
The owner scheduler explicitly backfills closed windows from REST after a gap.
Official market-stream schema checked 2026-11-10.
"""
from dataclasses import dataclass,replace
import json
import secrets
from urllib.parse import quote
from .account import InvalidState,PositionKey,nonnegative,positive
from .account_stream import _connect
from .binance_transport import CANDLE_PERIODS,TransportError,WireReply
from .public_market import parse_candles


@dataclass(frozen=True)
class KlineEvent:
    symbol:str
    interval:str
    event_ms:int
    first_arrival_ms:int
    available_ms:int
    closed:bool
    chart:dict
    observation:object=None
    venue:str='perpetual'


def parse_kline(payload,*,first_arrival_ms,subscriptions,venue='perpetual'):
    if venue not in ('perpetual','spot') or type(first_arrival_ms) is not int or first_arrival_ms<0 or type(payload) is not dict:
        raise InvalidState('实时 K 线接收时间或格式错误')
    try:
        stream=payload['stream'];d=payload['data'];k=d['k'];symbol=d['s'];interval=k['i']
        if (type(d) is not dict or type(k) is not dict or d['e']!='kline' or (symbol,interval) not in subscriptions
                or k['s']!=symbol or stream!=symbol.lower()+'@kline_'+interval or d.get('st',1)!=1):
            raise InvalidState('实时行情不是已订阅的品种与 UTC 周期')
        p=CANDLE_PERIODS[interval];start,end,event=k['t'],k['T']+1,d['E']
        if any(type(v) is not int for v in (start,k['T'],event)) or type(k['x']) is not bool:
            raise InvalidState('实时行情时间或收盘标记错误')
        if not 0<=start<end or start%p or end-start!=p or not start<=event<=first_arrival_ms+500:
            raise InvalidState('实时行情周期、时间或时钟偏差无效')
        if k['x'] and event<end-1:raise InvalidState('实时行情提前宣称收盘')
        o,h,l,c=map(positive,(k['o'],k['h'],k['l'],k['c']))
        v,q,bv,bq=map(nonnegative,(k['v'],k['q'],k['V'],k['Q']))
        if (not l<=min(o,c)<=max(o,c)<=h or (v==0)!=(q==0) or (bv==0)!=(bq==0)
                or bv>v or bq>q or type(k['n']) is not int or k['n']<0 or (k['n']==0)!=(v==0)):
            raise InvalidState('实时行情价格、成交量或笔数矛盾')
        available=max(first_arrival_ms,event,end if k['x'] else 0)
        chart=dict(start_ms=start,end_ms=end,open=str(o),high=str(h),low=str(l),close=str(c),
            volume=str(v),quote_volume=str(q),trades=k['n'],buy_volume=str(bv),buy_quote=str(bq),closed=k['x'])
        observation=None
        if k['x']:
            row=[start,str(o),str(h),str(l),str(c),str(v),end-1,str(q),k['n'],str(bv),str(bq),'0']
            page=parse_candles(WireReply([row],available,available),symbol=symbol,interval=interval,
                start_ms=start,end_ms=end,limit=1)
            observation=page.rows[0]
        return KlineEvent(symbol,interval,event,first_arrival_ms,available,k['x'],chart,observation,venue)
    except (KeyError,TypeError,ValueError):raise InvalidState('实时 K 线字段不完整或无效') from None


class MarketStream:
    BASE='wss://fstream.binance.com/market/stream?streams='
    SPOT_BASE='wss://stream.binance.com:443/stream?streams='

    def __init__(self,transport,store,subscriptions,*,connector=None,venue='perpetual',background=None):
        if venue not in ('perpetual','spot'):raise InvalidState('实时行情市场来源无效')
        pairs=tuple(subscriptions)
        if not 1<=len(pairs)<=200 or len(set(pairs))!=len(pairs):raise InvalidState('单连接订阅数量或重复品种无效')
        for symbol,interval in pairs:
            PositionKey(symbol,'BOTH')
            if interval not in CANDLE_PERIODS:raise InvalidState('实时行情周期不受支持')
        streams=[s.lower()+'@kline_'+i for s,i in pairs]
        if len(set(streams))!=len(streams):raise InvalidState('品种代码转换后产生重复订阅')
        self.transport,self.store,self.subscriptions=transport,store,frozenset(pairs)
        self.venue=venue
        self._url=(self.BASE if venue=='perpetual' else self.SPOT_BASE)+'/'.join(quote(s,safe='@_') for s in streams)
        self._connector=connector or _connect;self._socket=None;self._pending=None
        if background is not None and type(background) is not bool:raise InvalidState('公开行情接收模式无效')
        self._background=connector is None if background is None else background
        self._receiver=None
        self.last_receiver_status=None;self.maximum_poll_gap_ms=0
        self.latest={};self._last={};self.reason='NOT_CONNECTED';self._ping=None

    def start(self):
        if self._socket is not None:raise InvalidState('已有行情连接')
        try:
            self._socket=self._connector(self._url);now=self.transport.now_ms()
            self._opened=self._last_poll=self._last_pong=now;self._pending=self._ping=None
            self.latest.clear();self._last.clear();self.reason='BACKFILL_REQUIRED'
            if self._background:
                from .market_receiver import MarketReceiver
                self._receiver=MarketReceiver(self._socket,
                    lambda payload,at:parse_kline(payload,first_arrival_ms=at,subscriptions=self.subscriptions,venue=self.venue),
                    self.transport.now_ms)
                self._receiver.start()
            return self.reason
        except Exception:
            self.close('CONNECT_FAILED');raise TransportError('MARKET_STREAM_CONNECT_FAILED') from None

    def close(self,reason='OWNER_STOP'):
        sock,self._socket=self._socket,None;self._pending=self._ping=None
        receiver,self._receiver=self._receiver,None
        self.latest.clear();self.reason=reason
        if receiver is not None:
            self.last_receiver_status=receiver.status()
            receiver.close();return
        if sock is not None:
            try:
                if hasattr(sock,'shutdown'):sock.shutdown()
                else:sock.close()
            except Exception:pass

    def _deliver(self,event):
        now=self.transport.now_ms()
        if type(now) is not int or now<event.available_ms:raise InvalidState('实时行情尚未达到处理时间')
        # A deferred message may be processed after another REST observation.
        # Preserve wire receipt separately; publish its strategy availability
        # only now, never earlier than the actual release of the pending item.
        observation=event.observation
        if observation is not None:observation=replace(observation,bar=replace(observation.bar,received_ms=now))
        event=replace(event,available_ms=now,observation=observation)
        key=(event.symbol,event.interval);prior=self._last.get(key)
        if prior is not None:
            if event.event_ms<prior.event_ms or event.chart['start_ms']<prior.chart['start_ms']:
                raise InvalidState('实时行情顺序倒退')
            if prior.closed and prior.chart['start_ms']==event.chart['start_ms'] and not event.closed:
                raise InvalidState('已收盘行情不能回退为未收盘')
        if event.venue!=self.venue:raise InvalidState('现货与合约行情来源混用')
        if event.closed:self.store.put_closed(self.venue,event)
        self.latest[key]=event;self._last[key]=event
        self.reason='RECEIVING'  # Per-window completeness is checked by MarketStore.
        return event

    def poll(self):
        import websocket
        if self._socket is None:return 'DISCONNECTED'
        try:
            now=self.transport.now_ms()
            if now-self._opened>=23*3600000:
                self.close('ROTATE_CONNECTION');return 'DISCONNECTED'
            self.maximum_poll_gap_ms=max(self.maximum_poll_gap_ms,now-self._last_poll)
            # In direct mode the consumer also owns socket heartbeats. A
            # background reader owns its own liveness and bounded queue; a
            # slow calculation must not discard its correctly received closes.
            if self._receiver is None and now-self._last_poll>15000:
                self.close('CONSUMER_STALLED');return 'DISCONNECTED'
            if self._ping is not None and now-self._ping_ms>10000:
                self.close('HEARTBEAT_TIMEOUT');return 'DISCONNECTED'
            self._last_poll=now
            if self._receiver is not None:
                receiver_status=self._receiver.status()
                failure=receiver_status['failure']
                if failure is not None:
                    self.close(failure);return 'DISCONNECTED'
                if not receiver_status['running']:
                    self.close('PUBLIC_SOCKET_FAILED');return 'DISCONNECTED'
                if receiver_status['valid_frame_idle_ms']>15000:
                    self.close('HEARTBEAT_TIMEOUT');return 'DISCONNECTED'
            if self._pending is not None:
                if self._pending.available_ms>now:return 'WAIT_CLOCK_LAG'
                event,self._pending=self._pending,None
                return self._deliver(event)
            if self._receiver is not None:
                event=self._receiver.take()
                if event is None:return 'IDLE'
                if event.available_ms>now:
                    self._pending=event;return 'WAIT_CLOCK_LAG'
                return self._deliver(event)
            if self._ping is None and now-self._last_pong>=5000:
                self._ping=secrets.token_bytes(12);self._ping_ms=now;self._socket.ping(self._ping)
            # Avoid spending the fleet budget on an idle TLS connection.
            # SSL may already hold decrypted bytes even when its OS descriptor
            # is not readable; websocket may also retain an unfinished frame.
            wire=getattr(self._socket,'sock',None)
            if wire is not None and hasattr(wire,'fileno'):
                import select
                buffered=getattr(self._socket,'frame_buffer',None)
                tls_pending=getattr(wire,'pending',lambda:0)()
                raw_buffer=bool(buffered is not None and buffered.recv_buffer)
                if not tls_pending and not raw_buffer and not select.select([wire],[],[],0)[0]:
                    fragments=getattr(self._socket,'cont_frame',None)
                    partial=(buffered is not None and buffered.header is not None
                        or fragments is not None and (fragments.cont_data is not None or fragments.recving_frames is not None))
                    return 'WAIT_PARTIAL_FRAME' if partial else 'IDLE'
            opcode,frame=self._socket.recv_data_frame(control_frame=True)
            received=self.transport.now_ms()
            if opcode==websocket.ABNF.OPCODE_TEXT:
                if len(frame.data)>32000:raise InvalidState('实时行情消息过大')
                payload=json.loads(frame.data)
                if (type(payload) is dict and payload.get('stream')=='!serverShutdown'
                        and type(payload.get('data')) is dict and payload['data'].get('e')=='serverShutdown'):
                    stamp=payload['data'].get('E')
                    if type(stamp) is not int or not 0<=stamp<=received+500:raise InvalidState('行情服务关闭通知时间无效')
                    self.close('SERVER_SHUTDOWN');return 'DISCONNECTED'
                event=parse_kline(payload,first_arrival_ms=received,subscriptions=self.subscriptions,venue=self.venue)
                if event.available_ms>received:
                    self._pending=event;return 'WAIT_CLOCK_LAG'
                return self._deliver(event)
            if opcode==websocket.ABNF.OPCODE_PONG:
                if self._ping is not None and frame.data==self._ping:
                    self._ping=None;self._last_pong=received;return 'HEARTBEAT'
                return 'UNMATCHED_PONG'
            if opcode==websocket.ABNF.OPCODE_PING:return 'PEER_PING'
            self.close('SOCKET_FAILED');return 'DISCONNECTED'
        except websocket.WebSocketTimeoutException:
            buffer=getattr(self._socket,'frame_buffer',None);fragments=getattr(self._socket,'cont_frame',None)
            if buffer is not None and (buffer.header is not None or buffer.recv_buffer) or fragments is not None and (fragments.cont_data is not None or fragments.recving_frames is not None):
                return 'WAIT_PARTIAL_FRAME'
            return 'IDLE'
        except TransportError as exc:
            if exc.reason=='CLOCK_CALIBRATION_EXPIRED':return exc.reason
            self.close('TRANSPORT_FAILED');return 'DISCONNECTED'
        except Exception:
            self.close('INVALID_OR_UNSAVED_MARKET');return 'DISCONNECTED'

    def chart(self,symbol,interval,*,now_ms,max_age_ms=15000):
        if any(type(v) is not int for v in (now_ms,max_age_ms)) or max_age_ms<=0:raise InvalidState('图表时间错误')
        event=self.latest.get((symbol,interval))
        if (self._socket is None or event is None or not 0<=now_ms-event.available_ms<=max_age_ms
                or now_ms-event.event_ms>max_age_ms):return None
        return dict(venue=self.venue,symbol=symbol,interval=interval,first_arrival_ms=event.first_arrival_ms,
            available_ms=event.available_ms,event_ms=event.event_ms,**event.chart)

"""Bounded public-wire reader; no account, strategy, database or order access.

The reader keeps heartbeat I/O independent from candle persistence. Only
superseded unclosed chart updates may be coalesced. Every closed observation
stays ordered with its actual wire receipt; overflow fails closed.
"""
from collections import OrderedDict
import json,secrets,threading,time
from .account import InvalidState


class ReceiverFault(InvalidState):
    pass


class MarketReceiver:
    def __init__(self,socket,decode,clock,*,capacity=4096,monotonic=None):
        if type(capacity) is not int or not 1<=capacity<=10000:
            raise InvalidState('公开行情接收队列容量无效')
        self.socket,self.decode,self.clock=socket,decode,clock
        self.capacity=capacity;self.mono=monotonic or time.monotonic
        self.lock=threading.Lock();self.stop=threading.Event();self.thread=None
        self.queue=OrderedDict();self.partials={};self.previous={};self.serial=0
        self.failure=None;self.messages=0;self.coalesced=0;self.last_received_ms=None
        self.opened=self.last_pong=self.mono();self.ping=None;self.ping_ms=None
        self.last_frame=self.last_valid_frame=self.opened
        self.last_event_ms=None;self.wire_delay_ms=None;self.maximum_wire_delay_ms=0
        self.delayed_streak=0;self.delayed_events=0
        self.controls=dict(matched_pong=0,unmatched_pong=0,peer_ping=0,unanswered_ping_with_fresh_data=0)

    def start(self):
        if self.thread is not None or self.stop.is_set():raise InvalidState('公开行情接收器不能重复启动')
        self.thread=threading.Thread(target=self._run,name='C801-public-wire',daemon=True)
        self.thread.start()

    def _accept(self,event):
        key=(event.symbol,event.interval);start=event.chart['start_ms']
        with self.lock:
            previous=self.previous.get(key)
            if previous is not None:
                old_start,old_event,closed,old_chart=previous
                if start<old_start or event.event_ms<old_event or (start==old_start and closed and not event.closed):
                    raise ReceiverFault('PUBLIC_EVENT_REVERSED')
                if not event.closed and key in self.partials and event.event_ms==old_event and event.chart==old_chart:
                    self.messages+=1;self.coalesced+=1;self.last_received_ms=event.first_arrival_ms
                    return  # Preserve the queued identical message's first receipt.
            # Validate before replacing anything; bad partial updates cannot be
            # hidden by the subsequent valid chart observation.
            old_seq=self.partials.pop(key,None)
            if old_seq is not None:
                self.queue.pop(old_seq);self.coalesced+=1
            if len(self.queue)>=self.capacity:raise ReceiverFault('PUBLIC_QUEUE_OVERFLOW')
            self.serial+=1;self.queue[self.serial]=event
            if not event.closed:self.partials[key]=self.serial
            self.previous[key]=(start,event.event_ms,event.closed,dict(event.chart))
            self.messages+=1;self.last_received_ms=event.first_arrival_ms

    def _step(self):
        import websocket
        now=self.mono()
        if now-self.opened>=23*3600:raise ReceiverFault('ROTATE_CONNECTION')
        if self.ping is not None and now-self.ping_ms>10:
            if now-self.last_valid_frame>5:raise ReceiverFault('HEARTBEAT_TIMEOUT')
            # Successfully decoded current messages attest inbound liveness.
            # Record the unanswered probe, then probe again only after silence.
            # This says nothing about candle coverage or quote freshness; those
            # remain separately enforced before strategy/broker decisions.
            self.controls['unanswered_ping_with_fresh_data']+=1;self.ping=None
        if self.ping is None and now-self.last_valid_frame>=5:
            self.ping=secrets.token_bytes(12);self.ping_ms=now;self.socket.ping(self.ping)
        opcode,frame=self.socket.recv_data_frame(control_frame=True)
        self.last_frame=self.mono()
        received=self.clock()
        if opcode==websocket.ABNF.OPCODE_TEXT:
            if len(frame.data)>32000:raise ReceiverFault('PUBLIC_MESSAGE_TOO_LARGE')
            payload=json.loads(frame.data)
            if (type(payload) is dict and payload.get('stream')=='!serverShutdown'
                    and type(payload.get('data')) is dict and payload['data'].get('e')=='serverShutdown'):
                stamp=payload['data'].get('E')
                if type(stamp) is not int or not 0<=stamp<=received+500:raise ReceiverFault('INVALID_PUBLIC_SHUTDOWN')
                raise ReceiverFault('SERVER_SHUTDOWN')
            event=self.decode(payload,received)
            with self.lock:
                self.last_event_ms=event.event_ms;self.wire_delay_ms=max(0,received-event.event_ms)
                self.maximum_wire_delay_ms=max(self.maximum_wire_delay_ms,self.wire_delay_ms)
                late=self.wire_delay_ms>15000
                self.delayed_streak=self.delayed_streak+1 if late else 0
                self.delayed_events+=int(late)
                # Liveness and freshness are separate. Stop a persistently
                # backlogged socket; missing closes must be REST-reconciled by
                # the scheduler, never silently presented as complete/current.
                if self.delayed_streak>=3:raise ReceiverFault('PUBLIC_MARKET_DELAY')
            self._accept(event);self.last_valid_frame=self.mono();return 'RECEIVED'
        if opcode==websocket.ABNF.OPCODE_PONG:
            if self.ping is not None and frame.data==self.ping:
                self.ping=None;self.last_pong=self.last_valid_frame=self.mono();self.controls['matched_pong']+=1;return 'HEARTBEAT'
            self.controls['unmatched_pong']+=1
            return 'UNMATCHED_PONG'
        if opcode==websocket.ABNF.OPCODE_PING:
            self.controls['peer_ping']+=1;self.last_valid_frame=self.mono();return 'PEER_PING'
        raise ReceiverFault('SOCKET_FAILED')

    def _run(self):
        import websocket
        try:
            while not self.stop.is_set():
                try:self._step()
                except websocket.WebSocketTimeoutException:continue
        except ReceiverFault as exc:
            with self.lock:self.failure=str(exc)
        except (InvalidState,ValueError,KeyError,TypeError):
            with self.lock:self.failure='INVALID_PUBLIC_MARKET'
        except Exception:
            # Raw socket/vendor error strings are never retained.
            with self.lock:self.failure='PUBLIC_SOCKET_FAILED'
        finally:
            self.stop.set();self._shutdown()

    def take(self):
        with self.lock:
            if self.failure is not None or not self.queue:return None
            seq,event=self.queue.popitem(last=False);key=(event.symbol,event.interval)
            if self.partials.get(key)==seq:del self.partials[key]
            return event

    def status(self):
        now=self.mono()
        with self.lock:
            return dict(failure=self.failure,queued=len(self.queue),wire_messages=self.messages,
                coalesced_partials=self.coalesced,last_received_ms=self.last_received_ms,
                running=self.thread is not None and self.thread.is_alive(),controls=dict(self.controls),
                frame_idle_ms=int((now-self.last_frame)*1000),pong_idle_ms=int((now-self.last_pong)*1000),
                valid_frame_idle_ms=int((now-self.last_valid_frame)*1000),
                last_event_ms=self.last_event_ms,wire_delay_ms=self.wire_delay_ms,
                maximum_wire_delay_ms=self.maximum_wire_delay_ms,
                delayed_events=self.delayed_events,delayed_streak=self.delayed_streak,
                ping_age_ms=None if self.ping is None else int((now-self.ping_ms)*1000))

    def _shutdown(self):
        # A graceful websocket close can wait three seconds. That delay must
        # not cascade across ten connections after a single peer fails.
        try:self.socket.shutdown()
        except Exception:pass

    def close(self):
        self.stop.set();self._shutdown()
        if self.thread is not None:
            self.thread.join(timeout=.5)
            if self.thread.is_alive():raise InvalidState('公开行情接收器未能及时停止')

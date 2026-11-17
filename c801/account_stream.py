"""Ordered account notifications invalidate observations; they do not own trades.

A websocket has no replay cursor proving that all historical messages arrived.
Every reconnect therefore needs REST order/fill/income reconciliation. A quiet
socket or pong is liveness evidence only, never an account-completeness claim.
The sequential owner service polls this object; it starts no background thread.
"""
from contextlib import nullcontext
from collections import deque
from dataclasses import dataclass
import hashlib
import json
import re
import secrets
import sys
import urllib.parse
import urllib.request
from .account import InvalidState
from .audit import canonical
from .binance_transport import TransportError


EVENT_TYPES = frozenset({'ACCOUNT_UPDATE', 'ORDER_TRADE_UPDATE', 'ALGO_UPDATE',
    'ACCOUNT_CONFIG_UPDATE', 'TRADE_LITE', 'MARGIN_CALL',
    'CONDITIONAL_ORDER_TRIGGER_REJECT', 'listenKeyExpired'})
CLOSE_REASONS = frozenset({'OWNER_STOP', 'SOCKET_FAILED', 'SUBSCRIPTION_EXPIRED',
    'SUBSCRIPTION_FAILED', 'ROTATE_CONNECTION', 'INVALID_EVENT', 'EVENT_REVERSED',
    'MARGIN_CALL', 'PROTECTION_REJECTED', 'HEARTBEAT_TIMEOUT'})
_STREAM_REPLAY_CACHE_BYTES = 16 * 1024 * 1024
_WIRE_QUEUE_BYTES = 4 * 1024 * 1024
_WIRE_QUEUE_FRAMES = 256


class AccountStreamEvidenceExpired(InvalidState):
    """Only elapsed liveness evidence; never malformed/future stream state."""


@dataclass(frozen=True)
class StreamCursor:
    connection: str
    revision: int
    last_event_ms: int


def _initial():
    return dict(connection='', revision=0, connected=False, opened_ms=0,
        last_received_ms=0, last_event_ms=0, event_times={}, pending=None, reason='NOT_CONNECTED')


def _reduce(state, action, now_ms):
    """Pure replay of the minimal, secret-free transport journal."""
    s = json.loads(canonical(state))
    if type(now_ms) is not int or now_ms < s['last_received_ms']:
        raise InvalidState('账户通知接收时间倒退')
    kind = action['action']
    if kind == 'OPEN':
        if not re.fullmatch('[a-f0-9]{32}', action['connection']) or action['connection'] == s['connection']:
            raise InvalidState('账户连接代次无效')
        s.update(connection=action['connection'], connected=True, opened_ms=now_ms,
            event_times={}, last_event_ms=0, pending=None, reason='RECONCILIATION_REQUIRED')
        s['revision'] += 1
    else:
        if not s['connected'] or action['connection'] != s['connection']:
            raise InvalidState('通知来自已失效的账户连接')
        if kind == 'CLOSE':
            if action['reason'] not in CLOSE_REASONS:
                raise InvalidState('账户连接关闭原因无效')
            s.update(connected=False, pending=None, reason=action['reason'])
            s['revision'] += 1
        elif kind in ('EVENT', 'DEFER'):
            event, e, t = action['event'], action['event_ms'], action['transaction_ms']
            limit = now_ms + (500 if kind == 'DEFER' else 0)
            if event not in EVENT_TYPES or any(type(x) is not int or not 0 <= x <= limit for x in (e, t)):
                raise InvalidState('账户通知类型或时间无效')
            # Processing can follow a checkpoint transaction. Preserve the
            # original arrival separately; never backdate the audit or accept
            # a packet that was too far in the future when first received.
            pending = s['pending']
            arrival = action.get('first_arrival_ms', pending['first_arrival_ms'] if pending else now_ms)
            if (type(arrival) is not int or not s['opened_ms'] <= arrival <= now_ms
                    or max(e, t) > arrival + 500
                    or pending is not None and arrival != pending['first_arrival_ms']):
                raise InvalidState('账户通知首次到达时间无效')
            prior = s['event_times'].get(event)
            if prior and (e < prior[0] or t < prior[1]):
                raise InvalidState('同类账户事件顺序倒退')
            if not re.fullmatch('[a-f0-9]{64}', action['content_digest']):
                raise InvalidState('账户事件指纹无效')
            if kind == 'DEFER':
                if s['pending'] is not None or max(e, t) <= now_ms:
                    raise InvalidState('账户通知待处理状态无效')
                s['pending'] = dict(event=event, event_ms=e, transaction_ms=t,
                    content_digest=action['content_digest'], first_arrival_ms=arrival)
            else:
                if s['pending'] is not None and any(action[k] != s['pending'][k] for k in ('event', 'event_ms', 'transaction_ms', 'content_digest')):
                    raise InvalidState('待处理通知与实际处理内容不一致')
                s['pending'] = None
                s['event_times'][event] = [e, t]
                s['last_event_ms'] = max(s['last_event_ms'], e, t)
            s['revision'] += 1
            s['reason'] = 'EVENT_AWAITING_CLOCK' if kind == 'DEFER' else 'RECONCILIATION_REQUIRED'
        elif kind != 'HEARTBEAT':
            raise InvalidState('账户通知审计动作无效')
    s['last_received_ms'] = now_ms
    return s


class AccountStreamJournal:
    def __init__(self, execution):
        self.execution, self.db = execution, execution.db
        self.db.execute('CREATE TABLE IF NOT EXISTS account_stream_state(id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL)')
        # A new Python owner never inherits another process's live connection.
        self._connection = None
        self._replay_prefix = None

    def state(self):
        row = self.db.execute('SELECT body FROM account_stream_state WHERE id=1').fetchone()
        return json.loads(row[0]) if row else _initial()

    def _record(self, action, now_ms):
        with (nullcontext() if self.db.in_transaction else self.execution.transaction()):
            state = _reduce(self.state(), action, now_ms)
            self.db.execute('INSERT OR REPLACE INTO account_stream_state VALUES(1,?)', (canonical(state),))
            self.execution.append_in_transaction(now_ms, 'SELF_CHECK', dict(stage='ACCOUNT_STREAM', **action))
        return state

    def opened(self, *, now_ms):
        connection = secrets.token_hex(16)
        self._record(dict(action='OPEN', connection=connection), now_ms)
        self._connection = connection
        return connection

    def closed(self, reason, *, now_ms):
        if self._connection is not None and self.state()['connected']:
            self._record(dict(action='CLOSE', connection=self._connection, reason=reason), now_ms)
        self._connection = None

    def heartbeat(self, connection, *, received_ms):
        self._check_connection(connection)
        self._record(dict(action='HEARTBEAT', connection=connection), received_ms)

    def _check_connection(self, connection):
        s = self.state()
        if connection != self._connection or connection is None or not s['connected'] or s['connection'] != connection:
            raise InvalidState('账户连接已变化或尚未建立')

    def observe(self, connection, payload, *, received_ms, first_arrival_ms=None):
        self._check_connection(connection)
        try:
            if type(payload) is not dict or payload.get('e') not in EVENT_TYPES:
                raise InvalidState('账户通知格式或事件类型不受支持')
            event, e = payload['e'], payload['E']
            t = payload.get('T', e)
            # Never persist listenKey, payload, URL, or vendor rejection text.
            safe = {k: v for k, v in payload.items() if k != 'listenKey'}
            digest = hashlib.sha256(canonical(safe).encode()).hexdigest()
            prior = self.state()['event_times'].get(event)
            if prior and type(e) is int and type(t) is int and (e < prior[0] or t < prior[1]):
                self.closed('EVENT_REVERSED', now_ms=received_ms)
                raise InvalidState('同类账户事件顺序倒退，需要重新对账')
            action = dict(action='EVENT', connection=connection, event=event,
                event_ms=e, transaction_ms=t, content_digest=digest)
            if first_arrival_ms is not None: action['first_arrival_ms'] = first_arrival_ms
            self._record(action, received_ms)
        except (KeyError, TypeError, ValueError):
            last = self.db.execute('SELECT time_ms FROM audit ORDER BY seq DESC LIMIT 1').fetchone()
            safe_now = max(last[0] if last else 0, received_ms if type(received_ms) is int else 0)
            self.closed('INVALID_EVENT', now_ms=safe_now)
            raise InvalidState('账户事件无效，旧账户状态已作废') from None
        reason = {'listenKeyExpired': 'SUBSCRIPTION_EXPIRED', 'MARGIN_CALL': 'MARGIN_CALL',
            'CONDITIONAL_ORDER_TRIGGER_REJECT': 'PROTECTION_REJECTED'}.get(event)
        if reason:
            self.closed(reason, now_ms=received_ms)
        return event

    def defer(self, connection, payload, *, received_ms, first_arrival_ms=None):
        self._check_connection(connection)
        safe = {k: v for k, v in payload.items() if k != 'listenKey'}
        action = dict(action='DEFER', connection=connection, event=payload['e'], event_ms=payload['E'],
            transaction_ms=payload.get('T', payload['E']), content_digest=hashlib.sha256(canonical(safe).encode()).hexdigest())
        if first_arrival_ms is not None: action['first_arrival_ms'] = first_arrival_ms
        self._record(action, received_ms)

    def cursor(self, *, now_ms, max_silence_ms=15000):
        self._check_connection(self._connection)
        s = self.state()
        if s['pending'] is not None:
            raise InvalidState('已收到账户通知但尚未达到可处理时间，旧状态已作废')
        if type(now_ms) is not int or type(max_silence_ms) is not int or max_silence_ms <= 0 or now_ms < s['last_received_ms']:
            raise InvalidState('账户连接核对时钟或心跳期限无效')
        if now_ms - s['last_received_ms'] > max_silence_ms:
            raise AccountStreamEvidenceExpired('账户连接缺少及时的到达或心跳证据')
        return StreamCursor(s['connection'], s['revision'], s['last_event_ms'])

    def unchanged(self, cursor, *, now_ms):
        return isinstance(cursor, StreamCursor) and cursor == self.cursor(now_ms=now_ms)

    def verify(self):
        # Reuse only a pure reduction of an EXACT unchanged input prefix.
        # Every call still verifies the current full audit chain, rereads all
        # stream rows and compares the reconstructed current materialized state
        # inside one database snapshot. No heartbeat/connection permit is cached.
        with (nullcontext() if self.db.in_transaction else self.execution.transaction()), self.execution.verification_scope():
            try:
                self.execution.verify()
                rows = tuple(self.db.execute("""SELECT seq,time_ms,body FROM audit
                    WHERE kind='SELF_CHECK' AND json_extract(body,'$.stage')='ACCOUNT_STREAM'
                    ORDER BY seq"""))
                policy = (_initial, _reduce, canonical, json.loads, re.fullmatch,
                          EVENT_TYPES, CLOSE_REASONS, _STREAM_REPLAY_CACHE_BYTES)
                prior = self._replay_prefix
                if (prior is not None and prior[0] == policy and len(prior[1]) <= len(rows)
                        and rows[:len(prior[1])] == prior[1]):
                    start, s = len(prior[1]), json.loads(prior[2])
                else:
                    start, s = 0, _initial()
                for _, stamp, body in rows[start:]:
                    s = _reduce(s, json.loads(body), stamp)
                if s != self.state():
                    raise InvalidState('账户连接状态与独立审计重建不符')
                raw = canonical(s)
                size = (sys.getsizeof((policy, rows, raw)) + sys.getsizeof(rows) + sys.getsizeof(raw) + sys.getsizeof(policy)
                        + sum(sys.getsizeof(row) + sum(sys.getsizeof(v) for v in row) for row in rows))
                self._replay_prefix = (policy, rows, raw) if size <= _STREAM_REPLAY_CACHE_BYTES else None
                return True
            except BaseException:
                self._replay_prefix = None
                raise


def _proxy_options():
    """Honor the same existing OS/env proxy as urllib's REST adapter.

No alternate egress or country selection is configured here. websocket-client
otherwise ignores Windows Internet Settings and silently takes another path.
"""
    if urllib.request.proxy_bypass('fstream.binance.com'):
        return dict(http_no_proxy=['fstream.binance.com'])
    value = urllib.request.getproxies().get('https')
    if not value:
        return {}
    try:
        p = urllib.parse.urlsplit(value if '://' in value else 'http://' + value)
        if p.scheme != 'http' or not p.hostname or p.path not in ('', '/') or p.query or p.fragment:
            raise ValueError()
        options = dict(http_proxy_host=p.hostname, http_proxy_port=p.port or 80, proxy_type='http', http_proxy_timeout=5)
        if p.username is not None:
            options['http_proxy_auth'] = (urllib.parse.unquote(p.username), urllib.parse.unquote(p.password or ''))
        return options
    except ValueError:
        raise TransportError('EXISTING_PROXY_FORMAT_UNSUPPORTED') from None


def _connect(url):
    import websocket
    if websocket.__version__!='1.8.0':
        raise TransportError('ACCOUNT_STREAM_DECODER_VERSION_NOT_VERIFIED')
    # websocket-client trace prints handshake URLs; disable before credentials
    # can enter the handshake. TLS certificate checks remain enabled.
    websocket.enableTrace(False)
    try:
        sock = websocket.create_connection(url, timeout=5, redirect_limit=0,
            suppress_origin=True, enable_multithread=False, **_proxy_options())
    except websocket.WebSocketBadStatusException as exc:
        raise TransportError('ACCOUNT_STREAM_HANDSHAKE_FAILED', http_status=exc.status_code) from None
    except websocket.WebSocketTimeoutException:
        raise TransportError('ACCOUNT_STREAM_CONNECT_TIMEOUT') from None
    if sock.handshake_response.status != 101:
        sock.close()
        raise TransportError('STREAM_REDIRECT_OR_HANDSHAKE_REFUSED')
    sock.settimeout(0.02)
    return sock


class AccountStream:
    """Explicit subscription lifecycle; no trading, credential output or retry."""
    BASE = 'wss://fstream.binance.com/private/ws/'

    def __init__(self, transport, journal, *, connector=None):
        self.transport, self.journal = transport, journal
        self._connector = connector or _connect
        self._socket = self._listen_key = self._connection = None
        self._created_ms = self._renewed_ms = self._last_poll_ms = 0
        self._ping = None
        self._pending_event = None
        self._drained = False
        self._ping_ms = self._last_pong_ms = 0
        self._wire_queue = deque()
        self._wire_bytes = 0
        self._wire_failure = None
        self._refresh_required = False
        self._ping_for_journal = False

    def start(self):
        if self._socket is not None:
            raise InvalidState('已有账户连接，不能覆盖正在使用的连接')
        reply = self.transport.request('START_ACCOUNT_STREAM', {})
        try:
            key = reply.body['listenKey']
            if type(key) is not str or not re.fullmatch('[A-Za-z0-9_-]{16,512}', key):
                raise InvalidState('账户订阅返回无效')
            self._socket = self._connector(self.BASE + key)
            self._drained = False
            now = self.transport.now_ms()
            self._listen_key = key
            self._connection = self.journal.opened(now_ms=now)
            self._created_ms = self._renewed_ms = self._last_poll_ms = now
            self._last_pong_ms = now
        except TransportError:
            self.close('SOCKET_FAILED')
            raise
        except Exception:
            self.close('SOCKET_FAILED')
            raise TransportError('ACCOUNT_STREAM_CONNECT_FAILED') from None
        return 'RECONCILIATION_REQUIRED'

    def close(self, reason='OWNER_STOP'):
        sock, self._socket = self._socket, None
        self._drained = False
        self._listen_key = self._connection = self._ping = self._pending_event = None
        self._wire_queue.clear(); self._wire_bytes = 0
        self._wire_failure = None; self._refresh_required = False; self._ping_for_journal = False
        self.journal.closed(reason, now_ms=self.transport.now_ms())
        if sock is not None:
            try:
                sock.close(timeout=0)
            except Exception:
                pass

    def _fail(self, reason, *, network_only):
        if network_only:
            # The caller must unwind its financial snapshot. The ordinary
            # owner then closes/journals this connection outside that snapshot.
            self._wire_failure = reason
            self._drained = False
        else:
            self.close(reason)
        return 'DISCONNECTED'

    def _observe(self, raw, arrival, processed):
        payload = json.loads(raw)
        if type(payload) is dict and payload.get('e') in EVENT_TYPES:
            stamps = (payload.get('E'), payload.get('T', payload.get('E')))
            if all(type(x) is int for x in stamps) and processed < max(stamps) <= arrival + 500:
                self.journal.defer(self._connection, payload, received_ms=processed, first_arrival_ms=arrival)
                self._pending_event = (payload, arrival)
                return 'WAIT_CLOCK_LAG'
        event = self.journal.observe(self._connection, payload, received_ms=processed, first_arrival_ms=arrival)
        if not self.journal.state()['connected']:
            self.close(self.journal.state()['reason'])
        return event

    def poll(self):
        return self._poll(network_only=False)

    def maintain_network(self, *, maximum_frames=100):
        """Service only the wire while an account snapshot must stay fixed.

No journal/REST calls or account authority. Text frames survive transaction
rollback in a bounded process-local queue. Actual first arrival is retained.
A subsequent ordinary drain and fresh matching pong are required for a cursor.
"""
        if type(maximum_frames) is not int or not 1 <= maximum_frames <= 100:
            raise InvalidState('账户连接维护读取上限无效')
        self._drained = False
        self._refresh_required = True
        # A pong for a ping sent before the frozen snapshot cannot certify
        # the subsequent ordinary drain; require a newly sent normal ping.
        self._ping_for_journal = False
        for _ in range(maximum_frames):
            result = self._poll(network_only=True)
            if result in ('IDLE', 'DISCONNECTED', 'WAIT_PARTIAL_FRAME'):
                return result
        return 'STREAM_BACKLOG'

    def _poll(self, *, network_only):
        import websocket
        if self._wire_failure is not None:
            return self._fail(self._wire_failure, network_only=network_only)
        if self._socket is None:
            return 'DISCONNECTED'
        now = self.transport.now_ms()
        if now - self._created_ms >= 23 * 3600000:
            return self._fail('ROTATE_CONNECTION', network_only=network_only)
        # Local loop stalls invalidate observations even when a buffered pong
        # later arrives. A timeout of an idle recv by itself is not a failure.
        if now - self._last_poll_ms > 15000 or self._ping is not None and now - self._ping_ms > 10000:
            return self._fail('HEARTBEAT_TIMEOUT', network_only=network_only)
        self._last_poll_ms = now
        self._drained = False
        try:
            if not network_only and self._pending_event is not None:
                payload, arrival = self._pending_event
                if max(payload['E'], payload.get('T', payload['E'])) > now:
                    return 'WAIT_CLOCK_LAG'
                self._pending_event = None
                event = self.journal.observe(self._connection, payload, received_ms=now, first_arrival_ms=arrival)
                if not self.journal.state()['connected']:
                    self.close(self.journal.state()['reason'])
                return event
            if not network_only and self._wire_queue:
                raw, arrival = self._wire_queue.popleft()
                self._wire_bytes -= len(raw)
                return self._observe(raw, arrival, now)
            if self._ping is None and (now - self._last_pong_ms >= 5000 or not network_only and self._refresh_required):
                self._ping = secrets.token_bytes(12)
                self._ping_ms = now
                self._ping_for_journal = not network_only
                self._socket.ping(self._ping)
            if network_only and now - self._renewed_ms >= 55 * 60000:
                return self._fail('SUBSCRIPTION_FAILED', network_only=True)
            if not network_only and now - self._renewed_ms >= 30 * 60000:
                reply = self.transport.request('KEEP_ACCOUNT_STREAM', {})
                if reply.body.get('listenKey', self._listen_key) != self._listen_key:
                    self.close('SUBSCRIPTION_FAILED')
                    return 'DISCONNECTED'
                self._renewed_ms = reply.received_ms
                return 'RENEWED'
            opcode, frame = self._socket.recv_data_frame(control_frame=True)
            received = self.transport.now_ms()
            if opcode == websocket.ABNF.OPCODE_TEXT:
                raw = frame.data.encode('utf-8') if isinstance(frame.data, str) else frame.data
                if len(raw) > 1_000_000:
                    raise InvalidState('账户通知超出大小限制')
                if network_only:
                    if len(self._wire_queue) >= _WIRE_QUEUE_FRAMES or self._wire_bytes + len(raw) > _WIRE_QUEUE_BYTES:
                        return self._fail('SOCKET_FAILED', network_only=True)
                    self._wire_queue.append((raw, received)); self._wire_bytes += len(raw)
                    return 'BUFFERED_EVENT'
                return self._observe(raw, received, received)
            if opcode == websocket.ABNF.OPCODE_PONG:
                if self._ping is not None and frame.data == self._ping:
                    self._ping = None
                    self._last_pong_ms = received
                    if not network_only and self._ping_for_journal:
                        self.journal.heartbeat(self._connection, received_ms=received)
                        self._refresh_required = False
                        return 'HEARTBEAT'
                    return 'NETWORK_HEARTBEAT'
                return 'UNMATCHED_PONG'
            if opcode == websocket.ABNF.OPCODE_PING:
                # recv_data_frame already returns the RFC pong to the peer.
                return 'PEER_PING'
            return self._fail('SOCKET_FAILED', network_only=network_only)
        except websocket.WebSocketTimeoutException:
            # websocket-client 1.8.0 retains an incomplete frame/fragment after
            # recv timeout. A timeout while any of these buffers are occupied
            # is NOT an empty notification queue. Do not certify old balances.
            buffer=getattr(self._socket,'frame_buffer',None)
            fragments=getattr(self._socket,'cont_frame',None)
            if buffer is not None and (buffer.header is not None or buffer.recv_buffer) or fragments is not None and (fragments.cont_data is not None or fragments.recving_frames is not None):
                return 'WAIT_PARTIAL_FRAME'
            if not network_only and self._refresh_required:
                return 'WAIT_NETWORK_HEARTBEAT'
            self._drained = not network_only
            return 'IDLE'
        except TransportError as exc:
            if exc.reason in ('REQUEST_COOLDOWN', 'REQUEST_BUDGET_COOLDOWN', 'CLOCK_CALIBRATION_EXPIRED') and now - self._renewed_ms < 55 * 60000:
                # The outer service owns common pacing and clock renewal.
                return exc.reason
            return self._fail('SUBSCRIPTION_FAILED', network_only=network_only)
        except Exception:
            return self._fail('SOCKET_FAILED', network_only=network_only)

    def cursor(self):
        if (self._socket is None or not self._drained or self._wire_failure is not None
                or self._refresh_required or self._wire_queue or self.transport.now_ms() - self._last_poll_ms > 1000):
            raise InvalidState('账户通知尚未及时处理')
        return self.journal.cursor(now_ms=self.transport.now_ms())

    def drain(self, *, maximum_frames=100):
        """Bounded queue drain before REST barriers or broker decisions.

IDLE means the socket queue was read through to a receive timeout. It is not
a proof against future arrivals or silent exchange-side omissions.
"""
        if type(maximum_frames) is not int or not 1 <= maximum_frames <= 100:
            raise InvalidState('账户通知单轮读取上限无效')
        for _ in range(maximum_frames):
            outcome = self.poll()
            if outcome in ('IDLE', 'DISCONNECTED', 'WAIT_CLOCK_LAG', 'REQUEST_COOLDOWN',
                    'REQUEST_BUDGET_COOLDOWN', 'CLOCK_CALIBRATION_EXPIRED', 'WAIT_PARTIAL_FRAME', 'RENEWED', 'WAIT_NETWORK_HEARTBEAT'):
                return outcome
        return 'STREAM_BACKLOG'

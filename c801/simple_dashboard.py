"""Trade-first dashboard: local reads and a separate public chart worker.

The chart worker has no private credentials or trading callback. A chart miss
queues a bounded background fetch and immediately returns the fills already
saved. Strategy decisions never wait for an HTTP request to this dashboard.
"""
from contextlib import closing
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue, Empty, Full
from threading import Event, Lock, Thread
from urllib.parse import urlsplit, parse_qs
import hmac
import json
import secrets
import sqlite3
import time

from .account import InvalidState, PositionKey
from .simple_store import PERIODS, SimpleStore
from .simple_market import SimpleMarket


@dataclass(frozen=True)
class ChartNeed:
    venue: str
    symbol: str
    interval: str
    start_ms: int
    end_ms: int


class ChartWorker:
    def __init__(self, path, transport_factory, *, maximum_jobs=8):
        self.path = Path(path); self.factory = transport_factory
        self.queue = Queue(maximum_jobs); self.pending = set(); self.lock = Lock()
        self.stop = Event(); self.thread = None

    def request(self, need):
        with self.lock:
            if need in self.pending: return 'QUEUED'
            try: self.queue.put_nowait(need)
            except Full: return 'BUSY'
            self.pending.add(need)
        return 'QUEUED'

    def start(self):
        if self.thread is not None: return
        self.thread = Thread(target=self._run, name='c801-chart-display', daemon=True)
        self.thread.start()

    def _run(self):
        transport = None
        try:
            transport = self.factory()
            if getattr(transport, 'mutations_enabled', False) or getattr(transport, '_credentials', None) is not None:
                raise InvalidState('图表后台不能使用交易接口')
            while not self.stop.is_set():
                try: need = self.queue.get(timeout=.5)
                except Empty: continue
                try:
                    transport.sync_clock()
                    with closing(SimpleStore(self.path)) as store:
                        market = SimpleMarket(store)
                        # At most one page per job. The next display poll can
                        # request remaining holes without starving other views.
                        market.fetch_missing_page(transport, need)
                except Exception:
                    # The next user-visible refresh can queue another request.
                    # Neither a chart failure nor its exception reaches trading.
                    self.stop.wait(1)
                finally:
                    with self.lock: self.pending.discard(need)
                    self.queue.task_done()
        finally:
            if transport is not None: transport.close()

    def close(self):
        self.stop.set()
        if self.thread is not None: self.thread.join(timeout=6)


class DashboardReader:
    def __init__(self, path, *, enqueue_chart=None):
        self.path = Path(path).resolve(); self.enqueue = enqueue_chart

    def _open(self):
        if self.path.name != 'c801_simple.sqlite': raise InvalidState('网页只读取精简运行记录')
        db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=2)
        db.execute('PRAGMA query_only=ON')
        return db

    def status(self):
        if not self.path.exists():
            return dict(account=None, runtime=dict(mode='stopped', message='尚未启动策略'), fill_count=0)
        with closing(self._open()) as db:
            values = {name: json.loads(body) for name, body in db.execute("SELECT name,body FROM settings WHERE name IN ('account_display','runtime_display','dependency_display')")}
            count = db.execute('SELECT COUNT(*) FROM fills').fetchone()[0]
        return dict(account=values.get('account_display'), runtime=values.get('runtime_display', dict(mode='stopped', message='尚未启动策略')), fill_count=count,
                    dependencies=values.get('dependency_display'))

    def fills(self, *, before_ms=None, before_id=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 200: raise InvalidState('成交页大小错误')
        where = ''; params = []
        if before_ms is not None:
            if type(before_ms) is not int or before_ms < 0 or not isinstance(before_id, str): raise InvalidState('成交翻页位置错误')
            where = ' WHERE time_ms<? OR (time_ms=? AND trade_id<?)'
            params = [before_ms, before_ms, before_id]
        if not self.path.exists(): return dict(fills=[], next_cursor=None)
        with closing(self._open()) as db:
            rows = db.execute('SELECT body,sleeve,received_ms FROM fills' + where + ' ORDER BY time_ms DESC,trade_id DESC LIMIT ?', (*params, limit + 1)).fetchall()
        items = [dict(json.loads(body), sleeve=sleeve, received_ms=received) for body, sleeve, received in rows[:limit]]
        cursor = dict(before_ms=items[-1]['time_ms'], before_id=items[-1]['trade_id']) if len(rows) > limit else None
        return dict(fills=items, next_cursor=cursor)

    def chart(self, *, symbol, interval, now_ms, end_ms=None, count=180):
        PositionKey(symbol, 'BOTH')
        if interval not in ('1m','5m','15m') or type(count) is not int or not 30 <= count <= 499:
            raise InvalidState('图表周期或条数错误')
        if type(now_ms) is not int or now_ms < 0: raise InvalidState('图表时间错误')
        period = PERIODS[interval]; closed_end = now_ms // period * period
        if end_ms is not None and (type(end_ms) is not int or end_ms <= 0): raise InvalidState('图表窗口错误')
        # Actual fills in the currently forming interval appear immediately.
        # There is no invented candle for that interval and no strategy input.
        end = min(closed_end + period, closed_end + period if end_ms is None else end_ms // period * period)
        start = max(0, end - count * period)
        candle_end = min(end, closed_end)
        rows = []; markers = []
        if self.path.exists() and start < end:
            with closing(self._open()) as db:
                rows = db.execute('''SELECT start_ms,end_ms,body FROM (
                    SELECT start_ms,end_ms,body,ROW_NUMBER() OVER(PARTITION BY start_ms ORDER BY received_ms DESC) AS version
                    FROM candles WHERE venue='perpetual' AND symbol=? AND interval=? AND start_ms>=? AND end_ms<=? AND received_ms<=?)
                    WHERE version=1 ORDER BY start_ms''', (symbol,interval,start,candle_end,now_ms)).fetchall()
                markers = [dict(json.loads(body), sleeve=sleeve) for body, sleeve in db.execute(
                    'SELECT body,sleeve FROM fills WHERE symbol=? AND time_ms>=? AND time_ms<? AND received_ms<=? ORDER BY time_ms,trade_id', (symbol,start,end,now_ms))]
        candles = [dict(json.loads(b), start_ms=s, end_ms=e) for s,e,b in rows]
        missing = (candle_end - start) // period - len(candles)
        fetching = None
        if missing and self.enqueue is not None:
            fetching = self.enqueue(ChartNeed('perpetual', symbol, interval, start, candle_end))
        return dict(symbol=symbol, interval=interval, start_ms=start, end_ms=end,
                    candles=candles, markers=markers, missing=missing, history=fetching,
                    scale='linear', display_only=True, closed_end_ms=candle_end)


def dashboard_server(path, *, html_path, port=58445, chart_worker=None,
                     start_strategy=None, pause_entries=None, clock_ms=None):
    """Return a loopback server. No callback is invoked by construction or GET."""
    token = secrets.token_urlsafe(32)
    reader = DashboardReader(path, enqueue_chart=None if chart_worker is None else chart_worker.request)
    clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
    template = Path(html_path)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass

        def allowed_host(self):
            return self.headers.get('Host') == '127.0.0.1:' + str(self.server.server_port)

        def respond(self, status, body, kind='application/json; charset=utf-8'):
            data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False, allow_nan=False).encode()
            self.send_response(status); self.send_header('Content-Type', kind)
            self.send_header('Content-Length', str(len(data))); self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'")
            self.end_headers(); self.wfile.write(data)

        def do_GET(self):
            if not self.allowed_host(): return self.respond(403, {'error':'仅供本机访问'})
            url = urlsplit(self.path); params = parse_qs(url.query)
            try:
                if url.path == '/':
                    page=template.read_text(encoding='utf-8').replace('__CONTROL_TOKEN__',token).encode()
                    return self.respond(200,page,'text/html; charset=utf-8')
                if url.path == '/health':
                    return self.respond(200,dict(application='c801-simple',database=str(Path(path).resolve())))
                if url.path == '/api/status':
                    data = reader.status(); data['can_start'] = start_strategy is not None; data['can_pause'] = pause_entries is not None
                elif url.path == '/api/fills':
                    data = reader.fills(before_ms=int(params['before_ms'][0]) if 'before_ms' in params else None,
                                        before_id=params.get('before_id', [None])[0])
                elif url.path == '/api/chart':
                    data = reader.chart(symbol=params.get('symbol',['BTCUSDT'])[0], interval=params.get('interval',['1m'])[0],
                                        now_ms=clock_ms(), end_ms=int(params['end_ms'][0]) if 'end_ms' in params else None)
                else: return self.respond(404, {'error':'页面不存在'})
                self.respond(200, data)
            except (InvalidState, ValueError, KeyError): self.respond(400, {'error':'请求参数不正确'})
            except Exception: self.respond(503, {'error':'本机记录暂时不可读，请稍后重试'})

        def do_POST(self):
            origin = 'http://127.0.0.1:' + str(self.server.server_port)
            if (not self.allowed_host() or self.headers.get('Origin') != origin
                    or not hmac.compare_digest(self.headers.get('X-C801-Control',''), token)):
                return self.respond(403, {'error':'请从本机策略网页操作'})
            if self.headers.get('Content-Length', '0') not in ('0','2'):
                return self.respond(400, {'error':'控制请求不能携带额外参数'})
            if self.headers.get('Content-Length') == '2' and self.rfile.read(2) != b'{}':
                return self.respond(400, {'error':'控制请求内容错误'})
            callback = {'/api/start':start_strategy, '/api/pause':pause_entries}.get(self.path)
            if callback is None: return self.respond(409, {'error':'交易入口尚未接通'})
            try:
                result = callback(); self.respond(200, {'result':result})
            except Exception: self.respond(503, {'error':'操作未完成，请查看运行状态'})

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    server.daemon_threads = True
    return server

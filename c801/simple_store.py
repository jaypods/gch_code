"""Small durable working store for the direct strategy service.

Loading a strategy reads one current snapshot. There is no historical audit
replay, watchdog, release gate, or network operation in this module. Orders
awaiting a response keep their identity across restart; they are queried rather
than automatically submitted again. Candle arrival times remain explicit.
"""
from contextlib import contextmanager, nullcontext
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import sqlite3

from .account import InvalidState, PositionKey, positive, number
from .market import Bar


PERIODS = {'1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000,
           '1d': 86400000}
SLEEVES = ('M784', 'A787', 'S800')


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'),
                      allow_nan=False, default=lambda v: str(v) if isinstance(v, Decimal) else _unsupported(v))


def _unsupported(value):
    raise TypeError('Unsupported stored value: ' + type(value).__name__)


def _stamp(value):
    if type(value) is not int or value < 0:
        raise InvalidState('时间必须为非负毫秒整数')
    return value


class SimpleStore:
    def __init__(self, path):
        self.path = Path(path).resolve()
        # This service never modifies the previous account/audit databases.
        if self.path.name != 'c801_simple.sqlite':
            raise InvalidState('精简运行记录必须使用独立的 c801_simple.sqlite')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS strategy_state(
                id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL,
                clock_ms INTEGER NOT NULL, payload BLOB NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS orders(
                client_id TEXT PRIMARY KEY, sleeve TEXT NOT NULL, body TEXT NOT NULL,
                status TEXT NOT NULL, updated_ms INTEGER NOT NULL, result TEXT);
            CREATE TABLE IF NOT EXISTS fills(
                symbol TEXT NOT NULL, trade_id TEXT NOT NULL, order_id TEXT NOT NULL,
                sleeve TEXT, time_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL,
                body TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(symbol,trade_id));
            CREATE INDEX IF NOT EXISTS fills_time ON fills(time_ms,trade_id);
            CREATE TABLE IF NOT EXISTS receipts(
                event_id TEXT PRIMARY KEY, sleeve TEXT NOT NULL, lot_id TEXT NOT NULL,
                body TEXT NOT NULL, available_ms INTEGER NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0, fill_symbol TEXT, fill_id TEXT);
            CREATE TABLE IF NOT EXISTS candles(
                venue TEXT NOT NULL, symbol TEXT NOT NULL, interval TEXT NOT NULL,
                start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
                received_ms INTEGER NOT NULL, body TEXT NOT NULL,
                PRIMARY KEY(venue,symbol,interval,start_ms,received_ms));
            CREATE TABLE IF NOT EXISTS settings(name TEXT PRIMARY KEY,body TEXT NOT NULL);
        ''')

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        if self.db.in_transaction:
            raise InvalidState('不能嵌套运行记录事务')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def state(self):
        row = self.db.execute('SELECT revision,clock_ms,payload,digest FROM strategy_state WHERE id=1').fetchone()
        if row is None:
            return None
        if hashlib.sha256(row[2]).hexdigest() != row[3]:
            raise InvalidState('最新策略状态文件损坏')
        return dict(revision=row[0], clock_ms=row[1], payload=row[2])

    def save_state(self, payload, *, clock_ms, expected_revision, actions=(), consumed_fills=(),
                   consumed_receipts=(), decision_ms=None):
        """Publish the state, consumed fills and new order identities together."""
        _stamp(clock_ms)
        decision_ms = clock_ms if decision_ms is None else _stamp(decision_ms)
        if not clock_ms <= decision_ms < clock_ms + 60000:
            raise InvalidState('状态提交必须在当前决策分钟内')
        if type(payload) is not bytes or not payload or len(payload) > 128 * 1024**2:
            raise InvalidState('策略状态内容错误')
        if type(expected_revision) is not int or expected_revision < 0:
            raise InvalidState('策略状态版本错误')
        prepared = []
        for action in actions:
            if (not isinstance(action, dict) or action.get('sleeve') not in SLEEVES
                    or type(action.get('id')) is not str or not 1 <= len(action['id']) <= 128):
                raise InvalidState('策略动作缺少稳定身份或归属')
            available_ms = _stamp(action.get('known_ms', clock_ms))
            if available_ms < clock_ms:
                raise InvalidState('策略动作不能早于当前决策周期')
            prepared.append((action['id'], action['sleeve'], _json(action), available_ms))
        if len({a[0] for a in prepared}) != len(prepared):
            raise InvalidState('同一批动作身份重复')
        with self.transaction():
            old = self.db.execute('SELECT revision,clock_ms FROM strategy_state WHERE id=1').fetchone()
            if (old[0] if old else 0) != expected_revision or old and clock_ms < old[1]:
                raise InvalidState('策略状态已更新或时钟倒退')
            for identity, sleeve, body, available_ms in prepared:
                previous = self.db.execute('SELECT body FROM orders WHERE client_id=?', (identity,)).fetchone()
                if previous is not None and previous[0] != body:
                    raise InvalidState('相同动作身份对应不同下单内容')
                if previous is None:
                    self.db.execute('INSERT INTO orders VALUES(?,?,?,?,?,NULL)',
                                    (identity, sleeve, body, 'PENDING', available_ms))
            for symbol, identity in consumed_fills:
                changed = self.db.execute('UPDATE fills SET delivered=1 WHERE symbol=? AND trade_id=? AND delivered=0 AND sleeve IS NOT NULL',
                                          (symbol, str(identity))).rowcount
                if changed != 1:
                    raise InvalidState('成交记录不存在或已应用到策略')
            for identity in consumed_receipts:
                row = self.db.execute('SELECT delivered,available_ms,fill_symbol,fill_id FROM receipts WHERE event_id=?', (identity,)).fetchone()
                if row is None or row[0] or row[1] > decision_ms:
                    raise InvalidState('回报不存在、已应用或尚未到原策略可用时间')
                self.db.execute('UPDATE receipts SET delivered=1 WHERE event_id=?', (identity,))
                if row[2] is not None:
                    self.db.execute('UPDATE fills SET delivered=1 WHERE symbol=? AND trade_id=?', (row[2], row[3]))
            revision = expected_revision + 1
            self.db.execute('INSERT INTO strategy_state VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,clock_ms=excluded.clock_ms,payload=excluded.payload,digest=excluded.digest',
                            (revision, clock_ms, payload, hashlib.sha256(payload).hexdigest()))
        return revision

    def orders(self, *, statuses=('PENDING',)):
        if not statuses:
            return []
        rows = self.db.execute('SELECT client_id,sleeve,body,status,updated_ms,result FROM orders WHERE status IN (' + ','.join('?' for _ in statuses) + ') ORDER BY updated_ms,client_id', tuple(statuses)).fetchall()
        return [dict(id=r[0], sleeve=r[1], action=json.loads(r[2]), status=r[3],
                     updated_ms=r[4], result=None if r[5] is None else json.loads(r[5])) for r in rows]

    def claim_order(self, identity, *, now_ms):
        """Persist SENDING before contacting Binance; a retry cannot reclaim it."""
        _stamp(now_ms)
        with self.transaction():
            changed = self.db.execute("UPDATE orders SET status='SENDING',updated_ms=? WHERE client_id=? AND status='PENDING' AND updated_ms<=?", (now_ms, identity, now_ms)).rowcount
            if not changed:
                return None
            return json.loads(self.db.execute('SELECT body FROM orders WHERE client_id=?', (identity,)).fetchone()[0])

    def order_result(self, identity, *, status, result, now_ms):
        _stamp(now_ms)
        if status not in ('ACKNOWLEDGED', 'UNKNOWN', 'REJECTED', 'FINISHED'):
            raise InvalidState('订单结果状态错误')
        body = _json(result)
        with self.transaction():
            old = self.db.execute('SELECT status,updated_ms FROM orders WHERE client_id=?', (identity,)).fetchone()
            if old is None or old[0] == 'PENDING' or now_ms < old[1]:
                raise InvalidState('订单尚未发送或结果时间倒退')
            if old[0] in ('REJECTED', 'FINISHED'):
                if old[0] != status:
                    raise InvalidState('不能重新开启已经结束的订单')
                return False
            self.db.execute('UPDATE orders SET status=?,updated_ms=?,result=? WHERE client_id=?', (status, now_ms, body, identity))
        return True

    def put_fill(self, fill, *, sleeve=None, received_ms):
        """One actual exchange fill appears once, even after reconnect/requery."""
        _stamp(received_ms)
        symbol = fill['symbol']; PositionKey(symbol, 'BOTH')
        if sleeve is not None and sleeve not in SLEEVES:
            raise InvalidState('成交策略归属错误')
        at = _stamp(fill['time_ms'])
        if at > received_ms or fill['side'] not in ('BUY', 'SELL'):
            raise InvalidState('成交方向或接收时间错误')
        row = dict(fill, price=str(positive(fill['price'])), quantity=str(positive(fill['quantity'])),
                   fee=str(number(fill.get('fee', '0'))), trade_id=str(fill['trade_id']), order_id=str(fill['order_id']))
        body = _json(row)
        with (nullcontext() if self.db.in_transaction else self.transaction()):
            old = self.db.execute('SELECT body,sleeve FROM fills WHERE symbol=? AND trade_id=?', (symbol, row['trade_id'])).fetchone()
            if old is not None:
                if old != (body, sleeve):
                    raise InvalidState('同一成交身份出现不同内容或策略归属')
                return False
            self.db.execute('INSERT INTO fills VALUES(?,?,?,?,?,?,?,0)',
                            (symbol, row['trade_id'], row['order_id'], sleeve, at, received_ms, body))
        return True

    def fills(self, *, since_ms=0, limit=1000, pending=False):
        _stamp(since_ms)
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise InvalidState('成交读取数量错误')
        clause = ' AND delivered=0 AND sleeve IS NOT NULL' if pending else ''
        order = 'time_ms,trade_id' if pending else 'time_ms DESC,trade_id DESC'
        rows = self.db.execute('SELECT body,sleeve,received_ms,delivered FROM fills WHERE time_ms>=?' + clause + ' ORDER BY ' + order + ' LIMIT ?', (since_ms, limit)).fetchall()
        return [dict(json.loads(b), sleeve=s, received_ms=t, delivered=bool(done)) for b, s, t, done in rows]

    def put_receipt(self, sleeve, lot_id, receipt, *, fill_identity=None):
        from .strategies.main import MainReceipt
        from .strategies.alt import AltReceipt
        from .strategies.stock import StockReceipt
        classes = dict(M784=MainReceipt, A787=AltReceipt, S800=StockReceipt)
        if sleeve not in classes or type(receipt) is not classes[sleeve] or not lot_id.startswith(sleeve + ':'):
            raise InvalidState('回报所属策略或批次错误')
        receipt.validate()
        for name in ('event_ms', 'received_ms', 'available_ms'):
            _stamp(getattr(receipt, name))
        body = _json(asdict(receipt))
        symbol, trade_id = (None, None) if fill_identity is None else (fill_identity[0], str(fill_identity[1]))
        values = (sleeve, lot_id, body, receipt.available_ms, symbol, trade_id)
        with (nullcontext() if self.db.in_transaction else self.transaction()):
            old = self.db.execute('SELECT sleeve,lot_id,body,available_ms,fill_symbol,fill_id FROM receipts WHERE event_id=?', (receipt.event_id,)).fetchone()
            if old is not None:
                if old != values:
                    raise InvalidState('同一回报身份发生变化')
                return False
            if symbol is not None:
                fill = self.db.execute('SELECT sleeve FROM fills WHERE symbol=? AND trade_id=?', (symbol, trade_id)).fetchone()
                if fill != (sleeve,) or self.db.execute('SELECT 1 FROM receipts WHERE fill_symbol=? AND fill_id=?', (symbol, trade_id)).fetchone():
                    raise InvalidState('成交没有匹配归属或已经发布回报')
            self.db.execute('INSERT INTO receipts(event_id,sleeve,lot_id,body,available_ms,fill_symbol,fill_id) VALUES(?,?,?,?,?,?,?)',
                            (receipt.event_id, *values))
        return True

    def pending_receipts(self, *, now_ms):
        _stamp(now_ms)
        rows = self.db.execute('SELECT event_id,sleeve,lot_id,body FROM receipts WHERE delivered=0 AND available_ms<=? ORDER BY available_ms,rowid', (now_ms,)).fetchall()
        return [dict(id=i, sleeve=s, lot_id=l, receipt=json.loads(b)) for i, s, l, b in rows]

    def put_candles(self, venue, interval, observations):
        """Closed candles only. Corrections keep their later arrival time."""
        if venue not in ('perpetual', 'spot') or interval not in PERIODS:
            raise InvalidState('K线市场或周期错误')
        with self.transaction():
            for observation in observations:
                bar = observation if isinstance(observation, Bar) else observation.bar
                if not isinstance(bar, Bar) or bar.period_ms != PERIODS[interval]:
                    raise InvalidState('K线尚未收盘或周期错误')
                data = {k: str(getattr(bar, k)) for k in ('open','high','low','close','volume','quote_volume')}
                for name in ('buy_volume', 'buy_quote', 'trades'):
                    if hasattr(observation, name): data[name] = str(getattr(observation, name))
                body = _json(data)
                key = (venue, bar.symbol, interval, bar.start_ms)
                old = self.db.execute('SELECT received_ms,body FROM candles WHERE venue=? AND symbol=? AND interval=? AND start_ms=? ORDER BY received_ms DESC LIMIT 1', key).fetchone()
                if old is not None:
                    if old[1] == body:
                        continue
                    if bar.received_ms <= old[0]:
                        raise InvalidState('K线修订不能伪装成更早可知的数据')
                self.db.execute('INSERT INTO candles VALUES(?,?,?,?,?,?,?)', (*key, bar.end_ms, bar.received_ms, body))

    def candles(self, venue, symbol, interval, *, start_ms, end_ms, asof_ms):
        PositionKey(symbol, 'BOTH')
        for t in (start_ms, end_ms, asof_ms): _stamp(t)
        if interval not in PERIODS or venue not in ('perpetual', 'spot') or start_ms >= end_ms:
            raise InvalidState('K线窗口错误')
        rows = self.db.execute('''SELECT start_ms,end_ms,received_ms,body FROM (
            SELECT start_ms,end_ms,received_ms,body,
                   ROW_NUMBER() OVER(PARTITION BY start_ms ORDER BY received_ms DESC) AS n
            FROM candles WHERE venue=? AND symbol=? AND interval=?
                AND start_ms>=? AND start_ms<? AND end_ms<=? AND end_ms<=? AND received_ms<=?)
            WHERE n=1 ORDER BY start_ms''', (venue,symbol,interval,start_ms,end_ms,end_ms,asof_ms,asof_ms)).fetchall()
        return [dict(json.loads(b), symbol=symbol, venue=venue, interval=interval,
                     start_ms=s, end_ms=e, received_ms=r) for s,e,r,b in rows]

    def set_value(self, name, value):
        self.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(name) DO UPDATE SET body=excluded.body', (name, _json(value)))

    def value(self, name, default=None):
        row = self.db.execute('SELECT body FROM settings WHERE name=?', (name,)).fetchone()
        return default if row is None else json.loads(row[0])

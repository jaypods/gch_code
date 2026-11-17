"""Incremental actual trade/income inbox. No audit replay or release gate."""
import json

from .account import InvalidState, PositionKey, number
from .simple_orders import stamp, identity
from .simple_store import _json


class SimpleHistory:
    def __init__(self, exchange):
        self.exchange = exchange
        self.store, self.db = exchange.store, exchange.store.db
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS simple_history_windows(
                id TEXT PRIMARY KEY,kind TEXT NOT NULL,symbol TEXT NOT NULL,
                start_ms INTEGER NOT NULL,end_ms INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'PENDING',received_ms INTEGER);
            CREATE TABLE IF NOT EXISTS simple_raw_trades(
                symbol TEXT NOT NULL,trade_id TEXT NOT NULL,order_id TEXT NOT NULL,
                event_ms INTEGER NOT NULL,observed_ms INTEGER NOT NULL,body TEXT NOT NULL,
                bound INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(symbol,trade_id));
            CREATE INDEX IF NOT EXISTS simple_raw_trades_order ON simple_raw_trades(symbol,order_id,bound);
            CREATE TABLE IF NOT EXISTS simple_raw_funding(
                transaction_id TEXT PRIMARY KEY,event_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,observed_ms INTEGER NOT NULL,body TEXT NOT NULL);
        ''')

    def schedule(self, kind, *, start_ms, end_ms, symbol='*'):
        stamp(start_ms); stamp(end_ms)
        if kind not in ('TRADES', 'INCOME') or not start_ms <= end_ms < self.exchange.transport.now_ms():
            raise InvalidState('实际账单读取范围错误')
        if kind == 'TRADES':
            PositionKey(symbol, 'BOTH')
        elif symbol != '*':
            raise InvalidState('账户流水读取使用完整的时间窗口')
        ids = []
        # The exchange bounds each request; a multi-week outage is still
        # incremental, without one huge request or hidden omitted periods.
        while start_ms <= end_ms:
            last = min(end_ms, start_ms + 7*86400000 - 1)
            wid = identity([kind, symbol, start_ms, last])
            self.db.execute('INSERT OR IGNORE INTO simple_history_windows(id,kind,symbol,start_ms,end_ms) VALUES(?,?,?,?,?)',
                            (wid, kind, symbol, start_ms, last))
            ids.append(wid)
            start_ms = last + 1
        return ids

    def collect_one(self):
        row = self.db.execute("SELECT id,kind,symbol,start_ms,end_ms FROM simple_history_windows WHERE state='PENDING' ORDER BY start_ms,rowid LIMIT 1").fetchone()
        if row is None:
            return None
        wid, kind, symbol, begin, end = row
        if kind == 'TRADES':
            page = self.exchange.trade_page(symbol, start_ms=begin, end_ms=end)
        else:
            page = self.exchange.income_page(start_ms=begin, end_ms=end)
        with self.store.transaction():
            if kind == 'TRADES':
                for raw in page['rows']:
                    body = _json(raw)
                    old = self.db.execute('SELECT body FROM simple_raw_trades WHERE symbol=? AND trade_id=?', (symbol,str(raw['id']))).fetchone()
                    if old is not None and json.loads(old[0]) != raw:
                        raise InvalidState('相同交易所成交编号的内容发生变化')
                    self.db.execute('INSERT OR IGNORE INTO simple_raw_trades(symbol,trade_id,order_id,event_ms,observed_ms,body) VALUES(?,?,?,?,?,?)',
                        (symbol,str(raw['id']),str(raw['orderId']),raw['time'],page['received_ms'],body))
            else:
                for raw in page['rows']:
                    if raw['incomeType'] != 'FUNDING_FEE' or raw['asset'] != 'USDT':
                        continue
                    tid = str(raw['tranId'])
                    old = self.db.execute('SELECT body FROM simple_raw_funding WHERE transaction_id=?', (tid,)).fetchone()
                    if old is not None and json.loads(old[0]) != raw:
                        raise InvalidState('相同资金费流水的内容发生变化')
                    self.db.execute('INSERT OR IGNORE INTO simple_raw_funding VALUES(?,?,?,?,?)',
                                    (tid,raw['time'],raw['symbol'],page['received_ms'],_json(raw)))
            state = 'COMPLETE' if page['complete'] else 'SATURATED' if begin == end else 'SPLIT'
            self.db.execute('UPDATE simple_history_windows SET state=?,received_ms=? WHERE id=?', (state,page['received_ms'],wid))
            if state == 'SPLIT':
                for part in page['remaining']:
                    self.schedule(kind, start_ms=part['start_ms'], end_ms=part['end_ms'], symbol=symbol)
        return dict(id=wid,kind=kind,symbol=symbol,state=state,rows=len(page['rows']))

    def bind_known(self, *, limit=100):
        """Late order reports can bind an already saved fill after restart."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise InvalidState('成交处理数量错误')
        rows = self.db.execute('''SELECT t.symbol,t.trade_id,t.body,t.observed_ms,r.id
            FROM simple_raw_trades t JOIN simple_requests r ON r.exchange_id=t.order_id
            AND json_extract(r.body,'$.arguments.symbol')=t.symbol
            WHERE t.bound=0 AND r.exchange_status IS NOT NULL AND r.operation IN ('SEND_ORDER','ALGO_CHILD')
            ORDER BY t.event_ms,CAST(t.trade_id AS INTEGER) LIMIT ?''', (limit,)).fetchall()
        imported = 0
        for symbol, tid, raw, observed, request_id in rows:
            imported += int(self.exchange.broker.record_trade(request_id,json.loads(raw),
                received_ms=self.exchange.transport.now_ms(),observed_ms=observed))
            # A crash between the two commits is harmless: record_trade is
            # keyed to the exact exchange fill identity and cannot double it.
            self.db.execute('UPDATE simple_raw_trades SET bound=1 WHERE symbol=? AND trade_id=?', (symbol,tid))
        return imported

    def covered(self, kind, *, start_ms, end_ms, symbol='*'):
        stamp(start_ms); stamp(end_ms)
        if start_ms > end_ms:
            raise InvalidState('账单区间倒序')
        cursor = start_ms
        for begin, end in self.db.execute("SELECT start_ms,end_ms FROM simple_history_windows WHERE kind=? AND symbol=? AND state='COMPLETE' AND end_ms>=? AND start_ms<=? ORDER BY start_ms,end_ms",
                                          (kind,symbol,start_ms,end_ms)):
            if begin > cursor:
                return False
            cursor = max(cursor,end+1)
            if cursor > end_ms:
                return True
        return False

    def position_at(self, snapshot, *, symbol, event_ms):
        """Infer past quantity only from actual current quantity + all trades.

        Both sides of a non-atomic account sample must have no intervening
        fills. An event sharing a millisecond with a fill stays ambiguous.
        Manual trades are included rather than silently treated as strategy.
        """
        stamp(event_ms);PositionKey(symbol,'BOTH')
        if event_ms > snapshot.exchange_ms or not self.covered('TRADES',symbol=symbol,
                start_ms=event_ms,end_ms=snapshot.received_ms):
            raise InvalidState('尚未收到资金费时点至账户取样之间的完整成交')
        rows = self.db.execute('SELECT event_ms,body FROM simple_raw_trades WHERE symbol=? AND event_ms>=? AND event_ms<=? ORDER BY event_ms',
                               (symbol,event_ms,snapshot.received_ms)).fetchall()
        if any(t == event_ms or t >= snapshot.exchange_ms for t, raw in rows):
            raise InvalidState('资金费或账户取样与成交交错，不能猜测持仓先后')
        quantities = {p.key:p.amount for p in snapshot.positions if p.key.symbol == symbol and p.amount}
        for t, raw in rows:
            f = json.loads(raw); key = PositionKey(symbol, f['positionSide'])
            quantities[key] = quantities.get(key,number(0)) - number(f['qty']) * (1 if f['side'] == 'BUY' else -1)
        return {k:q for k,q in quantities.items() if q}

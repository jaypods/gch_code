"""Small durable broker queue for the user-operated strategy service.

No watchdog, audit replay, release approval, historical backtest, or chart
dependency. Orders are identified before transmission. Network uncertainty is
resolved by querying the original identity, never by submitting it again.
"""
from contextlib import nullcontext
from decimal import Decimal as D, ROUND_FLOOR
import json

from .account import InvalidState, number, positive, nonnegative
from .binance_transport import WireReply, TransportError, validate_parameters
from .simple_orders import (FIVE, MINUTE, stamp, identity, normalize_action,
                            entry_attempt, fill_receipt, receipt_available)
from .simple_store import _json

TERMINAL = {'FILLED', 'CANCELED', 'REJECTED', 'EXPIRED', 'EXPIRED_IN_MATCH'}
ALGO_TERMINAL = {'CANCELED', 'REJECTED', 'EXPIRED', 'FINISHED'}
# Explicit refusals from the official USD-M error reference (2026-11-15).
# Unknown execution (-1000/-1006/-1007), duplicate IDs and 5xx are excluded.
# https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/error-code
DEFINITE_REFUSALS = frozenset({-1021,-1022,-1100,-1101,-1102,-1103,-1104,-1105,-1106,
    -1111,-1114,-1115,-1116,-1117,-1118,-1119,-1120,-1121,-1130,
    -2014,-2015,-2018,-2019,-2020,-2021,-2022,-2023,-2024,-2025,-2026,-2027,-2028,
    -4001,-4002,-4003,-4004,-4005,-4006,-4007,-4013,-4014,-4016,-4023,-4164})
PRIORITY = {'CANCEL_ENTRY': 0, 'ENTRY_AMEND': 1, 'EXIT_REQUEST': 2,
            'STOP_AMEND': 3, 'PROTECTION_AMEND': 3, 'PROTECT_HALF': 3, 'ENTRY_CHECK': 10}


class SimpleBroker:
    def __init__(self, store):
        self.store = store; self.db = store.db
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS simple_plans(
                lot_id TEXT PRIMARY KEY, body TEXT NOT NULL,
                entry_closed INTEGER NOT NULL DEFAULT 0, first_entry_ms INTEGER,
                cancel_requested INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS simple_tasks(
                id TEXT PRIMARY KEY, lot_id TEXT NOT NULL, kind TEXT NOT NULL,
                due_ms INTEGER NOT NULL, body TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'PENDING', result TEXT);
            CREATE INDEX IF NOT EXISTS simple_tasks_due ON simple_tasks(state,due_ms);
            CREATE TABLE IF NOT EXISTS simple_requests(
                id TEXT PRIMARY KEY, task_id TEXT, lot_id TEXT NOT NULL,
                purpose TEXT NOT NULL, operation TEXT NOT NULL, body TEXT NOT NULL,
                created_ms INTEGER NOT NULL, expires_ms INTEGER,
                state TEXT NOT NULL DEFAULT 'PENDING', updated_ms INTEGER NOT NULL,
                exchange_id TEXT, exchange_status TEXT, cumulative TEXT NOT NULL DEFAULT '0',
                exchange_ms INTEGER NOT NULL DEFAULT 0, response TEXT, child_id TEXT);
        ''')

    def plan(self, lot_id):
        row = self.db.execute('SELECT body,entry_closed,first_entry_ms,cancel_requested FROM simple_plans WHERE lot_id=?', (lot_id,)).fetchone()
        if row is None: raise InvalidState('委托没有原策略计划')
        return dict(json.loads(row[0]), entry_closed=bool(row[1]), first_entry_ms=row[2], cancel_requested=bool(row[3]))

    def _schedule(self, plan, kind, due, source, payload=None):
        body = dict(lot_id=plan['lot_id'], kind=kind, due_ms=due, source=source, payload=payload or {})
        task_id = identity(body)
        self.db.execute('INSERT OR IGNORE INTO simple_tasks(id,lot_id,kind,due_ms,body) VALUES(?,?,?,?,?)',
                        (task_id, plan['lot_id'], kind, due, _json(body)))
        return task_id

    def accept_actions(self, *, now_ms):
        """Commit schedules from the strategy outbox; no network side effects."""
        stamp(now_ms); accepted = []
        with self.store.transaction():
            for row in self.store.orders():
                a = row['action']
                if a['known_ms'] > now_ms: continue
                first = self.db.execute('SELECT first_entry_ms FROM simple_plans WHERE lot_id=?', (a['lot_id'],)).fetchone()
                normalized = normalize_action(a['sleeve'], a['action'], now_ms=now_ms,
                                               first_entry_ms=first[0] if first else None)
                if normalized != a: raise InvalidState('保存的动作与原策略内容不符')
                if a['kind'] == 'ENTRY_PLAN':
                    if self.db.execute('SELECT 1 FROM simple_plans WHERE lot_id=?', (a['lot_id'],)).fetchone():
                        raise InvalidState('同一原策略计划不能重复创建')
                    self.db.execute('INSERT INTO simple_plans(lot_id,body) VALUES(?,?)', (a['lot_id'], _json(a)))
                    self._schedule(a, 'ENTRY_CHECK', a['due_ms'], a['id'])
                    self._schedule(a, 'CANCEL_ENTRY', a['expiry_ms'], a['id'], {'reason': 'entry_expiry'})
                    deadline = a['action']['order'].get('deadline_ms')
                    if deadline is not None:
                        self._schedule(a, 'EXIT_REQUEST', stamp(deadline), a['id'], {'reason': 'holding_deadline'})
                else:
                    self.plan(a['lot_id'])
                    self._schedule(a, a['kind'], a['due_ms'], a['id'], a['action'])
                self.db.execute("UPDATE orders SET status='FINISHED',updated_ms=?,result=? WHERE client_id=? AND status='PENDING'",
                                (now_ms, _json({'result': 'SCHEDULED'}), a['id']))
                accepted.append(a['id'])
        return accepted

    def task(self, task_id):
        row = self.db.execute('SELECT body,state,result FROM simple_tasks WHERE id=?', (task_id,)).fetchone()
        if row is None: raise InvalidState('执行时点不存在')
        return dict(json.loads(row[0]), id=task_id, state=row[1], result=json.loads(row[2]) if row[2] else None)

    def due(self, *, now_ms):
        stamp(now_ms)
        rows = [self.task(r[0]) for r in self.db.execute("SELECT id FROM simple_tasks WHERE state='PENDING' AND due_ms<=?", (now_ms,))]
        return sorted(rows, key=lambda t: (t['due_ms'], PRIORITY[t['kind']], t['id']))

    def finish_local_entry(self, task_id, result, *, now_ms):
        """Handle a price miss/rejection without inventing an exchange order."""
        with self.store.transaction():
            t = self.task(task_id); p = self.plan(t['lot_id'])
            if t['state'] != 'PENDING' or t['kind'] != 'ENTRY_CHECK' or now_ms < t['due_ms']:
                raise InvalidState('本地入场结果没有对应的到期尝试')
            if result['result'] == 'PREPARED': raise InvalidState('已准备的委托需要冻结发送内容')
            if self.db.execute('SELECT 1 FROM simple_requests WHERE task_id=?', (task_id,)).fetchone():
                raise InvalidState('已有实际委托，不能当成本地拒绝')
            if result['result'] == 'WAIT_LIMIT':
                next_due = t['due_ms'] + FIVE
                if p['sleeve'] != 'M784' or not t['due_ms'] <= now_ms < t['due_ms'] + MINUTE:
                    raise InvalidState('等待限价不是本周期主流币检查')
                if next_due < p['expiry_ms'] and not p['entry_closed'] and not p['cancel_requested']:
                    self._schedule(p, 'ENTRY_CHECK', next_due, p['id'])
                else: self._close_entry(p, now_ms)
            else:
                self._close_entry(p, now_ms)
            self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?", (_json(result), task_id))

    def _insert_request(self, *, request_id, task_id, lot_id, purpose, operation, body, now_ms, expires_ms=None):
        validate_parameters(operation, body['arguments'])
        if expires_ms is not None and not now_ms < stamp(expires_ms):
            raise InvalidState('发送窗口已经结束')
        values = (task_id, lot_id, purpose, operation, _json(body), now_ms, expires_ms)
        old = self.db.execute('SELECT task_id,lot_id,purpose,operation,body,created_ms,expires_ms FROM simple_requests WHERE id=?', (request_id,)).fetchone()
        if old is not None:
            if old != values: raise InvalidState('相同委托身份不能改变数量或用途')
            return False
        self.db.execute('INSERT INTO simple_requests(id,task_id,lot_id,purpose,operation,body,created_ms,expires_ms,updated_ms) VALUES(?,?,?,?,?,?,?,?,?)',
                        (request_id, *values, now_ms))
        return True

    def freeze_entry(self, task_id, prepared, *, now_ms):
        """The full initial stop/target are saved before the entry can be sent."""
        stamp(now_ms)
        with self.store.transaction():
            t = self.task(task_id); p = self.plan(t['lot_id']); attempt = entry_attempt(p, t['due_ms'])
            if (t['state'], t['kind'], p['entry_closed']) != ('PENDING', 'ENTRY_CHECK', False) or p['cancel_requested']:
                raise InvalidState('入场计划已使用或已终止')
            if prepared['result'] != 'PREPARED' or prepared['id'] != attempt['id'] or prepared['lot_id'] != p['lot_id']:
                raise InvalidState('冻结的入场委托不属于当前计划')
            if not t['due_ms'] <= prepared['created_ms'] <= now_ms < prepared['expires_ms'] <= attempt['expires_ms']:
                raise InvalidState('冻结委托已经错过可发送时点')
            if self.db.execute("SELECT 1 FROM simple_requests WHERE lot_id=? AND purpose='ENTRY' AND state NOT IN ('TERMINAL','NOT_SENT')", (p['lot_id'],)).fetchone():
                raise InvalidState('上一入场结果未确定，不重复开仓')
            self._insert_request(request_id=prepared['id'], task_id=task_id, lot_id=p['lot_id'], purpose='ENTRY',
                                 operation='SEND_ORDER', body=prepared, now_ms=now_ms, expires_ms=prepared['expires_ms'])
            self.db.execute("UPDATE simple_tasks SET state='WAITING' WHERE id=?", (task_id,))
        return prepared['id']

    def request_record(self, request_id):
        cursor = self.db.execute('SELECT * FROM simple_requests WHERE id=?', (request_id,))
        row = cursor.fetchone()
        if row is None: raise InvalidState('委托尚未保存')
        result = dict(zip((d[0] for d in cursor.description), row))
        result['body'] = json.loads(result['body'])
        result['response'] = json.loads(result['response']) if result['response'] else None
        return result

    def requests(self, *, states=('PENDING', 'SENDING', 'UNKNOWN', 'ACKNOWLEDGED')):
        if not states: return []
        ids = self.db.execute('SELECT id FROM simple_requests WHERE state IN (' + ','.join('?' for _ in states) + ') ORDER BY created_ms,id', tuple(states)).fetchall()
        return [self.request_record(r[0]) for r in ids]

    def send_once(self, request_id, transport, *, enabled=False):
        """User-operated runtime only. Tests use a fake, unauthenticated venue.

        A callback at the actual wire boundary excludes orders that expired
        while waiting for a network connection or rate limit.
        """
        if enabled is not True or getattr(transport, 'mutations_enabled', False) is not True:
            raise InvalidState('实盘开关未开启')
        now = transport.now_ms(); stamp(now)
        with self.store.transaction():
            r = self.request_record(request_id)
            if r['state'] != 'PENDING': return 'QUERY_REQUIRED'
            if now < r['created_ms']: raise InvalidState('不能提前发送委托')
            if r['expires_ms'] is not None and now >= r['expires_ms']:
                self.db.execute("UPDATE simple_requests SET state='NOT_SENT',updated_ms=? WHERE id=?", (now, request_id))
                return 'NOT_SENT'
            if r['operation'] == 'SEND_PROTECTION' and number(r['body']['arguments']['quantity']) > self.lot_totals(r['lot_id'])['remaining']:
                self.db.execute("UPDATE simple_requests SET state='NOT_SENT',updated_ms=? WHERE id=?", (now, request_id))
                return 'NOT_SENT'
            self.db.execute("UPDATE simple_requests SET state='SENDING',updated_ms=? WHERE id=?", (now, request_id))
        reached_wire = False
        expired_before_wire = False
        def before_attempt():
            nonlocal reached_wire, expired_before_wire
            at = transport.now_ms()
            if r['expires_ms'] is not None and at >= r['expires_ms']:
                expired_before_wire = True
                raise InvalidState('委托在实际发送前已经过期')
            reached_wire = True
        try:
            reply = transport.request(r['operation'], r['body']['arguments'], before_attempt=before_attempt)
            if not reached_wire or not isinstance(reply, WireReply) or not now <= reply.sent_ms <= reply.received_ms:
                raise InvalidState('通信缺少实际发送和接收时点')
        except Exception as exc:
            at = transport.now_ms()
            if (reached_wire and r['operation'] in ('SEND_ORDER','SEND_PROTECTION','CHANGE_LEVERAGE')
                    and isinstance(exc,TransportError) and exc.http_status in (200,400,401)
                    and exc.exchange_code in DEFINITE_REFUSALS):
                self.db.execute("UPDATE simple_requests SET state='TERMINAL',updated_ms=?,exchange_status='REJECTED',exchange_ms=?,response=? WHERE id=?",
                    (at,at,_json(dict(http_status=exc.http_status,exchange_code=exc.exchange_code,rejected=True)),request_id))
                return 'REJECTED'
            state = 'NOT_SENT' if not reached_wire else 'UNKNOWN'
            # Never persist exception text, signed URLs, headers or credentials.
            self.db.execute('UPDATE simple_requests SET state=?,updated_ms=? WHERE id=?', (state, at, request_id))
            return state
        # A successful POST response is not evidence of a fill or protection.
        self.db.execute("UPDATE simple_requests SET state='ACKNOWLEDGED',updated_ms=? WHERE id=?", (reply.received_ms, request_id))
        return 'ACKNOWLEDGED'

    def query_request(self, request_id, transport):
        r = self.request_record(request_id)
        if r['state'] in ('PENDING', 'NOT_SENT'): raise InvalidState('尚未发送的委托不应查询成成交')
        args = r['body']['arguments']
        if r['operation'] == 'CHANGE_LEVERAGE':
            reply = transport.request('QUERY_SYMBOL_CONFIG', dict(symbol=args['symbol']))
            rows = reply.body
            if (not isinstance(reply, WireReply) or reply.http_status != 200 or not isinstance(rows, list)
                    or len(rows) != 1 or rows[0].get('symbol') != args['symbol'] or type(rows[0].get('leverage')) is not int):
                raise InvalidState('实际杠杆查询没有对应合约')
            matched = rows[0]['leverage'] == args['leverage']
            self.db.execute('UPDATE simple_requests SET state=?,updated_ms=?,response=? WHERE id=?',
                ('TERMINAL' if matched else 'ACKNOWLEDGED', reply.received_ms,
                 _json(dict(leverage=rows[0]['leverage'], matched=matched)), request_id))
            return self.request_record(request_id)
        if r['operation'] in ('CANCEL_ORDER', 'CANCEL_PROTECTION'):
            target = self.query_request(r['body']['target_id'], transport)
            if target['state'] == 'TERMINAL':
                self.db.execute("UPDATE simple_requests SET state='TERMINAL',updated_ms=?,exchange_status=?,response=? WHERE id=?",
                                (transport.now_ms(), target['exchange_status'], _json(dict(target_id=target['id'],
                                 target_status=target['exchange_status'], cancelled=target['exchange_status']=='CANCELED')), request_id))
            return self.request_record(request_id)
        if r['operation'] == 'SEND_ORDER':
            operation = 'QUERY_ORDER'; params = dict(symbol=args['symbol'], origClientOrderId=args['newClientOrderId'])
        elif r['operation'] == 'ALGO_CHILD':
            operation = 'QUERY_ORDER'; params = dict(symbol=args['symbol'], orderId=r['exchange_id'])
        elif r['operation'] == 'SEND_PROTECTION':
            operation = 'QUERY_PROTECTION'; params = dict(clientAlgoId=args['clientAlgoId'])
        else:
            raise InvalidState('该类委托需要其对应查询接口')
        reply = transport.request(operation, params)
        observe = self.observe_protection if operation == 'QUERY_PROTECTION' else self.observe_order
        observe(request_id, reply.body, received_ms=reply.received_ms, query_ms=reply.sent_ms)
        return self.request_record(request_id)

    def retry_cancel(self, request_id, transport, *, enabled=False):
        """A failed DELETE may repeat only after a fresh query of its target.

        This path cannot retry POSTs. The target identity and, for a live
        protective order, the original replacement/exit reason stay fixed.
        """
        if enabled is not True or getattr(transport, 'mutations_enabled', False) is not True:
            raise InvalidState('实盘开关未开启')
        r = self.request_record(request_id)
        if r['operation'] not in ('CANCEL_ORDER', 'CANCEL_PROTECTION'):
            raise InvalidState('只有原订单的撤单可以重试')
        if r['state'] == 'PENDING':
            return self.send_once(request_id, transport, enabled=enabled)
        target = self.query_request(r['body']['target_id'], transport)
        now = transport.now_ms()
        if target['state'] == 'TERMINAL':
            self.db.execute("UPDATE simple_requests SET state='TERMINAL',updated_ms=?,exchange_status=?,response=? WHERE id=?",
                (now, target['exchange_status'], _json(dict(target_id=target['id'], target_status=target['exchange_status'],
                 cancelled=target['exchange_status'] == 'CANCELED')), request_id))
            return 'TERMINAL'
        # Re-check a replacement because it may have triggered while the
        # cancel was in flight. Do not remove the remaining live protection.
        if r['body'].get('replacement') is not None:
            replacement = self.request_record(r['body']['replacement'])
            if replacement['state'] != 'TERMINAL':
                self.query_request(replacement['id'], transport)
        self.freeze_cancel(target['id'], now_ms=transport.now_ms(), replacement=r['body'].get('replacement'),
                           exit_task=r['body'].get('exit_task'))
        self.db.execute("UPDATE simple_requests SET state='PENDING',updated_ms=? WHERE id=?",
                        (transport.now_ms(), request_id))
        return self.send_once(request_id, transport, enabled=enabled)

    def freeze_leverage(self, task_id, *, snapshot, owned_positions, owned_orders, now_ms):
        """Set the original strategy leverage without touching manual holdings."""
        from .account import PositionKey
        stamp(now_ms)
        t = self.task(task_id); p = self.plan(t['lot_id']); o = p['action']['order']
        attempt = entry_attempt(p, t['due_ms'])
        if t['kind'] != 'ENTRY_CHECK' or t['state'] != 'PENDING' or not t['due_ms'] <= now_ms < attempt['expires_ms']:
            raise InvalidState('调杠杆必须属于当前有效入场尝试')
        if not 0 <= now_ms-snapshot.received_ms <= 5000:
            raise InvalidState('调杠杆需要当前账户持仓')
        symbol = o['symbol']
        actual = {v.key:v.amount for v in snapshot.positions if v.key.symbol == symbol and v.amount}
        expected = {k:number(q) for k,q in owned_positions.items() if k.symbol == symbol and number(q)}
        if actual != expected or any(w.key.symbol == symbol and w.client_id not in owned_orders for w in snapshot.orders):
            raise InvalidState('该合约存在外部持仓或挂单，不能调整其杠杆')
        lev = o['lev'] if p['sleeve'] == 'M784' else o['leverage'] if p['sleeve'] == 'A787' else 3
        rid = identity([task_id,'leverage',lev])
        with self.store.transaction():
            if not self.db.execute('SELECT 1 FROM simple_requests WHERE id=?',(rid,)).fetchone():
                self._insert_request(request_id=rid,task_id=task_id,lot_id=p['lot_id'],purpose='LEVERAGE',
                    operation='CHANGE_LEVERAGE',body=dict(arguments=dict(symbol=symbol,leverage=lev)),
                    now_ms=now_ms,expires_ms=attempt['expires_ms'])
        return rid

    def publish_settled(self, lot_id, history, *, now_ms):
        """Close original M/S accounting from actual fills and funding only."""
        from .strategies.main import MainReceipt
        from .strategies.stock import StockReceipt
        stamp(now_ms)
        if history.exchange.broker is not self:
            raise InvalidState('结算流水不属于当前订单记录')
        p = self.plan(lot_id)
        if p['sleeve'] == 'A787':
            return False
        eid = identity([lot_id,'settled'])
        if self.db.execute('SELECT 1 FROM receipts WHERE event_id=?',(eid,)).fetchone():
            return False
        totals = self.lot_totals(lot_id)
        if not p['entry_closed'] or totals['remaining']:
            return False
        for (rid,) in self.db.execute("SELECT id FROM simple_requests WHERE lot_id=? AND operation IN ('SEND_ORDER','ALGO_CHILD','SEND_PROTECTION')", (lot_id,)):
            r = self.request_record(rid)
            if r['state'] not in ('TERMINAL','NOT_SENT'):
                return False
            if r['operation'] != 'SEND_PROTECTION' and self.filled_quantity(rid) != number(r['cumulative']):
                return False
            if r['child_id']:
                child = self.db.execute("SELECT id FROM simple_requests WHERE operation='ALGO_CHILD' AND json_extract(body,'$.parent_id')=?", (rid,)).fetchone()
                if child is None:
                    return False
        fills = self.db.execute("SELECT time_ms FROM fills WHERE json_extract(body,'$.lot_id')=? ORDER BY time_ms", (lot_id,)).fetchall()
        funding = D(0)
        if fills:
            begin, end = fills[0][0], fills[-1][0]
            if not history.covered('INCOME',start_ms=begin,end_ms=end):
                history.schedule('INCOME',start_ms=begin,end_ms=end)
                return False
            for (tid,) in self.db.execute('SELECT transaction_id FROM simple_raw_funding WHERE symbol=? AND event_ms>=? AND event_ms<=?',(p['action']['order']['symbol'],begin,end)):
                row = self.db.execute('SELECT allocation FROM simple_funding WHERE transaction_id=?',(tid,)).fetchone()
                if row is None:
                    return False
                funding += sum((number(a['amount']) for a in json.loads(row[0]) if a['lot_id']==lot_id),D(0))
        prior = self.db.execute('SELECT MAX(available_ms) FROM receipts WHERE lot_id=?',(lot_id,)).fetchone()[0] or 0
        available = receipt_available(p['sleeve'],'settled',now_ms,now_ms,prior)
        if p['sleeve'] == 'M784':
            receipt = MainReceipt(eid,p['strategy_order_id'],'settled',now_ms,now_ms,available,settled=True)
        else:
            receipt = StockReceipt(eid,p['strategy_order_id'],'settled',now_ms,now_ms,available,reconciled=True,
                parent_net=float(totals['realized_pnl']-totals['fees']+funding),entry_quantity=float(totals['entered']))
        return self.store.put_receipt(p['sleeve'],lot_id,receipt)

    def observe_order(self, request_id, report, *, received_ms, query_ms):
        stamp(received_ms); stamp(query_ms)
        with (nullcontext() if self.db.in_transaction else self.store.transaction()):
            r = self.request_record(request_id); a = r['body']['arguments']
            if r['operation'] not in ('SEND_ORDER', 'ALGO_CHILD') or r['state'] in ('PENDING', 'NOT_SENT') or not query_ms <= received_ms:
                raise InvalidState('订单回报没有对应的发送记录')
            required = {'symbol','clientOrderId','orderId','positionSide','side','origQty','executedQty','status','updateTime','type','price'}
            if not isinstance(report, dict) or not required <= report.keys(): raise InvalidState('需要完整交易所订单回报')
            if any(report[name] != expected for name, expected in dict(symbol=a['symbol'], clientOrderId=a['newClientOrderId'],
                    positionSide=a['positionSide'], side=a['side'], type=a['type']).items()):
                raise InvalidState('交易所订单身份与本地委托不匹配')
            if number(report['price']) != number(a.get('price', 0)) or a['type'] == 'LIMIT' and report.get('timeInForce') != a['timeInForce']:
                raise InvalidState('订单限价或有效方式发生变化')
            if a.get('reduceOnly') == 'true' and report.get('reduceOnly') is not True:
                raise InvalidState('平仓回报未确认只减仓')
            total = positive(report['origQty']); cumulative = nonnegative(report['executedQty'])
            at = stamp(report['updateTime']); status = report['status']; exchange_id = str(report['orderId'])
            if total != positive(a['quantity']) or cumulative > total or status not in TERMINAL | {'NEW','PARTIALLY_FILLED'}:
                raise InvalidState('交易所订单数量或状态错误')
            if not r['created_ms'] <= at <= received_ms or not exchange_id.isdigit() or int(exchange_id) < 1:
                raise InvalidState('交易所订单时点或编号错误')
            if (status == 'NEW' and cumulative != 0 or status == 'FILLED' and cumulative != total
                    or status == 'PARTIALLY_FILLED' and not 0 < cumulative < total):
                raise InvalidState('订单状态与累计成交量矛盾')
            if r['exchange_id'] is not None and r['exchange_id'] != exchange_id:
                raise InvalidState('同一委托对应多个交易所订单')
            if at < r['exchange_ms']: return False
            if cumulative < number(r['cumulative']) or cumulative < self.filled_quantity(request_id):
                raise InvalidState('累计成交量不能倒退')
            if r['exchange_status'] in TERMINAL and (status, cumulative) != (r['exchange_status'], number(r['cumulative'])):
                raise InvalidState('已结束订单不能重新开启')
            if at == r['exchange_ms'] and (status, cumulative) != (r['exchange_status'], number(r['cumulative'])) and query_ms <= r['updated_ms']:
                raise InvalidState('同毫秒状态变化需要后发查询')
            collision = self.db.execute("SELECT id FROM simple_requests WHERE id<>? AND operation IN ('SEND_ORDER','ALGO_CHILD') AND exchange_id=? AND json_extract(body,'$.arguments.symbol')=?", (request_id, exchange_id, a['symbol'])).fetchone()
            if collision: raise InvalidState('交易所订单已归属于另一委托')
            state = 'TERMINAL' if status in TERMINAL else 'ACKNOWLEDGED'
            self.db.execute('UPDATE simple_requests SET state=?,updated_ms=?,exchange_id=?,exchange_status=?,cumulative=?,exchange_ms=?,response=? WHERE id=?',
                            (state, received_ms, exchange_id, status, str(cumulative), at, _json({k: report[k] for k in required}), request_id))
        return True

    def filled_quantity(self, request_id):
        return sum((number(json.loads(r[0])['quantity']) for r in self.db.execute(
            "SELECT body FROM fills WHERE json_extract(body,'$.request_id')=?", (request_id,))), D(0))

    def record_trade(self, request_id, raw, *, received_ms, observed_ms=None):
        """Save real fills and their delayed strategy receipt in one commit."""
        stamp(received_ms)
        observed_ms=received_ms if observed_ms is None else stamp(observed_ms)
        if observed_ms>received_ms: raise InvalidState('成交观察时间晚于实际交付时间')
        with self.store.transaction():
            r = self.request_record(request_id); a = r['body']['arguments']; p = self.plan(r['lot_id'])
            if r['exchange_id'] is None or r['exchange_status'] is None or r['purpose'] not in ('ENTRY', 'EXIT'):
                raise InvalidState('成交尚未绑定到实际交易所订单')
            if any(raw.get(k) != v for k, v in dict(symbol=a['symbol'], side=a['side'], positionSide=a['positionSide']).items()) or str(raw.get('orderId')) != r['exchange_id']:
                raise InvalidState('成交与已确认订单不匹配')
            at = stamp(raw['time']); qty = positive(raw['qty'])
            if not r['created_ms'] <= at <= observed_ms:
                raise InvalidState('成交不能发生在本批委托前或接收后')
            if not str(raw['id']).isdigit(): raise InvalidState('成交编号格式错误')
            fill = dict(symbol=a['symbol'], trade_id=str(raw['id']), order_id=r['exchange_id'],
                        request_id=request_id, lot_id=r['lot_id'], purpose=r['purpose'],
                        time_ms=at, side=a['side'], position_side=a['positionSide'],
                        quantity=str(qty), price=str(positive(raw['price'])), fee=str(number(raw['commission'])),
                        fee_asset=raw['commissionAsset'], realized_pnl=str(number(raw['realizedPnl'])))
            old = self.db.execute('SELECT body FROM fills WHERE symbol=? AND trade_id=?', (fill['symbol'], fill['trade_id'])).fetchone()
            if old is None and self.filled_quantity(request_id) + qty > positive(a['quantity']):
                raise InvalidState('真实成交超过本批委托数量')
            if old is None and r['state'] == 'TERMINAL' and self.filled_quantity(request_id) + qty > number(r['cumulative']):
                raise InvalidState('成交超过交易所终态累计数量')
            if old is None and r['purpose'] == 'EXIT' and qty > self.lot_totals(r['lot_id'])['remaining']:
                raise InvalidState('平仓成交超过本策略批次实际持仓')
            prior = self.db.execute('SELECT MAX(available_ms) FROM receipts WHERE lot_id=?', (r['lot_id'],)).fetchone()[0] or 0
            receipt = fill_receipt(p, fill, purpose=r['purpose'], received_ms=received_ms, prior_available_ms=prior)
            fresh = self.store.put_fill(fill, sleeve=p['sleeve'], received_ms=observed_ms)
            if not fresh: return False
            self.store.put_receipt(p['sleeve'], p['lot_id'], receipt, fill_identity=(fill['symbol'], fill['trade_id']))
            if r['purpose'] == 'ENTRY':
                self.db.execute('UPDATE simple_plans SET first_entry_ms=CASE WHEN first_entry_ms IS NULL THEN ? ELSE MIN(first_entry_ms,?) END WHERE lot_id=?', (at, at, p['lot_id']))
        return True

    def lot_totals(self, lot_id):
        self.plan(lot_id)
        totals = dict(entered=D(0), exited=D(0), entry_notional=D(0), realized_pnl=D(0), fees=D(0))
        for (raw,) in self.db.execute("SELECT body FROM fills WHERE json_extract(body,'$.lot_id')=? ORDER BY time_ms,trade_id", (lot_id,)):
            f = json.loads(raw); q = number(f['quantity'])
            if f['purpose'] == 'ENTRY':
                totals['entered'] += q; totals['entry_notional'] += q * number(f['price'])
            else: totals['exited'] += q
            totals['realized_pnl'] += number(f['realized_pnl']); totals['fees'] += number(f['fee'])
        totals['remaining'] = totals['entered'] - totals['exited']
        return totals

    def freeze_protection(self, entry_id, proposal, *, now_ms, replaces=None, task_id=None):
        """Protect real holdings; replacing a stop keeps its old order alive."""
        stamp(now_ms)
        with self.store.transaction():
            entry = self.request_record(entry_id); p = self.plan(entry['lot_id'])
            if entry['purpose'] != 'ENTRY' or proposal['lot_id'] != entry['lot_id'] or self.filled_quantity(entry_id) <= 0:
                raise InvalidState('保护单没有对应的实际入场成交')
            a = proposal['arguments']; e = entry['body']['arguments']; role = proposal['role']
            if role not in ('SL', 'TP', 'HALF_SL') or (role == 'HALF_SL' and p['sleeve'] != 'S800'):
                raise InvalidState('保护单角色不属于原策略')
            if a['type'] != ('TAKE_PROFIT_MARKET' if role == 'TP' else 'STOP_MARKET') or a['clientAlgoId'] != proposal['id']:
                raise InvalidState('保护角色或身份与实际请求不符')
            if (a['symbol'], a['positionSide'], a['side']) != (e['symbol'], e['positionSide'], 'SELL' if e['side'] == 'BUY' else 'BUY'):
                raise InvalidState('保护单不能增加或改变持仓方向')
            held = self.lot_totals(entry['lot_id'])['remaining']
            if positive(a['quantity']) > held or proposal['created_ms'] > now_ms:
                raise InvalidState('保护数量超过实际持仓或使用了未来请求')
            if task_id is not None:
                task = self.task(task_id)
                if task['lot_id'] != entry['lot_id'] or task['due_ms'] > now_ms or task['kind'] not in ('STOP_AMEND','PROTECTION_AMEND','PROTECT_HALF'):
                    raise InvalidState('保护更新缺少已生效的原策略请求')
            if replaces is not None:
                old = self.request_record(replaces)
                if old['lot_id'] != entry['lot_id'] or old['purpose'] != role or old['operation'] != 'SEND_PROTECTION':
                    raise InvalidState('不能替换另一批次或不同用途的保护')
                direction = 1 if e['side'] == 'BUY' else -1
                if role != 'TP' and direction * (positive(a['triggerPrice']) - positive(old['body']['arguments']['triggerPrice'])) < 0:
                    raise InvalidState('更新不能放宽原策略止损')
            body = dict(proposal, replaces=replaces, entry_id=entry_id)
            self._insert_request(request_id=proposal['id'], task_id=task_id, lot_id=entry['lot_id'], purpose=role,
                                 operation='SEND_PROTECTION', body=body, now_ms=now_ms)
        return proposal['id']

    def observe_protection(self, request_id, report, *, received_ms, query_ms):
        stamp(received_ms); stamp(query_ms)
        with self.store.transaction():
            r = self.request_record(request_id); a = r['body']['arguments']
            required = {'algoId','clientAlgoId','algoType','orderType','symbol','side','positionSide','quantity','algoStatus',
                        'actualOrderId','triggerPrice','workingType','closePosition','priceProtect','reduceOnly','createTime','updateTime'}
            if r['operation'] != 'SEND_PROTECTION' or r['state'] in ('PENDING','NOT_SENT') or not query_ms <= received_ms:
                raise InvalidState('保护回报没有对应已发送请求')
            if not isinstance(report, dict) or not required <= report.keys(): raise InvalidState('保护单回报字段不完整')
            expected = {k: a[k] for k in ('clientAlgoId','symbol','positionSide','side','algoType','workingType')}
            expected['orderType'] = a['type']
            if any(report[k] != v for k, v in expected.items()) or number(report['quantity']) != positive(a['quantity']) or number(report['triggerPrice']) != positive(a['triggerPrice']):
                raise InvalidState('保护单身份、数量或触发价与委托不匹配')
            if report['closePosition'] is not False or report['priceProtect'] is not False or type(report['reduceOnly']) is not bool or (a['positionSide'] == 'BOTH' and report['reduceOnly'] is not True):
                raise InvalidState('保护单平仓范围发生变化')
            aid = str(report['algoId']); at = stamp(report['updateTime']); created = stamp(report['createTime']); status = report['algoStatus']
            if not aid.isdigit() or int(aid) < 1 or not r['created_ms'] <= created <= at <= received_ms or status not in ALGO_TERMINAL | {'NEW','TRIGGERING','TRIGGERED'}:
                raise InvalidState('保护单时间、状态或编号无效')
            child = None if report['actualOrderId'] in (None, '', 0, '0') else str(report['actualOrderId'])
            if child is not None and (not child.isdigit() or int(child) < 1 or report.get('actualType') != 'MARKET'):
                raise InvalidState('保护触发没有有效市价子订单')
            if r['exchange_id'] is not None and aid != r['exchange_id'] or r['child_id'] is not None and child != r['child_id']:
                raise InvalidState('保护单或子订单身份变化')
            if at < r['exchange_ms']: return False
            if r['exchange_status'] in ALGO_TERMINAL and status != r['exchange_status']:
                raise InvalidState('结束的保护单不能重新开启')
            if at == r['exchange_ms'] and (status, child) != (r['exchange_status'], r['child_id']) and query_ms <= r['updated_ms']:
                raise InvalidState('保护单同毫秒变化需要后发查询')
            collision = self.db.execute("SELECT id FROM simple_requests WHERE id<>? AND operation='SEND_PROTECTION' AND (exchange_id=? OR (? IS NOT NULL AND child_id=?)) AND json_extract(body,'$.arguments.symbol')=?",
                                        (request_id, aid, child, child, a['symbol'])).fetchone()
            if collision: raise InvalidState('保护订单或子订单已属于其他委托')
            state = 'TERMINAL' if status in ALGO_TERMINAL else 'ACKNOWLEDGED'
            self.db.execute('UPDATE simple_requests SET state=?,updated_ms=?,exchange_id=?,exchange_status=?,exchange_ms=?,response=?,child_id=? WHERE id=?',
                            (state, received_ms, aid, status, at, _json({k: report[k] for k in required}), child, request_id))
        return True

    def bind_protection_child(self, parent_id, report, *, received_ms, query_ms):
        """Only the queried parent's exact actualOrderId can become an exit."""
        stamp(received_ms); stamp(query_ms)
        with self.store.transaction():
            parent = self.request_record(parent_id); a = parent['body']['arguments']
            if not parent['child_id'] or str(report.get('orderId')) != parent['child_id']:
                raise InvalidState('子订单没有对应的保护触发记录')
            if report.get('type') != 'MARKET' or number(report.get('price')) != 0:
                raise InvalidState('触发子订单必须是市价平仓')
            rid = identity([parent_id, 'child', parent['child_id']])
            args = dict(symbol=a['symbol'], positionSide=a['positionSide'], side=a['side'], type='MARKET',
                        quantity=a['quantity'], newClientOrderId=report['clientOrderId'], newOrderRespType='ACK')
            if a.get('reduceOnly') == 'true': args['reduceOnly'] = 'true'
            validate_parameters('SEND_ORDER', args)
            body = dict(arguments=args, parent_id=parent_id)
            old = self.db.execute('SELECT body FROM simple_requests WHERE id=?', (rid,)).fetchone()
            if old is None:
                self.db.execute("INSERT INTO simple_requests(id,lot_id,purpose,operation,body,created_ms,state,updated_ms,exchange_id) VALUES(?,?,'EXIT','ALGO_CHILD',?,?,'ACKNOWLEDGED',?,?)",
                                (rid, parent['lot_id'], _json(body), parent['created_ms'], received_ms, parent['child_id']))
            elif json.loads(old[0]) != body: raise InvalidState('触发子订单内容变化')
            self.observe_order(rid, report, received_ms=received_ms, query_ms=query_ms)
        return rid

    def freeze_cancel(self, target_id, *, now_ms, replacement=None, exit_task=None):
        """Cancel owned protection for a confirmed replacement or explicit conflict."""
        stamp(now_ms)
        with self.store.transaction():
            target = self.request_record(target_id); p = self.plan(target['lot_id']); a = target['body']['arguments']
            if target['operation'] not in ('SEND_ORDER','SEND_PROTECTION') or target['exchange_id'] is None or target['state'] != 'ACKNOWLEDGED':
                raise InvalidState('撤单需要对应的实际活动订单')
            if target['operation'] == 'SEND_PROTECTION':
                if target['exchange_status'] != 'NEW': raise InvalidState('保护单已经触发，先接收对应平仓成交')
                held = self.lot_totals(target['lot_id'])['remaining']
                if exit_task is not None:
                    task = self.task(exit_task)
                    if task['kind'] != 'EXIT_REQUEST' or task['lot_id'] != target['lot_id'] or task['due_ms'] > now_ms or task['state'] == 'FINISHED' or replacement is not None:
                        raise InvalidState('撤保护单没有对应的原策略退出请求')
                elif held:
                    if replacement is None: raise InvalidState('持仓仍存在，需要新保护单确认生效')
                    new = self.request_record(replacement)
                    conflict = (new['state']=='TERMINAL' and new['exchange_status']=='REJECTED'
                                and (new['response'] or {}).get('exchange_code') == -2022)
                    if ((new['lot_id'],new['purpose'],new['operation']) != (target['lot_id'],target['purpose'],'SEND_PROTECTION')
                            or not conflict and (new['state'],new['exchange_status']) != ('ACKNOWLEDGED','NEW')):
                        raise InvalidState('替代保护单尚未确认生效')
                    if new['body'].get('replaces') != target_id or positive(new['body']['arguments']['quantity']) < min(held,positive(a['quantity'])):
                        raise InvalidState('替代保护单覆盖不足或没有对应原单')
                elif not p['entry_closed']:
                    raise InvalidState('入场仍可能成交，不能移除全部保护')
                operation='CANCEL_PROTECTION'; params=dict(clientAlgoId=a['clientAlgoId'])
            else:
                operation='CANCEL_ORDER'; params=dict(symbol=a['symbol'], origClientOrderId=a['newClientOrderId'])
            rid = identity([target_id, 'cancel'])
            existing = self.db.execute('SELECT id FROM simple_requests WHERE id=?', (rid,)).fetchone()
            if existing: return rid
            self._insert_request(request_id=rid, task_id=None, lot_id=target['lot_id'], purpose='CANCEL', operation=operation,
                                 body=dict(arguments=params, target_id=target_id, replacement=replacement, exit_task=exit_task), now_ms=now_ms)
        return rid

    def protection_exit(self, request_id, *, now_ms):
        """Latch the original trigger when the venue explicitly reports -2021.

        No inferred fill: this only creates an owned exit task. HALF_SL retains
        its original quantity cap, including across partial fills/restarts.
        """
        with self.store.transaction():
            r=self.request_record(request_id);p=self.plan(r['lot_id'])
            if (r['operation'],r['state'],r['exchange_status']) != ('SEND_PROTECTION','TERMINAL','REJECTED') or (r['response'] or {}).get('exchange_code') != -2021 or now_ms<r['updated_ms']:
                raise InvalidState('保护退出缺少交易所明确的触发拒绝')
            source=request_id+':triggered'
            for (tid,) in self.db.execute("SELECT id FROM simple_tasks WHERE lot_id=? AND kind='EXIT_REQUEST'",(r['lot_id'],)):
                if self.task(tid)['source']==source:return tid
            payload=dict(reason='original_trigger_crossed',protection_id=request_id,role=r['purpose'])
            if r['purpose']=='HALF_SL':
                totals=self.lot_totals(r['lot_id'])
                payload['exit_cap']=str(min(totals['remaining'],positive(r['body']['arguments']['quantity'])))
                payload['exited_before']=str(totals['exited'])
            return self._schedule(p,'EXIT_REQUEST',now_ms,source,payload)

    def _exit_remaining(self, task):
        totals=self.lot_totals(task['lot_id']);remaining=totals['remaining']
        if 'exit_cap' in task['payload']:
            cap=nonnegative(task['payload']['exit_cap'])
            done=max(D(0),totals['exited']-nonnegative(task['payload']['exited_before']))
            remaining=min(remaining,max(D(0),cap-done))
        return remaining

    def apply_entry_refresh(self, task_id, *, now_ms):
        stamp(now_ms)
        with self.store.transaction():
            t = self.task(task_id); p = self.plan(t['lot_id'])
            if t['kind'] != 'ENTRY_AMEND' or t['state'] != 'PENDING' or now_ms < t['due_ms']:
                raise InvalidState('限价更新尚未生效或已经处理')
            unknown = self.db.execute("SELECT 1 FROM simple_requests WHERE lot_id=? AND purpose='ENTRY' AND state NOT IN ('TERMINAL','NOT_SENT')", (p['lot_id'],)).fetchone()
            if unknown: return False
            applied = bool(t['payload']['eligible']) and not p['entry_closed'] and not p['cancel_requested']
            if applied:
                raw = self.db.execute('SELECT body FROM simple_plans WHERE lot_id=?', (p['lot_id'],)).fetchone()[0]
                body = json.loads(raw); body['current_limit'] = str(positive(t['payload']['new_limit']))
                self.db.execute('UPDATE simple_plans SET body=? WHERE lot_id=?', (_json(body), p['lot_id']))
            self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?", (_json({'applied':applied}), task_id))
        return applied

    def stop_entry_attempts(self, task_id, *, now_ms):
        """An expiry/exit stops new attempts; it does not fabricate cancel ACKs."""
        stamp(now_ms)
        with self.store.transaction():
            t = self.task(task_id); p = self.plan(t['lot_id'])
            if t['kind'] not in ('CANCEL_ENTRY','EXIT_REQUEST') or now_ms < t['due_ms']:
                raise InvalidState('没有到期的撤单或平仓请求')
            self.db.execute('UPDATE simple_plans SET cancel_requested=1 WHERE lot_id=?', (p['lot_id'],))
            self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE lot_id=? AND kind='ENTRY_CHECK' AND state='PENDING'", (_json({'result':'CANCEL_REQUESTED'}),p['lot_id']))
            self.db.execute("UPDATE simple_requests SET state='NOT_SENT',updated_ms=? WHERE lot_id=? AND purpose='ENTRY' AND state='PENDING'", (now_ms,p['lot_id']))
            entries = [self.request_record(r[0]) for r in self.db.execute("SELECT id FROM simple_requests WHERE lot_id=? AND purpose='ENTRY'", (p['lot_id'],))]
            unresolved = [r['id'] for r in entries if r['state'] not in ('TERMINAL','NOT_SENT') or self.filled_quantity(r['id']) != number(r['cumulative'])]
            if not unresolved:
                self._close_entry(p, now_ms)
                if t['kind'] == 'CANCEL_ENTRY':
                    self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?", (_json({'result':'ENTRY_CLOSED'}), task_id))
        return unresolved

    def pause_entries(self, *, now_ms):
        """User pause cancels pending entries; original exits remain active."""
        stamp(now_ms)
        with self.store.transaction():
            plans=[self.plan(r[0]) for r in self.db.execute('SELECT lot_id FROM simple_plans WHERE entry_closed=0 AND cancel_requested=0')]
            for p in plans:
                self.db.execute('UPDATE simple_plans SET cancel_requested=1 WHERE lot_id=?',(p['lot_id'],))
                self.db.execute("UPDATE simple_requests SET state='NOT_SENT',updated_ms=? WHERE lot_id=? AND purpose IN ('ENTRY','LEVERAGE') AND state='PENDING'",(now_ms,p['lot_id']))
                self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE lot_id=? AND kind='ENTRY_CHECK' AND state='PENDING'",(_json(dict(result='USER_PAUSED')),p['lot_id']))
                self._schedule(p,'CANCEL_ENTRY',now_ms,p['id']+':user_pause',dict(reason='user_pause'))
        return len(plans)

    def freeze_exit(self, task_id, *, snapshot, rules, owned_positions, now_ms):
        """Exit the actually held quantity after entry/old protection finality."""
        from .account import PositionKey
        stamp(now_ms)
        with self.store.transaction():
            t = self.task(task_id); p = self.plan(t['lot_id']); o = p['action']['order']
            if t['kind'] != 'EXIT_REQUEST' or t['state'] == 'FINISHED' or now_ms < t['due_ms']:
                raise InvalidState('原策略退出请求尚未生效或已经完成')
            if not p['entry_closed']: raise InvalidState('仍有入场可能成交，先完成对应撤单')
            for (rid,) in self.db.execute("SELECT id FROM simple_requests WHERE lot_id=? AND operation='SEND_PROTECTION'", (p['lot_id'],)):
                r = self.request_record(rid)
                if r['state'] not in ('TERMINAL','NOT_SENT'):
                    raise InvalidState('旧保护单仍可能触发，先处理原批次保护')
                if r['child_id']:
                    child_id = identity([rid,'child',r['child_id']])
                    child = self.request_record(child_id)
                    if child['state'] != 'TERMINAL' or self.filled_quantity(child_id) != number(child['cumulative']):
                        raise InvalidState('已经触发的保护平仓尚未收齐')
            past = [self.request_record(r[0]) for r in self.db.execute("SELECT id FROM simple_requests WHERE lot_id=? AND purpose='EXIT' AND operation='SEND_ORDER'", (p['lot_id'],))]
            for r in past:
                if r['state'] not in ('TERMINAL','NOT_SENT') or self.filled_quantity(r['id']) != number(r['cumulative']):
                    raise InvalidState('上一次平仓结果未定，不重复减仓')
            held = self._exit_remaining(t)
            if held == 0:
                self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?", (_json({'result':'NO_REMAINING_POSITION'}), task_id))
                return None
            key = PositionKey(o['symbol'], ('LONG' if o['side'] == 1 else 'SHORT') if snapshot.hedge_mode else 'BOTH')
            actual = next((v.amount for v in snapshot.positions if v.key == key), D(0))
            if actual != number(owned_positions.get(key, 0)) or held > abs(actual) or o['side'] * actual <= 0:
                raise InvalidState('账户持仓与本策略实际成交不符，不能接管外部持仓')
            if not 0 <= now_ms - snapshot.received_ms <= 5000 or rules.key != key or not 0 <= now_ms - rules.observed_ms <= 3600000:
                raise InvalidState('需要当前账户数量与对应交易规则')
            qty = (min(held,rules.maximum_quantity) / rules.step).to_integral_value(rounding=ROUND_FLOOR) * rules.step
            if qty <= 0: raise InvalidState('实际剩余数量小于交易所步长')
            rid = identity([task_id,'exit',len(past)])
            args = dict(symbol=key.symbol, positionSide=key.side, side='SELL' if o['side'] == 1 else 'BUY',
                        type='MARKET', quantity=str(qty), newClientOrderId=rid, newOrderRespType='ACK')
            if key.side == 'BOTH': args['reduceOnly'] = 'true'
            self._insert_request(request_id=rid, task_id=task_id, lot_id=p['lot_id'], purpose='EXIT', operation='SEND_ORDER',
                                 body=dict(arguments=args), now_ms=now_ms, expires_ms=now_ms+5000)
            self.db.execute("UPDATE simple_tasks SET state='WAITING' WHERE id=?", (task_id,))
        return rid

    def resolve_exit(self, request_id, *, now_ms):
        stamp(now_ms)
        with self.store.transaction():
            r = self.request_record(request_id)
            if r['purpose'] != 'EXIT' or r['operation'] != 'SEND_ORDER': raise InvalidState('该委托不是策略主动平仓')
            if r['state'] not in ('TERMINAL','NOT_SENT') or self.filled_quantity(request_id) != number(r['cumulative']) or r['updated_ms'] > now_ms:
                return False
            task=self.task(r['task_id'])
            if task['state']=='FINISHED':return False
            remaining = self._exit_remaining(task)
            # Do not release a task waiting on a later partial-exit request.
            if any(x[0]!=request_id for x in self.db.execute("SELECT id FROM simple_requests WHERE task_id=? AND state NOT IN ('TERMINAL','NOT_SENT')",(r['task_id'],))):return False
            state = 'FINISHED' if remaining == 0 else 'PENDING'
            self.db.execute('UPDATE simple_tasks SET state=?,result=? WHERE id=?',
                            (state,_json({'remaining':str(remaining)}),r['task_id']))
        return True

    def publish_alt_protection(self, lot_id, stop_id, target_id, *, now_ms, task_id=None):
        from .strategies.alt import AltReceipt
        stamp(now_ms)
        with self.store.transaction():
            p = self.plan(lot_id); o = p['action']['order']; stop = self.request_record(stop_id); target = self.request_record(target_id)
            if p['sleeve'] != 'A787': raise InvalidState('该保护确认仅属于原山寨策略')
            for r, role in ((stop,'SL'),(target,'TP')):
                if (r['lot_id'],r['purpose'],r['operation'],r['state'],r['exchange_status']) != (lot_id,role,'SEND_PROTECTION','ACKNOWLEDGED','NEW') or r['updated_ms'] > now_ms:
                    raise InvalidState('止盈止损尚未确认在交易所生效')
            source = None if task_id is None else self.task(task_id)
            if source is not None and (source['lot_id'] != lot_id or source['kind'] != 'PROTECTION_AMEND' or source['due_ms'] > now_ms):
                raise InvalidState('保护确认没有对应已生效的策略更新')
            requested = dict(stop=o['stop'],target=o['tp2'],request_id='initial:'+str(o['id'])) if source is None else source['payload']
            sl=positive(stop['body']['arguments']['triggerPrice']); tp=positive(target['body']['arguments']['triggerPrice'])
            quantity=min(positive(stop['body']['arguments']['quantity']),positive(target['body']['arguments']['quantity']))
            if sl < number(requested['stop']) or tp > number(requested['target']) or sl >= tp:
                raise InvalidState('保护确认价超出原策略请求')
            eid=identity([lot_id,stop_id,target_id,'confirmed'])
            if self.db.execute('SELECT 1 FROM receipts WHERE event_id=?',(eid,)).fetchone(): return False
            prior=self.db.execute('SELECT MAX(available_ms) FROM receipts WHERE lot_id=?',(lot_id,)).fetchone()[0] or 0
            event=max(stop['exchange_ms'],target['exchange_ms']); kind='initial_protection' if source is None else 'protection'
            receipt=AltReceipt(eid,o['id'],kind,event,now_ms,receipt_available('A787',kind,event,now_ms,prior),
                               request_id=requested['request_id'],stop=float(sl),target=float(tp),requested_stop=requested['stop'],
                               requested_target=requested['target'],covered_quantity=float(quantity))
            self.store.put_receipt('A787',lot_id,receipt)
            if task_id is not None:
                self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?",(_json({'result':'PROTECTION_CONFIRMED'}),task_id))
        return True

    def _close_entry(self, plan, now_ms):
        from .strategies.main import MainReceipt
        from .strategies.alt import AltReceipt
        from .strategies.stock import StockReceipt
        if plan['entry_closed']: return
        if self.db.execute("SELECT 1 FROM simple_requests WHERE lot_id=? AND purpose='ENTRY' AND state NOT IN ('TERMINAL','NOT_SENT')", (plan['lot_id'],)).fetchone():
            raise InvalidState('入场订单仍可能成交，不能宣布完成')
        for (rid,) in self.db.execute("SELECT id FROM simple_requests WHERE lot_id=? AND purpose='ENTRY' AND state='TERMINAL'", (plan['lot_id'],)):
            if self.filled_quantity(rid) != number(self.request_record(rid)['cumulative']):
                raise InvalidState('入场累计成交尚未收齐')
        prior = self.db.execute('SELECT MAX(available_ms) FROM receipts WHERE lot_id=?', (plan['lot_id'],)).fetchone()[0] or 0
        cls = {'M784': MainReceipt, 'A787': AltReceipt, 'S800': StockReceipt}[plan['sleeve']]
        receipt = cls(event_id=identity([plan['id'], 'entry_done']), order_id=plan['strategy_order_id'],
                      kind='entry_done', event_ms=now_ms, received_ms=now_ms,
                      available_ms=receipt_available(plan['sleeve'], 'entry_done', now_ms, now_ms, prior))
        self.store.put_receipt(plan['sleeve'], plan['lot_id'], receipt)
        self.db.execute('UPDATE simple_plans SET entry_closed=1 WHERE lot_id=?', (plan['lot_id'],))
        self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE lot_id=? AND kind='ENTRY_CHECK' AND state='PENDING'",
                        (_json({'result': 'ENTRY_CLOSED'}), plan['lot_id']))

    def resolve_entry(self, request_id, *, now_ms):
        stamp(now_ms)
        with self.store.transaction():
            r = self.request_record(request_id); p = self.plan(r['lot_id']); t = self.task(r['task_id'])
            if t['state'] == 'FINISHED': return False
            if r['purpose'] != 'ENTRY' or t['state'] != 'WAITING' or r['updated_ms'] > now_ms:
                raise InvalidState('没有对应的入场等待结果')
            filled = self.filled_quantity(request_id)
            if r['state'] not in ('TERMINAL', 'NOT_SENT') or filled != number(r['cumulative']):
                return False
            next_due = t['due_ms'] + FIVE
            if (p['sleeve'] == 'M784' and not filled and r['exchange_status'] in ('CANCELED', 'EXPIRED')
                    and not p['entry_closed'] and not p['cancel_requested'] and next_due < p['expiry_ms']):
                self._schedule(p, 'ENTRY_CHECK', next_due, p['id'])
            else:
                self._close_entry(p, now_ms)
            self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?",
                            (_json({'result': 'ENTRY_TERMINAL', 'quantity': str(filled)}), t['id']))
        return True

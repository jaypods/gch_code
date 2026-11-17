"""User-operated execution of durable original-strategy intentions.

The website is not an input. Each step performs bounded work and returns to
the owner loop. The switch is off by default; tests use synthetic venues only.
"""
import json
from decimal import Decimal as D
from types import SimpleNamespace

from .account import InvalidState, PositionKey, number, positive
from .simple_orders import entry_attempt, prepare_entry, protection_request, tick_round
from .simple_store import _json


class SimpleExecutor:
    def __init__(self, exchange, cash, history):
        self.exchange, self.cash, self.history = exchange, cash, history
        self.broker, self.store = exchange.broker, exchange.store
        if cash.broker is not self.broker or history.exchange is not exchange:
            raise InvalidState('执行和成交记录必须属于同一服务')
        self.db = self.store.db
        self._query_after = {}
        self._lot_cursor = 0
        self._task_attempt = {}
        self._task_sequence = 0
        self._funding_cursor = 0

    def _lot_requests(self, lot_id):
        return [self.broker.request_record(r[0]) for r in self.db.execute(
            'SELECT id FROM simple_requests WHERE lot_id=? ORDER BY created_ms,rowid',(lot_id,))]

    def _resolve(self, r):
        now = self.exchange.transport.now_ms()
        if r['purpose']=='ENTRY':
            self.broker.resolve_entry(r['id'],now_ms=now)
        elif r['purpose']=='EXIT' and r['operation']=='SEND_ORDER':
            self.broker.resolve_exit(r['id'],now_ms=now)

    def sync_one(self, *, enabled=False):
        """One send/query. Never resend an uncertain entry or protection POST."""
        now = self.exchange.transport.now_ms()
        rows = self.broker.requests(states=('PENDING','SENDING','UNKNOWN','ACKNOWLEDGED','TERMINAL','NOT_SENT'))
        priority = {'SL':0,'HALF_SL':1,'TP':2,'CANCEL':3,'EXIT':4,'LEVERAGE':5,'ENTRY':6}
        rows.sort(key=lambda r:(r['state']!='PENDING',priority.get(r['purpose'],9),r['updated_ms'],r['id']))
        for r in rows:
            if r['state'] in ('TERMINAL','NOT_SENT'):
                self._resolve(r)
                if r['operation'] in ('SEND_ORDER','ALGO_CHILD') and self.broker.filled_quantity(r['id']) < number(r['cumulative']):
                    args=r['body']['arguments']
                    self.history.schedule('TRADES',symbol=args['symbol'],start_ms=r['created_ms'],end_ms=now-1)
                continue
            if self._query_after.get(r['id'],0)>now:
                continue
            if r['state']=='PENDING':
                if not enabled:
                    continue
                # The remaining position may change after this proposal was
                # saved. send_once checks quantity again before transmission.
                self._query_after[r['id']]=now+1000
                return self.broker.send_once(r['id'],self.exchange.transport,enabled=enabled)
            self._query_after[r['id']]=now+2000
            if r['operation'] in ('CANCEL_ORDER','CANCEL_PROTECTION') and enabled:
                return self.broker.retry_cancel(r['id'],self.exchange.transport,enabled=enabled)
            observed=self.exchange.order(r['id'])
            self._resolve(observed)
            if observed['operation'] in ('SEND_ORDER','ALGO_CHILD') and self.broker.filled_quantity(r['id']) < number(observed['cumulative']):
                self.history.schedule('TRADES',symbol=observed['body']['arguments']['symbol'],
                                      start_ms=observed['created_ms'],end_ms=self.exchange.transport.now_ms()-1)
            return observed['state']
        return None

    def _alt_positions(self):
        lots,_ = self.cash.inventory()
        result=[]
        for (sleeve,lot,key),q in lots.items():
            if sleeve!='A787':
                continue
            p=self.broker.plan(lot);o=p['action']['order'];totals=self.broker.lot_totals(lot)
            stops=[r for r in self._lot_requests(lot) if r['purpose']=='SL' and r['exchange_status']=='NEW'
                   and r['state']=='ACKNOWLEDGED' and number(r['body']['arguments']['quantity'])>=abs(q)]
            if not stops:
                raise InvalidState('原山寨策略需要已有持仓的实际止损价')
            result.append(dict(id=o['id'],symbol=key.symbol,quantity=float(abs(q)),
                entry_fill=float(totals['entry_notional']/totals['entered']),leverage=o['leverage'],
                stop=float(max(number(r['body']['arguments']['triggerPrice']) for r in stops)),
                leadership_event_hour=o['leadership_event_hour']))
        return result

    def entry(self, task):
        now=self.exchange.transport.now_ms();p=self.broker.plan(task['lot_id']);o=p['action']['order']
        attempt=entry_attempt(p,task['due_ms'])
        if now>=attempt['expires_ms']:
            self.broker.finish_local_entry(task['id'],dict(result='MISSED_WINDOW'),now_ms=now)
            return 'MISSED_WINDOW'
        snapshot=self.exchange.account().snapshot
        view=self.cash.view(snapshot,now_ms=self.exchange.transport.now_ms())
        key=PositionKey(o['symbol'],('LONG' if o['side']==1 else 'SHORT') if snapshot.hedge_mode else 'BOTH')
        rules,config=self.exchange.execution_rules(key,entry=True)
        lev=o['lev'] if p['sleeve']=='M784' else o['leverage'] if p['sleeve']=='A787' else 3
        if config['leverage']!=lev:
            rid=self.broker.freeze_leverage(task['id'],snapshot=snapshot,owned_positions=view['owned_positions'],
                owned_orders=view['owned_orders'],now_ms=self.exchange.transport.now_ms())
            return dict(leverage_request=rid)
        quote=self.exchange.quote(o['symbol']);now=self.exchange.transport.now_ms()
        prepared=prepare_entry(p,attempt,snapshot=snapshot,funds=view['funds'][p['sleeve']],rules=rules,
            quote=quote,installed_leverage=config['leverage'],owned_positions=view['owned_positions'],
            owned_orders=view['owned_orders'],pending_margin=view['pending_margin'],now_ms=now,
            alt_positions=self._alt_positions() if p['sleeve']=='A787' else ())
        if prepared['result']=='PREPARED':
            return dict(entry_request=self.broker.freeze_entry(task['id'],prepared,now_ms=now))
        self.broker.finish_local_entry(task['id'],prepared,now_ms=now)
        return prepared

    def cancel_entries(self, task):
        now=self.exchange.transport.now_ms()
        unresolved=self.broker.stop_entry_attempts(task['id'],now_ms=now)
        for rid in unresolved:
            r=self.broker.request_record(rid)
            if r['state']=='ACKNOWLEDGED' and r['exchange_id'] is not None:
                self.broker.freeze_cancel(rid,now_ms=now)
        return not unresolved

    def exit(self, task):
        if not self.cancel_entries(task):
            return 'WAIT_ENTRY_RESPONSE'
        now=self.exchange.transport.now_ms()
        for r in self._lot_requests(task['lot_id']):
            if r['operation']!='SEND_PROTECTION':
                continue
            if r['state']=='PENDING':
                self.db.execute("UPDATE simple_requests SET state='NOT_SENT',updated_ms=? WHERE id=?",(now,r['id']))
            elif r['state']=='ACKNOWLEDGED' and r['exchange_status']=='NEW':
                self.broker.freeze_cancel(r['id'],now_ms=now,exit_task=task['id'])
                return 'CANCEL_PROTECTION'
            elif r['state'] not in ('TERMINAL','NOT_SENT'):
                return 'WAIT_PROTECTION_RESPONSE'
        snapshot=self.exchange.account().snapshot
        view=self.cash.view(snapshot,now_ms=self.exchange.transport.now_ms())
        o=self.broker.plan(task['lot_id'])['action']['order']
        key=PositionKey(o['symbol'],('LONG' if o['side']==1 else 'SHORT') if snapshot.hedge_mode else 'BOTH')
        rules,_=self.exchange.execution_rules(key)
        return self.broker.freeze_exit(task['id'],snapshot=snapshot,rules=rules,
            owned_positions=view['owned_positions'],now_ms=self.exchange.transport.now_ms())

    def protect(self, lot_id):
        """Keep the original bracket and its due strategy amendments current."""
        now=self.exchange.transport.now_ms();p=self.broker.plan(lot_id);rows=self._lot_requests(lot_id)
        held=self.broker.lot_totals(lot_id)['remaining']
        if not held:
            if p['entry_closed']:
                for r in rows:
                    if r['operation']=='SEND_PROTECTION' and r['state']=='ACKNOWLEDGED' and r['exchange_status']=='NEW':
                        return self.broker.freeze_cancel(r['id'],now_ms=now)
                    if r['operation']=='SEND_PROTECTION' and r['state']=='PENDING':
                        self.db.execute("UPDATE simple_requests SET state='NOT_SENT',updated_ms=? WHERE id=?",(now,r['id']))
            return None
        if self.db.execute("SELECT 1 FROM simple_tasks WHERE lot_id=? AND kind='EXIT_REQUEST' AND due_ms<=? AND state<>'FINISHED'",(lot_id,now)).fetchone():
            return None
        for r in rows:
            if r['child_id']:
                children=[x for x in rows if x['operation']=='ALGO_CHILD' and x['body']['parent_id']==r['id']]
                if not children or any(x['state']!='TERMINAL' or self.broker.filled_quantity(x['id'])!=number(x['cumulative']) for x in children):
                    return None
        entries=[r for r in rows if r['purpose']=='ENTRY' and self.broker.filled_quantity(r['id'])>0]
        if len(entries)!=1:
            raise InvalidState('原批次需要唯一实际成交的入场请求')
        entry=entries[0];body=entry['body'];args=body['arguments']
        tasks=[self.broker.task(r[0]) for r in self.db.execute("SELECT id FROM simple_tasks WHERE lot_id=? AND kind IN ('STOP_AMEND','PROTECTION_AMEND','PROTECT_HALF') AND due_ms<=? ORDER BY due_ms,rowid",(lot_id,now))]
        desired={'SL':(number(body['stop']),held,None),'TP':(number(body['target']),held,None)}
        for t in tasks:
            a=t['payload']
            if t['kind']=='STOP_AMEND':desired['SL']=(number(a['new_stop']),held,t)
            elif t['kind']=='PROTECTION_AMEND':
                desired['SL']=(number(a['stop']),held,t);desired['TP']=(number(a['target']),held,t)
            else:
                half_exit=self.db.execute("SELECT 1 FROM simple_tasks WHERE lot_id=? AND kind='EXIT_REQUEST' AND json_extract(body,'$.payload.role')='HALF_SL'",(lot_id,)).fetchone()
                if not half_exit and not any(r['purpose']=='HALF_SL' and r['child_id'] for r in rows):
                    desired['HALF_SL']=(number(a['stop']),min(held,number(a['quantity'])),t)
        rules,_=self.exchange.execution_rules(PositionKey(args['symbol'],args['positionSide']))
        confirmed={}
        for role,(trigger,quantity,task) in desired.items():
            same=[r for r in rows if r['operation']=='SEND_PROTECTION' and r['purpose']==role]
            active=[r for r in same if r['state'] not in ('TERMINAL','NOT_SENT')]
            unknown=[r for r in active if r['exchange_status']!='NEW' or r['state']!='ACKNOWLEDGED']
            if unknown:
                return None
            price=tick_round(trigger,rules,(body['side']==1)==(role!='TP'))
            quantity=(quantity//rules.step)*rules.step
            if not quantity:
                raise InvalidState('原策略保护数量小于交易所最小步长')
            correct=[r for r in active if number(r['body']['arguments']['triggerPrice'])==price and number(r['body']['arguments']['quantity'])==quantity]
            if correct:
                new=correct[-1];confirmed[role]=new
                for old in active:
                    if old['id']!=new['id']:
                        return self.broker.freeze_cancel(old['id'],now_ms=self.exchange.transport.now_ms(),replacement=new['id'])
                continue
            old=active[-1] if active else None
            failed=[r for r in same if r['state']=='TERMINAL' and r['exchange_status']=='REJECTED'
                and number(r['body']['arguments']['triggerPrice'])==price and number(r['body']['arguments']['quantity'])==quantity]
            if failed:
                last=failed[-1];code=(last['response'] or {}).get('exchange_code')
                if code == -2021:
                    return self.broker.protection_exit(last['id'],now_ms=self.exchange.transport.now_ms())
                if code == -2022 and last['body'].get('replaces'):
                    previous=self.broker.request_record(last['body']['replaces'])
                    if previous['state']=='ACKNOWLEDGED' and previous['exchange_status']=='NEW':
                        return self.broker.freeze_cancel(previous['id'],now_ms=self.exchange.transport.now_ms(),replacement=last['id'])
                    if previous['state'] not in ('TERMINAL','NOT_SENT'):return None
                else:
                    raise InvalidState('原策略保护被交易所拒绝（'+str(code)+'），未重复提交')
            proposal=protection_request(body,role=role,quantity=quantity,trigger=trigger,rules=rules,
                generation=len(same),now_ms=self.exchange.transport.now_ms(),
                previous_stop=old['body']['arguments']['triggerPrice'] if old and role!='TP' else None)
            snapshot=self.exchange.account().snapshot
            view=self.cash.view(snapshot,now_ms=self.exchange.transport.now_ms())
            key=PositionKey(args['symbol'],args['positionSide'])
            actual=next((x.amount for x in snapshot.positions if x.key==key),D(0))
            if actual!=view['owned_positions'].get(key,D(0)) or body['side']*actual<held:
                raise InvalidState('该合约实际持仓包含外部变化，不能扩大保护范围')
            return self.broker.freeze_protection(entry['id'],proposal,now_ms=self.exchange.transport.now_ms(),
                replaces=old['id'] if old else None,task_id=task['id'] if task else None)
        for task in tasks:
            if task['state']=='FINISHED':
                continue
            role='HALF_SL' if task['kind']=='PROTECT_HALF' else 'SL'
            if task['kind']=='PROTECTION_AMEND':
                self.broker.publish_alt_protection(lot_id,confirmed['SL']['id'],confirmed['TP']['id'],
                    now_ms=self.exchange.transport.now_ms(),task_id=task['id'])
            elif role in confirmed:
                self.db.execute("UPDATE simple_tasks SET state='FINISHED',result=? WHERE id=?",
                    (_json(dict(result='PROTECTION_CONFIRMED',request_id=confirmed[role]['id'])),task['id']))
        if p['sleeve']=='A787' and not any(t['kind']=='PROTECTION_AMEND' for t in tasks):
            self.broker.publish_alt_protection(lot_id,confirmed['SL']['id'],confirmed['TP']['id'],now_ms=self.exchange.transport.now_ms())
        return None

    def task_one(self, *, entries_allowed=True):
        now=self.exchange.transport.now_ms()
        tasks=[t for t in self.broker.due(now_ms=now) if t['kind'] in ('CANCEL_ENTRY','ENTRY_AMEND','EXIT_REQUEST') or t['kind']=='ENTRY_CHECK' and entries_allowed]
        tasks.sort(key=lambda t:self._task_attempt.get(t['id'],0))
        for task in tasks:
            self._task_sequence+=1;self._task_attempt[task['id']]=self._task_sequence
            if task['kind']=='CANCEL_ENTRY':return self.cancel_entries(task)
            if task['kind']=='ENTRY_AMEND':return self.broker.apply_entry_refresh(task['id'],now_ms=now)
            if task['kind']=='EXIT_REQUEST':return self.exit(task)
            if task['kind']=='ENTRY_CHECK' and entries_allowed:return self.entry(task)
        return None

    def funding_one(self):
        """Attribute one actual funding receipt using contemporaneous quantity."""
        rows=self.db.execute('''SELECT f.transaction_id,f.body FROM simple_raw_funding f
            LEFT JOIN simple_funding a ON a.transaction_id=f.transaction_id
            WHERE a.transaction_id IS NULL ORDER BY f.event_ms''').fetchall()
        if rows:
            offset=self._funding_cursor%len(rows);rows=rows[offset:]+rows[:offset];self._funding_cursor+=1
        for tid,body in rows:
            raw=json.loads(body);symbol=raw['symbol'];at=raw['time']
            lots,_=self.cash.inventory(through_ms=at)
            if not any(key.symbol==symbol for sleeve,lot,key in lots):
                continue
            sample_name='funding_sample:'+tid
            sample=self.store.value(sample_name)
            if sample is not None:
                overlap=self.db.execute('SELECT 1 FROM simple_raw_trades WHERE symbol=? AND event_ms>=? AND event_ms<=? LIMIT 1',
                    (symbol,sample['exchange_ms'],sample['received_ms'])).fetchone()
                if at>sample['exchange_ms'] or overlap:sample=None
            if sample is None:
                snapshot=self.exchange.account().snapshot
                sample=dict(exchange_ms=snapshot.exchange_ms,received_ms=snapshot.received_ms,
                    positions=[dict(symbol=p.key.symbol,side=p.key.side,amount=str(p.amount))
                               for p in snapshot.positions if p.key.symbol==symbol])
                self.store.set_value(sample_name,sample)
            # Freeze this observation across subsequent fetches/restarts.
            # Refreshing it each pass would move the target forever.
            snapshot=SimpleNamespace(exchange_ms=sample['exchange_ms'],received_ms=sample['received_ms'],
                positions=[SimpleNamespace(key=PositionKey(p['symbol'],p['side']),amount=number(p['amount'])) for p in sample['positions']])
            now=self.exchange.transport.now_ms()
            if snapshot.received_ms>=now or at>snapshot.exchange_ms:
                return 'WAIT_ACCOUNT_SAMPLE'
            if not self.history.covered('TRADES',symbol=symbol,start_ms=at,end_ms=snapshot.received_ms):
                self.history.schedule('TRADES',symbol=symbol,start_ms=at,end_ms=snapshot.received_ms)
                return 'READ_EVENT_TRADES'
            position=self.history.position_at(snapshot,symbol=symbol,event_ms=at)
            return self.cash.record_funding(raw,received_ms=now,owned_position_at_event=position)
        return None

    def step(self, *, enabled=False, entries_allowed=True):
        """One bounded execution pass; all phases are independent of charts.

        Failure reading one fill must not prevent servicing a different held
        lot's original stop. Errors are retained as phase names/types only,
        never HTTP exception text or credentials. No independent supervisor,
        audit proof, release state or completed backtest is consulted.
        """
        results={};errors=[]
        def run(name, fn):
            try:
                value=fn();results[name]=value
                return value
            except Exception as exc:
                errors.append(dict(phase=name,error=type(exc).__name__))
                return None
        run('actions',lambda:self.broker.accept_actions(now_ms=self.exchange.transport.now_ms()))
        if enabled and not entries_allowed:
            run('pause_entries',lambda:self.broker.pause_entries(now_ms=self.exchange.transport.now_ms()))
        run('orders',lambda:self.sync_one(enabled=enabled))
        run('history',self.history.collect_one)
        run('fills',self.history.bind_known)
        run('funding',self.funding_one)
        # Only user activation allows preparing or sending a financial action.
        # Normal readonly account/fill display continues while the switch is off.
        if enabled:
            lots,_=self.cash.inventory()
            active={lot for sleeve,lot,key in lots}
            active.update(r[0] for r in self.db.execute("SELECT lot_id FROM simple_plans WHERE entry_closed=0"))
            active.update(r[0] for r in self.db.execute("SELECT DISTINCT lot_id FROM simple_requests WHERE state NOT IN ('TERMINAL','NOT_SENT')"))
            active.update(r[0] for r in self.db.execute("""SELECT p.lot_id FROM simple_plans p WHERE
                json_extract(p.body,'$.sleeve') IN ('M784','S800') AND NOT EXISTS
                (SELECT 1 FROM receipts r WHERE r.lot_id=p.lot_id AND json_extract(r.body,'$.kind')='settled')"""))
            ids=sorted(active)
            if ids:
                lot=ids[self._lot_cursor%len(ids)];self._lot_cursor+=1
                run('protection',lambda:self.protect(lot))
                run('settlement',lambda:self.broker.publish_settled(lot,self.history,now_ms=self.exchange.transport.now_ms()))
            run('task',lambda:self.task_one(entries_allowed=entries_allowed))
        return dict(results=results,errors=errors)

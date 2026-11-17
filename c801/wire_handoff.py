"""One-call proof of a committed send which has not reached the wire yet.

Never persisted or recovered. A restarted SENDING row has no such proof and
must still be queried. This object neither approves risk nor extends a lease.
"""
import time
import uuid
import json
from .account import InvalidState


class PrewireExpired(InvalidState):
    pass


class WireHandoff:
    def __init__(self, identity, *, clock, not_before_ms, expires_ms, check=None, category='ORDER'):
        if type(not_before_ms) is not int or type(expires_ms) is not int or not_before_ms >= expires_ms:
            raise InvalidState('发送前有效时间窗口为空')
        if category not in ('ORDER','PROTECTION_SEND','PROTECTION_CANCEL','ORDER_CANCEL','LEVERAGE'):
            raise InvalidState('发送交接用途错误')
        if identity is None and category not in ('ORDER_CANCEL','LEVERAGE'):
            raise InvalidState('发送交接缺少原始委托身份')
        self.identity, self.category = identity, category
        self.nonce = uuid.uuid4().hex
        self.clock, self.check = clock, check
        self.not_before_ms, self.expires_ms = not_before_ms, expires_ms
        self._phase = 'NEW'
        self._execution = self._row = None

    @property
    def attempted(self):
        return self._phase == 'ATTEMPTED'

    def bind(self, execution, row):
        if self._phase != 'NEW' or not execution.db.in_transaction or self.identity is not None and row['id'] != self.identity:
            raise InvalidState('发送交接只能绑定本次已核验的状态迁移')
        expected_state='CANCELING' if self.category=='PROTECTION_CANCEL' else 'SENDING'
        if row['state'] != expected_state or self.category in ('ORDER','PROTECTION_SEND') and row['send_count'] != 1 or row.get('acknowledged'):
            raise InvalidState('发送交接缺少唯一未发出的新委托')
        latest=execution.db.execute('SELECT kind,body FROM audit ORDER BY seq DESC LIMIT 1').fetchone()
        event=json.loads(latest[1]) if latest is not None else {}
        if self.category=='ORDER':
            valid=latest is not None and latest[0]=='RISK_ACCEPT' and (event.get('id'),event.get('client_id'))==(row['id'],row['client_id'])
        else:
            kind,stage,field={
                'PROTECTION_SEND':('PROTECTION_ACK','ALGO_SENDING','protection'),
                'PROTECTION_CANCEL':('PROTECTION_ACK','ALGO_CANCEL_SENDING','protection'),
                'ORDER_CANCEL':('RECOVERY','ORDER_CANCEL_STARTED','cancel'),
                'LEVERAGE':('RECOVERY','LEVERAGE_SENDING','change')}[self.category]
            valid=latest is not None and latest[0]==kind and event.get('stage')==stage and event.get(field)==row
        if not valid or event.get('send_handoff')!=self.nonce:
            raise InvalidState('旧发送状态不能重新生成未发送证明')
        from copy import deepcopy
        self.identity=row['id']
        self._execution, self._row = execution, deepcopy(row)
        self._changes = execution.db.total_changes
        self._version = execution.db.execute('PRAGMA data_version').fetchone()[0]
        self._schemas = tuple(execution.db.execute('PRAGMA '+name+'.schema_version').fetchone()[0] for name in ('main','temp'))
        self._phase = 'READY'

    def _unchanged(self):
        x = self._execution
        if (x.db.in_transaction or x.db.total_changes != self._changes or x.db.execute('PRAGMA data_version').fetchone()[0] != self._version
                or tuple(x.db.execute('PRAGMA '+name+'.schema_version').fetchone()[0] for name in ('main','temp')) != self._schemas):
            raise InvalidState('核验后的本机账户状态发生变化，委托尚未发送')

    def _time(self):
        now = self.clock()
        if type(now) is not int or not self.not_before_ms <= now < self.expires_ms:
            raise PrewireExpired('委托或核验依据已超过发送有效期')

    def before_attempt(self):
        if self._phase != 'READY':
            raise InvalidState('发送交接已使用或失效，禁止重复通信')
        self._time()
        self._unchanged()
        monotonic_deadline = self.check() if self.check is not None else None
        self._unchanged()
        # Check again AFTER all validation, signing and adapter preparation.
        self._time()
        if monotonic_deadline is not None and time.monotonic_ns() > monotonic_deadline:
            raise PrewireExpired('独立监管在发送前已超时')
        self._phase = 'ATTEMPTED'

    def abort(self, execution, identity, *, category='ORDER'):
        if self._phase != 'READY' or execution is not self._execution or identity != self.identity or category!=self.category:
            raise InvalidState('没有本次调用确定未发出的交接证明')
        self._phase = 'ABORTED'  # A failed database commit cannot grant reuse.
        return self._row

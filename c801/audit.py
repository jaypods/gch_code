"""Durable ordered audit trail. No raw API payloads or credentials are accepted.

Hash chaining detects accidental/tampered rows relative to a trusted anchor; it
does not claim to resist an administrator replacing the database and its anchor.
"""
import hashlib
import json
import re
import sqlite3
import sys
from contextlib import contextmanager
from .account import InvalidState
from .immutable_memo import ImmutableMemo


FORBIDDEN = {"apikey", "apisecret", "secret", "secretkey", "signature", "authorization",
             "credential", "credentials", "password", "privatekey", "accesskey", "token"}
EVENTS = {"STARTUP", "ACCOUNT_RECONCILED", "DECISION", "RISK_REJECT", "RISK_ACCEPT",
          "ORDER_PREPARED", "ORDER_SEND_UNKNOWN", "ORDER_ACK", "FILL", "FUNDING",
          "PROTECTION_ACK", "PROTECTION_FAILED", "HEARTBEAT", "HALT", "SELF_CHECK",
          "RECOVERY", "MANUAL_ACTION", "BACKTEST_COMPLETE", "CHECKPOINT"}

# Store only fixed-size SHA-256 values, never payloads or decoded objects.
# Once full, retain the validated prefix and check later bodies normally. A
# complete scan larger than the cap must not evict every useful old entry.
_BODY_VALIDATION_CACHE_LIMIT = 262144
_DIGEST_MEMO = ImmutableMemo(128 * 1024 * 1024)
# Retain the present long-running account history within a fixed budget.
# Every external snapshot still rereads all exact rows and checks the chain.
_ROW_VALIDATION_CACHE_BYTES = 256 * 1024 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def unique_object(pairs):
    """SQLite and Python must never interpret a repeated JSON key differently."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidState('审计记录包含重复字段')
        result[key] = value
    return result


def reject_secrets(value):
    if isinstance(value, dict):
        for k, item in value.items():
            if not isinstance(k, str) or re.sub(r"[^a-z]", "", k.lower()) in FORBIDDEN:
                raise InvalidState("审计日志拒绝敏感字段")
            reject_secrets(item)
    elif isinstance(value, (tuple, list)):
        for item in value:
            reject_secrets(item)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise InvalidState("审计日志字段类型不支持")


class _AuditConnection(sqlite3.Connection):
    """Track audit writes at SQL preparation, including cursors and triggers.

    Statement caching is disabled so the authorizer sees EVERY execution.
    External commits are independently detected with data_version in a held
    read snapshot. Neither hook replaces the hash chain or materialized checks.
    """
    def __init__(self, *args, **kwargs):
        kwargs['cached_statements'] = 0
        super().__init__(*args, **kwargs)
        self.transaction_epoch = 0
        self.audit_epoch = 0
        self.checkpoint_epoch = 0
        self._append_target = None
        self._append_active = False
        self.prefix_reuse_enabled = True
        self._diagnostic_trace = None
        self._extra_authorizer = None
        super().set_trace_callback(self._trace)
        super().set_authorizer(self._authorize)

    def _authorize(self, action, arg1, arg2, database, trigger):
        table=(arg1 or '').lower()
        if (action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE)
                and table in ('audit','runtime_inputs','runtime_cycles','runtime_meta',
                    'runtime_code_rebinds','runtime_unstarted_rearms','runtime_outage_reconstructions')):
            # Only the fixed plain INSERT below can preserve an immutable
            # prefix. Trigger writes, REPLACE/UPSERT, cursors and ordinary SQL
            # invalidate it. The new suffix still needs full verification.
            if (action==sqlite3.SQLITE_INSERT and database=='main'
                    and trigger is None and table==self._append_target):
                # Consume the authorization at preparation. SQL invoked from
                # a trigger/UDF must not inherit this token while execute runs.
                self._append_target=None
            else:
                self.checkpoint_epoch += 1
        # SQLite prepares an INSERT into sqlite_master even for a no-op
        # CREATE TABLE/INDEX IF NOT EXISTS. Real DDL changes schema_version;
        # writable-schema access disables reuse, including the config API.
        # Do not turn routine existing-table checks into full audit rescans.
        if action==sqlite3.SQLITE_PRAGMA and table=='writable_schema' and arg2 is not None:
            self.prefix_reuse_enabled=False
        if ((action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE) and table=='audit')
                or (action in (sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE)
                    and table in ('sqlite_master','sqlite_schema','sqlite_temp_master','sqlite_temp_schema'))
                or action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH)
                or action == sqlite3.SQLITE_PRAGMA and arg2 is not None):
            self.audit_epoch += 1
        if (action in (sqlite3.SQLITE_ATTACH,sqlite3.SQLITE_DETACH)
                or action==sqlite3.SQLITE_PRAGMA and arg2 is not None):
            self.checkpoint_epoch += 1
        if self._extra_authorizer is not None:
            return self._extra_authorizer(action, arg1, arg2, database, trigger)
        return sqlite3.SQLITE_OK

    def set_authorizer(self, callback):
        if callback is not None and not callable(callback):
            raise TypeError('authorizer must be callable or None')
        self.audit_epoch += 1
        self.checkpoint_epoch += 1
        self._extra_authorizer = callback

    def setconfig(self, op, enable=True, /):
        if op==getattr(sqlite3,'SQLITE_DBCONFIG_WRITABLE_SCHEMA',None) and enable:
            self.audit_epoch+=1
            self.prefix_reuse_enabled=False
        return super().setconfig(op,enable)

    def blobopen(self, table, column, row, /, *, readonly=False, name='main'):
        # A writable handle can mutate again after a successful verification.
        # Keep this connection on full scans for its remaining lifetime.
        if not readonly:
            self.audit_epoch += 1
            self.prefix_reuse_enabled = False
        return super().blobopen(table, column, row, readonly=readonly, name=name)

    def deserialize(self, data, /, *, name='main'):
        self.audit_epoch += 1
        self.prefix_reuse_enabled = False
        return super().deserialize(data, name=name)

    def _trace(self, statement):
        # total_changes does not decrease when ROLLBACK TO undoes a write.
        # Trace executes even for cached SQL, unlike an authorizer callback.
        # Conservative matches in comments/literals only discard a proof.
        if re.search(r'\b(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b', statement, re.I):
            self.transaction_epoch += 1
        if re.search(r'\bROLLBACK\b', statement, re.I):
            self.audit_epoch += 1
            self.checkpoint_epoch += 1
        if self._diagnostic_trace is not None:
            self._diagnostic_trace(statement)

    def set_trace_callback(self, trace_callback):
        if trace_callback is not None and not callable(trace_callback):
            raise TypeError('trace callback must be callable or None')
        self._diagnostic_trace = trace_callback


def _append_immutable_row(db,table,values):
    """Record an append shape, never a proof of its contents or membership.

    No caller-supplied SQL, REPLACE or UPSERT can use this path. Unknown trigger
    effects still invalidate the connection's checkpoint prefix. Readers must
    verify every newly inserted row against its actual audited source.
    """
    sql={'audit':'INSERT INTO audit VALUES(?,?,?,?,?,?)',
         'runtime_inputs':'INSERT INTO runtime_inputs VALUES(?,?,?,?,?,?,?,?)',
         'runtime_cycles':'INSERT INTO runtime_cycles VALUES(?,?,?,?,?,?,?,?,?)'}[table]
    if type(db) is not _AuditConnection:return db.execute(sql,values)
    if db._append_active:raise InvalidState('不能嵌套已知追加操作')
    db._append_active=True;db._append_target=table
    try:return db.execute(sql,values)
    finally:db._append_target=None;db._append_active=False


class AuditLog:
    def __init__(self, path):
        self.db = sqlite3.connect(str(path), isolation_level=None, factory=_AuditConnection)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS audit(
            seq INTEGER PRIMARY KEY, time_ms INTEGER NOT NULL, kind TEXT NOT NULL,
            body TEXT NOT NULL, previous TEXT NOT NULL, digest TEXT NOT NULL)""")
        # Lookup only; it grants no permission to omit materialized records.
        self.db.execute('CREATE INDEX IF NOT EXISTS audit_kind_seq ON audit(kind,seq)')

    def close(self):
        self.db.close()
        cache = getattr(self, '_body_validation_cache', None)
        if cache is not None:
            cache.clear()
        rows = getattr(self, '_validated_rows', None)
        if rows is not None:
            rows.clear()
        self._validated_row_bytes = 0
        self._chain_prefix_proof = None

    @staticmethod
    def digest(seq, time_ms, kind, body, previous):
        # A complete exact row is the input, not its recorded digest or seq
        # alone. This reuses a pure calculation, never an audit-chain proof.
        return _DIGEST_MEMO.value((seq, time_ms, kind, body, previous),
            lambda: hashlib.sha256(canonical([seq, time_ms, kind, body, previous]).encode("utf-8")).hexdigest(),
            policy=(canonical, json.dumps, hashlib.sha256))

    def append(self, time_ms, kind, payload):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            result = self.append_in_transaction(time_ms, kind, payload)
            self.db.execute("COMMIT")
            return result
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def append_in_transaction(self, time_ms, kind, payload):
        """Bind a state mutation and its audit row to one SQLite commit."""
        if not self.db.in_transaction:
            raise InvalidState("审计事件必须与状态变更一起保存")
        if type(time_ms) is not int or time_ms < 0 or kind not in EVENTS or not isinstance(payload, dict):
            raise InvalidState("审计事件不合法")
        reject_secrets(payload)
        body = canonical(payload)
        # Certify the exact serialized body, not a mutable/custom mapping's
        # earlier traversal. Only such checked appends may extend a proof.
        decoded = json.loads(body, object_pairs_hook=unique_object)
        if type(decoded) is not dict:raise InvalidState('审计事件必须是对象')
        reject_secrets(decoded)
        last = self.db.execute("SELECT seq,time_ms,digest FROM audit ORDER BY seq DESC LIMIT 1").fetchone()
        seq, previous = (last[0] + 1, last[2]) if last else (1, "0" * 64)
        if last and time_ms < last[1]:
            raise InvalidState("审计接收时间不能倒退")
        digest = self.digest(seq, time_ms, kind, body, previous)
        before = self._prefix_key()
        prefix = getattr(self, '_chain_prefix_proof', None)
        _append_immutable_row(self.db,'audit',(seq, time_ms, kind, body, previous, digest))
        after = self._prefix_key()
        if (before is not None and after is not None and prefix is not None and prefix[0] == before
                and prefix[1] == seq-1 and prefix[3] == previous
                and after[0] == before[0] and after[1] == before[1]+1
                and after[2:5] == before[2:5] and after[6:] == before[6:]
                and after[5] == (seq, time_ms, digest)):
            # Any trigger writing another audit row, schema mutation, rollback,
            # outside commit or policy change prevents this extension.
            self._chain_prefix_proof = (after, seq, time_ms, digest)
        return {"seq": seq, "digest": digest}

    @contextmanager
    def verification_scope(self):
        """Reuse a complete audit proof only in a held database snapshot.

        Materialized-state proofs remain scoped to the exact held snapshot.
        The independent audit-prefix certificate may survive scopes only under
        _prefix_key's stronger connection/write/rollback/schema/anchor checks.
        No incomplete audit or materialized-state result is cached.
        """
        if not self.db.in_transaction:raise InvalidState('审计核验复用需要同一数据库事务')
        prior=getattr(self,'_verification_cache',None)
        self._verification_cache={} if prior is None else prior
        try:yield
        finally:self._verification_cache=prior

    @staticmethod
    def _validation_policy():
        return (reject_secrets, unique_object, json.loads, frozenset(FORBIDDEN), _BODY_VALIDATION_CACHE_LIMIT)

    def _body_cache(self):
        """Memoize only JSON/field validation after checking the full current row.

        This is not an audit, database, anchor or materialized-state proof.
        A full scan rereads the exact bytes of every row and checks the
        complete chain. A changed row cannot reuse structural validation under
        its old recorded digest. No mutable decoded value is retained.
        """
        policy = self._validation_policy()
        if getattr(self, '_body_validation_policy', None) != policy:
            self._body_validation_policy = policy
            self._body_validation_cache = set()
        return self._body_validation_cache

    def _row_policy(self):
        return (self.digest, canonical, json.dumps, hashlib.sha256,
                self._validation_policy(), frozenset(EVENTS), _ROW_VALIDATION_CACHE_BYTES)

    def _row_cache(self):
        # Exact immutable current rows only, including the recorded digest.
        # A hit reuses pure hash/JSON validation, never row existence, order,
        # chronology, links, anchors or any materialized-state proof.
        policy = self._row_policy()
        if getattr(self, '_validated_row_policy', None) != policy:
            self._validated_row_policy = policy
            self._validated_rows = set()
            self._validated_row_bytes = 0
        return self._validated_rows

    def _remember_row(self, row):
        # SQLite normally returns integer-affinity values as int, but a
        # replacement/view may return 2.0, which compares equal to 2 while
        # having a different canonical digest. Such rows never use this memo.
        if type(row[0]) is not int or type(row[1]) is not int:
            return
        rows = self._validated_rows
        charge = sys.getsizeof(row) + sum(sys.getsizeof(v) for v in row)
        if self._validated_row_bytes + charge + 2 * sys.getsizeof(rows) + 512 > _ROW_VALIDATION_CACHE_BYTES:
            return
        rows.add(row)
        self._validated_row_bytes += charge
        if self._validated_row_bytes + sys.getsizeof(rows) > _ROW_VALIDATION_CACHE_BYTES:
            rows.clear()
            self._validated_row_bytes = 0

    def _verification_key(self, kind, trusted_anchor):
        if getattr(self,'_verification_cache',None) is None:return None
        if not self.db.in_transaction:raise InvalidState('审计核验事务已经结束')
        if isinstance(self.db, _AuditConnection) and (not self.db.prefix_reuse_enabled or self.db._extra_authorizer is not None):return None
        if self.db.row_factory is not None or self.db.text_factory is not str:return None
        # DDL is not counted by total_changes. Both schemas matter: a temporary
        # table/view can mask an unqualified financial or audit table.
        # Scoped materialized proofs track actual writes/transactions/schema,
        # not conservative authorizer preparation attempts (e.g. no-op CREATE
        # TABLE IF NOT EXISTS). Writable blobs and custom decoders/authorizers
        # disable scoped reuse entirely above.
        return (kind,self.db.total_changes,self.db.transaction_epoch,
                self.db.execute('PRAGMA main.data_version').fetchone()[0],
                self.db.execute('PRAGMA main.schema_version').fetchone()[0],
                self.db.execute('PRAGMA temp.schema_version').fetchone()[0],
                canonical(trusted_anchor), self._row_policy(),self.db.row_factory,self.db.text_factory)

    def _prefix_key(self):
        """Evidence that a previously checked chain is still the same chain.

        Only our uncached-statement connection is eligible. Reading the head
        pins SQLite's snapshot before checking external/schema versions. Own
        audit writes are tracked by the authorizer, including cached-looking
        repeated SQL, cursors and triggers. The certificate is process-local;
        reopening a file always requires a full scan.
        """
        db=self.db
        if (type(db) is not _AuditConnection or not db.in_transaction or not db.prefix_reuse_enabled or db._extra_authorizer is not None
                or db.row_factory is not None or db.text_factory is not str):
            return None
        head=db.execute('SELECT seq,time_ms,digest FROM audit ORDER BY seq DESC LIMIT 1').fetchone()
        versions=tuple(db.execute('PRAGMA '+p).fetchone()[0] for p in
            ('main.data_version','main.schema_version','temp.schema_version'))
        return (id(db),db.audit_epoch,*versions,head,self._row_policy())

    def _check_anchor(self, trusted_anchor):
        if trusted_anchor is None:return
        row=(self.db.execute('SELECT seq,digest FROM audit WHERE seq=?',
            (trusted_anchor['seq'],)).fetchone() if trusted_anchor else None)
        if row is None or row[0]!=trusted_anchor['seq']:raise InvalidState('审计链缺少已保存锚点')
        if row[1]!=trusted_anchor['digest']:raise InvalidState('审计链与已保存锚点不一致')

    def verify(self, trusted_anchor=None):
        if self.db.in_transaction:return self._verify_snapshot(trusted_anchor)
        if getattr(self,'_verification_cache',None) is not None:
            raise InvalidState('审计核验事务已经结束')
        # A head/version/proof must all describe one actual SQLite snapshot.
        # No old proof is promoted across an external writer racing this read.
        self.db.execute('BEGIN')
        try:
            proof=self._verify_snapshot(trusted_anchor)
            self.db.execute('COMMIT')
            return proof
        except BaseException:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise

    def _verify_snapshot(self, trusted_anchor):
        prefix_key=self._prefix_key()
        cache=getattr(self,'_verification_cache',None)
        key=self._verification_key('audit',trusted_anchor)
        if key is not None and key in cache:return dict(cache[key])
        whole_key=self._verification_key('audit',None)
        if whole_key is not None and whole_key in cache:
            # The complete chain has already been hashed in this exact held
            # snapshot. A different requested anchor needs its own equality
            # check, not another hash scan of the same chain. Any local write
            # (including rollback), schema/data change or policy change gives
            # this snapshot a different key.
            self._check_anchor(trusted_anchor)
            proof=dict(cache[whole_key]);cache[key]=dict(proof)
            return proof
        prefix=getattr(self,'_chain_prefix_proof',None)
        if prefix_key is not None and prefix is not None and prefix[0]==prefix_key:
            self._check_anchor(trusted_anchor)
            proof=dict(events=prefix[1],digest=prefix[3],verified=True)
            if key is not None:
                cache[key]=dict(proof);cache[whole_key]=dict(proof)
            return proof
        bodies = self._body_cache()
        validated_rows = self._row_cache()
        previous = "0" * 64
        expected, last_ms, anchor_seen = 1, -1, trusted_anchor is None
        for row in self.db.execute("SELECT * FROM audit ORDER BY seq"):
            seq, time_ms, kind, body, recorded_previous, digest = row
            if seq != expected or time_ms < last_ms or recorded_previous != previous:
                raise InvalidState("审计链序号、时间或前序指纹错误")
            if kind not in EVENTS:
                raise InvalidState("审计内容指纹错误")
            if type(seq) is not int or type(time_ms) is not int or row not in validated_rows:
                actual = self.digest(seq, time_ms, kind, body, previous)
                if digest != actual:
                    raise InvalidState("审计内容指纹错误")
                fingerprint = bytes.fromhex(actual)
                if fingerprint not in bodies:
                    decoded = json.loads(body, object_pairs_hook=unique_object)
                    if type(decoded) is not dict:
                        raise InvalidState('审计事件必须是对象')
                    reject_secrets(decoded)
                    if len(bodies) < _BODY_VALIDATION_CACHE_LIMIT:
                        bodies.add(fingerprint)
                self._remember_row(row)
            if trusted_anchor and seq == trusted_anchor["seq"]:
                if digest != trusted_anchor["digest"]:
                    raise InvalidState("审计链与已保存锚点不一致")
                anchor_seen = True
            expected, last_ms, previous = seq + 1, time_ms, digest
        if not anchor_seen:
            raise InvalidState("审计链缺少已保存锚点")
        proof={"events": expected - 1, "digest": previous, "verified": True}
        if prefix_key is not None and prefix_key==self._prefix_key():
            self._chain_prefix_proof=(prefix_key,expected-1,last_ms,previous)
        if key is not None and key==self._verification_key('audit',trusted_anchor):
            cache[key]=dict(proof)
            cache[whole_key]=dict(proof)
        return proof

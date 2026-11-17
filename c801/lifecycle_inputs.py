"""Actually received perpetual lifecycle dates and fixed local exit policy.

exchangeInfo supplies deliveryDate, not an official no-new-order timestamp.
The local thirty-minute entry/exit cutoff preserves the research convention;
it is labelled as a local rule. No announcement publication/arrival date is
invented. Far-future perpetual placeholder dates never become active calendar
objects. Once active, a changed date/identity requires reconciliation rather
than silently changing an already scheduled exit.
"""
from dataclasses import asdict
import json,hashlib
from .account import InvalidState,PositionKey
from .audit import AuditLog,canonical
from .market_metadata import parse_exchange_info,ContractMetadata
from .strategies.alt import AltCalendar

CUTOFF_MS=30*60000
ACTIVE_HORIZON_MS=30*86400000
MAX_AGE_MS=300000


class LifecycleInputs:
    def __init__(self,path):
        self.audit=AuditLog(path);self.db=self.audit.db
        self.rows={};self.first={};self.events={};self.received_ms=None;self.source_digest=None;self.faulted=False
        try:
            self.audit.verify()
            for stamp,kind,body in self.db.execute('SELECT time_ms,kind,body FROM audit ORDER BY seq'):
                value=json.loads(body)
                if kind=='HALT' and value.get('stage')=='LIFECYCLE_HALT':self.faulted=True;continue
                if kind!='SELF_CHECK':raise InvalidState('下架日历日志类型无效')
                self._apply(value,stamp)
        except BaseException:self.audit.close();raise

    def close(self):self.audit.close()

    def _healthy(self):
        if self.faulted:raise InvalidState('下架日历来源已暂停，需要核对')

    @staticmethod
    def identity(row):return row.contract_type,row.underlying_type,row.listed_ms,row.delivery_ms

    def _apply(self,value,stamp):
        self._healthy()
        if (type(stamp) is not int or stamp<0 or value.get('stage')!='LIFECYCLE_CATALOG'
                or type(value.get('contracts')) is not list or not value['contracts']
                or type(value.get('source_digest')) is not str or len(value['source_digest'])!=64
                or value.get('cutoff_ms')!=CUTOFF_MS or value.get('active_horizon_ms')!=ACTIVE_HORIZON_MS):
            raise InvalidState('下架日历来源记录无效')
        rows={};first={};events=dict(self.events)
        for data in value['contracts']:
            row=ContractMetadata(**data);PositionKey(row.symbol,'BOTH')
            if (row.symbol in rows or row.known_ms!=stamp or row.category not in ('main','alt','stock_us')
                    or type(row.listed_ms) is not int or type(row.delivery_ms) is not int
                    or not 0<=row.listed_ms<row.delivery_ms or not row.contract_type or not row.status):
                raise InvalidState('下架合约身份或时间无效')
            rows[row.symbol]=row;old=self.rows.get(row.symbol)
            first[row.symbol]=self.first[row.symbol] if old and self.identity(old)==self.identity(row) else stamp
            if row.symbol in events and (old is None or self.identity(old)!=self.identity(row)):
                raise InvalidState('LIFECYCLE_CHANGED_REQUIRES_RECONCILIATION')
            if row.symbol in events:continue
            # A past termination first seen at startup is not an actionable
            # notice from the past. Current tradability independently blocks it.
            if row.listed_ms<=stamp<row.delivery_ms<=stamp+ACTIVE_HORIZON_MS:
                known=first[row.symbol];cutoff=max(known,row.delivery_ms-CUTOFF_MS)
                event=AltCalendar(row.symbol,known,cutoff,cutoff,row.delivery_ms);event.validate()
                events[row.symbol]=event
        self.rows,self.first,self.events=rows,first,events
        self.received_ms=stamp;self.source_digest=value['source_digest']

    def observe(self,reply):
        self._healthy();universe=parse_exchange_info(reply)
        value=dict(stage='LIFECYCLE_CATALOG',source_digest=universe.source_digest,
            cutoff_ms=CUTOFF_MS,active_horizon_ms=ACTIVE_HORIZON_MS,
            contracts=[asdict(r) for r in universe.contracts if r.category!='other'])
        clone=object.__new__(type(self));clone.__dict__=self.__dict__.copy()
        try:clone._apply(value,universe.received_ms)
        except InvalidState as exc:
            if str(exc)=='LIFECYCLE_CHANGED_REQUIRES_RECONCILIATION':
                self.audit.append(universe.received_ms,'HALT',dict(stage='LIFECYCLE_HALT',reason=str(exc),source_digest=universe.source_digest))
                self.faulted=True
            raise
        self.audit.append(universe.received_ms,'SELF_CHECK',value)
        for name in ('rows','first','events','received_ms','source_digest'):setattr(self,name,getattr(clone,name))
        return universe

    def calendars(self,symbols,*,now_ms):
        self._healthy()
        if (type(now_ms) is not int or self.received_ms is None
                or not 0<=now_ms-self.received_ms<=MAX_AGE_MS):
            raise InvalidState('下架目录尚未收到或已过期')
        symbols=tuple(symbols)
        if not symbols or len(set(symbols))!=len(symbols):raise InvalidState('下架日历检查范围缺失或重复')
        result=[]
        for symbol in symbols:
            PositionKey(symbol,'BOTH');row=self.rows.get(symbol);event=self.events.get(symbol)
            if row is None:
                if event is None:raise InvalidState('LIFECYCLE_SYMBOL_MISSING')
                # Keep an observed exit instruction even after catalog removal.
                # The execution/account layer must reconcile any surviving fill.
                result.append(event);continue
            if row.category!='alt':raise InvalidState('山寨日历范围包含其他账户品种')
            if event is not None:result.append(event)
            elif not row.tradable(now_ms=now_ms):raise InvalidState('LIFECYCLE_INACTIVE_WITHOUT_NOTICE')
        return tuple(result)

    def status(self):
        return dict(received_ms=self.received_ms,source_digest=self.source_digest,faulted=self.faulted,
            contracts=len(self.rows),active_calendars=len(self.events),
            late_first_arrivals=sum(e.known_ms>e.settlement_ms-CUTOFF_MS for e in self.events.values()),
            cutoff_rule='local_30_minutes_before_delivery_not_exchange_no_new_timestamp')

    def verify(self):return dict(self.audit.verify(),**self.status())

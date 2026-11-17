"""Actual published funding parser; no audit journal or replay gate."""
from dataclasses import dataclass
from decimal import Decimal,InvalidOperation
import hashlib
from .account import InvalidState,PositionKey
from .audit import canonical
from .execution_market import receipt


def sha(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def integer(value):return type(value) is int and value>=0


def number(value,*,positive=False):
    if type(value) not in (str,int,float):raise InvalidState('资金费字段不是数值')
    try:d=Decimal(str(value))
    except InvalidOperation:raise InvalidState('资金费数值无效') from None
    if not d.is_finite() or (positive and d<=0):raise InvalidState('资金费数值无效')
    return str(d)


@dataclass(frozen=True)
class SettledFunding:
    symbol:str
    time_ms:int
    rate_type:str
    rate:str
    mark:str
    received_ms:int
    source_digest:str

    @property
    def key(self):return self.symbol,self.time_ms,self.rate_type

    def shadow_event(self):
        return dict(symbol=self.symbol,time_ms=self.time_ms,rate_type=self.rate_type,
            rate=float(self.rate),mark=float(self.mark),mark_event_ms=self.time_ms,
            mark_known_ms=self.received_ms,received_ms=self.received_ms,
            source_digest=self.source_digest)


def parse_funding_history(reply,*,symbol,start_ms,end_ms,limit=1000):
    receipt(reply);PositionKey(symbol,'BOTH')
    if (not integer(start_ms) or not integer(end_ms) or start_ms>end_ms
            or end_ms>=reply.sent_ms or type(limit) is not int or not 1<=limit<=1000
            or type(reply.body) is not list or len(reply.body)>limit):
        raise InvalidState('资金费查询范围、接收时间或分页无效')
    result=[];seen=set();last=-1
    for row in reply.body:
        if type(row) is not dict or row.get('symbol')!=symbol:raise InvalidState('资金费合约不匹配')
        t=row.get('fundingTime');kind=row.get('rateType','Regular')
        if not integer(t) or not start_ms<=t<=end_ms or t<last or kind not in ('Regular','Special'):
            raise InvalidState('资金费时间、排序或费率类型无效')
        key=(symbol,t,kind)
        if key in seen:raise InvalidState('资金费记录缺少可区分的唯一身份')
        seen.add(key);last=t
        result.append(SettledFunding(symbol,t,kind,number(row.get('fundingRate')),
            number(row.get('markPrice'),positive=True),reply.received_ms,sha(row)))
    return tuple(result)

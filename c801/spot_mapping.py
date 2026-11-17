"""Observed exact-asset spot/perpetual identities, with durable first-known time.

No ticker-prefix guessing or retrospective first-known dates. For example a
1000-unit futures asset is not silently equated to a differently named spot
asset. Unmatched pairs are explicit and the rotation/spot-flow branch stays
unavailable for them; the independent ratio-leader branch is unchanged.
"""
from dataclasses import dataclass
import hashlib,json,re
from .account import InvalidState,PositionKey
from .audit import canonical
from .binance_transport import WireReply
from .market_metadata import parse_exchange_info


@dataclass(frozen=True)
class SpotPair:
    perpetual_symbol:str
    spot_symbol:str
    asset:str
    known_ms:int


def verify_mapping_record(previous,record,stamp):
    if (record.get('stage')!='SPOT_MAPPING' or type(stamp) is not int or stamp<0
            or type(record.get('identities')) is not list or type(record.get('unmatched')) is not list):
        raise InvalidState('品种对应关系记录无效')
    for name in ('futures_received_ms','spot_received_ms'):
        t=record.get(name)
        if type(t) is not int or not 0<=stamp-t<=300000:raise InvalidState('对应关系目录在记录时已过期')
    for name in ('futures_digest','spot_digest'):
        if type(record.get(name)) is not str or not re.fullmatch('[a-f0-9]{64}',record[name]):raise InvalidState('品种对应关系缺少来源指纹')
    result={}
    for row in record['identities']:
        if type(row) is not dict or set(row)!={'perpetual_symbol','spot_symbol','asset','known_ms'}:raise InvalidState('品种对应关系字段错误')
        p,s,a,k=(row[n] for n in ('perpetual_symbol','spot_symbol','asset','known_ms'))
        PositionKey(p,'BOTH');PositionKey(s,'BOTH')
        if p in result or type(a) is not str or not a or not a.isalnum() or s!=a+'USDT':raise InvalidState('品种对应关系身份冲突')
        old=previous.get(p);same=old is not None and old['spot_symbol']==s and old['asset']==a
        expected=old['known_ms'] if same else stamp
        if type(k) is not int or k!=expected:raise InvalidState('品种对应关系首次可知时间被回填或改动')
        result[p]=row
    unmatched=record['unmatched']
    if len(set(unmatched))!=len(unmatched) or set(unmatched)&result.keys():raise InvalidState('未匹配品种范围冲突')
    for symbol in unmatched:PositionKey(symbol,'BOTH')
    return result


class SpotMappings:
    def __init__(self,store):self.store=store

    def state(self):
        row=self.store.db.execute("SELECT time_ms,body FROM audit WHERE kind='SELF_CHECK' AND json_extract(body,'$.stage')='SPOT_MAPPING' ORDER BY seq DESC LIMIT 1").fetchone()
        if row is None:return None
        return row[0],json.loads(row[1])

    def record(self,futures,spot):
        universe=parse_exchange_info(futures)
        if (not isinstance(spot,WireReply) or any(type(t) is not int for t in (spot.sent_ms,spot.received_ms))
                or not 0<=spot.sent_ms<=spot.received_ms or type(spot.body) is not dict or type(spot.body.get('symbols')) is not list
                or not spot.body['symbols']):raise InvalidState('完整现货目录未收到')
        now=max(futures.received_ms,spot.received_ms);seen=set();assets={}
        for row in spot.body['symbols']:
            if type(row) is not dict or type(row.get('symbol')) is not str or row['symbol'] in seen:raise InvalidState('现货目录品种无效或重复')
            seen.add(row['symbol'])
            if row.get('quoteAsset')!='USDT':continue
            if row.get('status')!='TRADING' or row.get('isSpotTradingAllowed') is not True:continue
            asset=row.get('baseAsset');symbol=row['symbol'];PositionKey(symbol,'BOTH')
            if type(asset) is not str or not asset or not asset.isalnum() or symbol!=asset+'USDT' or asset in assets:
                raise InvalidState('现货目录标的身份不完整或有歧义')
            assets[asset]=symbol
        active={c.symbol for c in universe.contracts if c.category in ('main','alt') and c.tradable(now_ms=now)}
        previous=self.state();previous={} if previous is None else {r['perpetual_symbol']:r for r in previous[1]['identities']}
        identities=[];unmatched=[]
        for row in sorted(futures.body['symbols'],key=lambda r:r['symbol']):
            symbol=row['symbol']
            if symbol not in active:continue
            asset=row.get('baseAsset')
            if type(asset) is not str or not asset or not asset.isalnum():raise InvalidState('合约缺少可核实的基础资产')
            if asset not in assets:unmatched.append(symbol);continue
            spot_symbol=assets[asset];old=previous.get(symbol)
            known=old['known_ms'] if old and (old['spot_symbol'],old['asset'])==(spot_symbol,asset) else now
            identities.append(dict(perpetual_symbol=symbol,spot_symbol=spot_symbol,asset=asset,known_ms=known))
        record=dict(stage='SPOT_MAPPING',identities=identities,unmatched=unmatched,
            futures_received_ms=futures.received_ms,spot_received_ms=spot.received_ms,
            futures_digest=universe.source_digest,spot_digest=hashlib.sha256(canonical(spot.body).encode()).hexdigest())
        verify_mapping_record(previous,record,now)
        if self.store.faulted():raise InvalidState('行情缓存已暂停')
        self.store.put_mapping(record,received_ms=now)
        return dict(mapped=len(identities),unmatched=len(unmatched),received_ms=now)

    def current(self,*,now_ms):
        state=self.state()
        if state is None:raise InvalidState('尚未观察完整的现货与合约对应关系')
        stamp,record=state
        if type(now_ms) is not int or now_ms<stamp or any(not 0<=now_ms-record[k]<=300000 for k in ('futures_received_ms','spot_received_ms')):
            raise InvalidState('现货与合约身份目录过期或尚未收到')
        return {r['perpetual_symbol']:SpotPair(**r) for r in record['identities']}

"""Observed USDT contract identities, not retrospectively invented universes.

EQUITY is the US equity schedule category; other equity regions, commodities,
FX, crypto indices and pre-market contracts stay outside the three target pools.
Strategy-specific exclusions and cohort ordering still belong to each strategy.
This current catalog cannot prove membership in a historical backtest universe.
"""
from dataclasses import dataclass
import hashlib
from .account import InvalidState,PositionKey
from .audit import canonical
from .binance_transport import WireReply


@dataclass(frozen=True)
class ContractMetadata:
    symbol:str
    contract_type:str
    underlying_type:str
    status:str
    listed_ms:int
    delivery_ms:int
    known_ms:int
    category:str
    source_digest:str

    def tradable(self,*,now_ms,max_age_ms=300000):
        if type(now_ms) is not int or type(max_age_ms) is not int or max_age_ms<=0:
            raise InvalidState('合约身份核查时间错误')
        return (self.category!='other' and self.status=='TRADING'
                and self.listed_ms<=now_ms<self.delivery_ms
                and 0<=now_ms-self.known_ms<=max_age_ms)


@dataclass(frozen=True)
class MarketUniverse:
    received_ms:int
    contracts:tuple[ContractMetadata,...]
    total_exchange_symbols:int
    excluded_margin_symbols:int
    source_digest:str

    def symbols(self,category,*,now_ms):
        if category not in ('main','alt','stock_us','other'):raise InvalidState('品种分类错误')
        # Stable lexical order, independent of REST array reorder. Original
        # strategy source cohorts may impose their own explicitly saved order.
        return tuple(sorted(c.symbol for c in self.contracts if c.category==category and c.tradable(now_ms=now_ms)))


def parse_exchange_info(reply):
    if (not isinstance(reply,WireReply) or type(reply.sent_ms) is not int or type(reply.received_ms) is not int
            or not 0<=reply.sent_ms<=reply.received_ms or type(reply.body) is not dict
            or type(reply.body.get('symbols')) is not list or not reply.body['symbols']):
        raise InvalidState('完整合约目录未收到或格式错误')
    seen=set();contracts=[];excluded=0
    try:
        for row in reply.body['symbols']:
            if type(row) is not dict or type(row.get('symbol')) is not str or not row['symbol'] or row['symbol'] in seen:
                raise InvalidState('合约目录包含无效或重复身份')
            seen.add(row['symbol'])
            if not all(type(row.get(k)) is str and row[k] for k in ('quoteAsset','marginAsset')):
                raise InvalidState('合约缺少计价与保证金币种')
            if row['quoteAsset']!='USDT' or row['marginAsset']!='USDT':excluded+=1;continue
            symbol=row['symbol']
            if not 1<=len(symbol)<=80 or not symbol.replace('_','').isalnum():
                raise InvalidState('合约目录身份含无效字符')
            if not all(type(row.get(k)) is str and row[k] for k in ('contractType','underlyingType','status')):
                raise InvalidState('合约缺少可核实的标的类型或状态')
            listed,delivery=row['onboardDate'],row['deliveryDate']
            if type(listed) is not int or type(delivery) is not int or not 0<=listed<delivery:
                raise InvalidState('合约上市或到期时间无效')
            kind,underlying=row['contractType'],row['underlyingType'];category='other'
            if kind=='PERPETUAL' and underlying=='COIN':category='main' if symbol in ('BTCUSDT','ETHUSDT') else 'alt'
            elif kind=='TRADIFI_PERPETUAL' and underlying=='EQUITY':category='stock_us'
            if category!='other':PositionKey(symbol,'BOTH')
            contracts.append(ContractMetadata(symbol,kind,underlying,row['status'],listed,delivery,
                reply.received_ms,category,hashlib.sha256(canonical(row).encode()).hexdigest()))
    except (KeyError,TypeError):raise InvalidState('合约目录字段不完整') from None
    return MarketUniverse(reply.received_ms,tuple(contracts),len(seen),excluded,
        hashlib.sha256(canonical(reply.body).encode()).hexdigest())

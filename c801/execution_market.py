"""Observed execution prices and exact exchange/account filter conversion.

No candle extremes, symbol-name heuristics, configured leverage guesses or
precision-field rounding. The account's observed symbol-config notional limit
is authoritative; a notionalCoef is never guessed/applied a second time.
Public REST clock leads up to 500ms may be retained by the collector, but can
only be delivered at a later actual time that is at least the event timestamp.
"""
from dataclasses import dataclass
from decimal import Decimal as D
import hashlib,math
from .account import InvalidState,PositionKey,ContractRules,positive,nonnegative
from .audit import canonical
from .binance_transport import WireReply
from .market_metadata import parse_exchange_info



@dataclass(frozen=True)
class EntryQuote:
    symbol:str
    event_ms:int
    received_ms:int
    bid:D
    ask:D
    request_ms:int|None=None

    def __post_init__(self):
        PositionKey(self.symbol,'BOTH')
        if type(self.event_ms) is not int or type(self.received_ms) is not int or not 0<=self.event_ms<=self.received_ms:raise InvalidState('可成交报价时间错误')
        object.__setattr__(self,'bid',positive(self.bid));object.__setattr__(self,'ask',positive(self.ask))
        if self.bid>self.ask:raise InvalidState('可成交买卖报价交叉')
        if self.request_ms is not None and (type(self.request_ms) is not int or not 0<=self.request_ms<=self.received_ms):raise InvalidState('报价实际查询时间错误')

    @property
    def freshness_ms(self):
        # REST time is the latest book transaction, which may be old on a
        # quiet market. Preserve it and separately use an actual query start;
        # never advance freshness merely by rereading an in-memory value.
        return max(self.event_ms,self.request_ms if self.request_ms is not None else 0)


@dataclass(frozen=True)
class ContractPrice:
    symbol:str
    event_ms:int
    received_ms:int
    price:D
    request_ms:int|None=None

    def __post_init__(self):
        PositionKey(self.symbol,'BOTH')
        if any(type(t) is not int or t<0 for t in (self.event_ms,self.received_ms)):raise InvalidState('成交价格时间格式错误')
        if self.event_ms>self.received_ms:raise InvalidState('合约成交价格尚未收到')
        object.__setattr__(self,'price',positive(self.price))
        if self.request_ms is not None and (type(self.request_ms) is not int or not 0<=self.request_ms<=self.received_ms):raise InvalidState('成交价实际查询时间错误')

    @property
    def freshness_ms(self):return max(self.event_ms,self.request_ms if self.request_ms is not None else 0)

def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def receipt(reply):
    if (not isinstance(reply,WireReply) or type(reply.sent_ms) is not int or type(reply.received_ms) is not int
            or not 0<=reply.sent_ms<=reply.received_ms or reply.http_status!=200):
        raise InvalidState('行情或规则缺少实际成功接收时间')


def timed(reply,symbol,delivered_ms):
    receipt(reply);PositionKey(symbol,'BOTH');r=reply.body
    if type(r) is not dict or r.get('symbol')!=symbol:raise InvalidState('行情合约与请求不一致')
    at=reply.received_ms if delivered_ms is None else delivered_ms;t=r.get('time')
    if (type(at) is not int or at<reply.received_ms or type(t) is not int
            or not 0<=t<=reply.received_ms+500 or t>at):
        raise InvalidState('公开价格尚未可知或时钟偏差过大')
    return r,t,at


def parse_book_ticker(reply,*,symbol,delivered_ms=None):
    r,t,at=timed(reply,symbol,delivered_ms)
    try:
        positive(r['bidQty']);positive(r['askQty'])
        return EntryQuote(symbol,t,at,r['bidPrice'],r['askPrice'],request_ms=reply.sent_ms)
    except KeyError:raise InvalidState('买卖报价缺少价格或可成交数量') from None


def parse_contract_price(reply,*,symbol,delivered_ms=None):
    r,t,at=timed(reply,symbol,delivered_ms)
    if 'price' not in r:raise InvalidState('合约成交价缺失')
    return ContractPrice(symbol,t,at,r['price'],request_ms=reply.sent_ms)


@dataclass(frozen=True)
class MarkPrice:
    symbol:str
    event_ms:int
    received_ms:int
    price:D
    first_arrival_ms:int
    source_digest:str


def parse_mark_price(reply,*,symbol,delivered_ms=None):
    r,t,at=timed(reply,symbol,delivered_ms)
    if 'markPrice' not in r:raise InvalidState('标记价格缺失，不能用成交价替代')
    return MarkPrice(symbol,t,at,positive(r['markPrice']),reply.received_ms,digest(r))


@dataclass(frozen=True)
class LeverageBrackets:
    symbol:str
    query_ms:int
    received_ms:int
    maximum_leverage:int
    tiers:tuple
    notional_coefficient:D
    source_digest:str


def parse_leverage_brackets(reply,*,symbol):
    receipt(reply);PositionKey(symbol,'BOTH');r=reply.body
    if type(r) is list:
        if len(r)!=1:raise InvalidState('风险档位必须来自指定单一合约')
        r=r[0]
    if type(r) is not dict or r.get('symbol')!=symbol or type(r.get('brackets')) is not list or not r['brackets']:
        raise InvalidState('账户合约风险档位缺失或身份错误')
    tiers=[];prior_cap=D(0);prior_lev=None;prior_ratio=D(0)
    try:
        for i,b in enumerate(r['brackets'],1):
            if type(b) is not dict or type(b.get('bracket')) is not int or b['bracket']!=i:raise InvalidState('风险档位序号不连续')
            lev=b['initialLeverage'];floor=nonnegative(b['notionalFloor']);cap=positive(b['notionalCap'])
            ratio=positive(b['maintMarginRatio']);cum=nonnegative(b['cum'])
            if (type(lev) is not int or lev<1 or prior_lev is not None and lev>prior_lev or floor!=prior_cap or cap<=floor
                    or not prior_ratio<=ratio<1):raise InvalidState('风险档位范围、杠杆或维持保证金矛盾')
            tiers.append((lev,floor,cap,ratio,cum));prior_cap,prior_lev,prior_ratio=cap,lev,ratio
        coefficient=positive(r.get('notionalCoef',1))
    except KeyError:raise InvalidState('账户风险档位字段不完整') from None
    return LeverageBrackets(symbol,reply.sent_ms,reply.received_ms,tiers[0][0],tuple(tiers),coefficient,digest(r))


def common_step(left,right):
    exponent=min(left.as_tuple().exponent,right.as_tuple().exponent,0)
    if exponent < -18:raise InvalidState('数量步长精度超过支持范围')
    a=int(left.scaleb(-exponent));b=int(right.scaleb(-exponent))
    return D(math.lcm(a,b)).scaleb(exponent)


@dataclass(frozen=True)
class SymbolFilters:
    metadata:object
    minimum_price:D
    maximum_price:D|None
    tick:D
    step:D
    minimum_quantity:D
    maximum_quantity:D
    minimum_notional:D
    percent_up:D
    percent_down:D
    maximum_orders:int
    maximum_algo_orders:int|None
    entry_allowed:bool
    source_digest:str

    def rules(self,key,*,brackets,symbol_config,mark,now_ms):
        c=self.metadata
        if not isinstance(key,PositionKey) or key.symbol!=c.symbol or not isinstance(brackets,LeverageBrackets) or brackets.symbol!=c.symbol:
            raise InvalidState('公开合约规则与账户风险档位不匹配')
        if (type(now_ms) is not int or not 0<=now_ms-c.known_ms<=300000
                or not 0<=brackets.query_ms<=brackets.received_ms<=now_ms or now_ms-brackets.query_ms>3600000):
            raise InvalidState('公开合约规则或账户风险档位过期、尚未收到')
        q=symbol_config
        if (type(q) is not dict or q.get('symbol')!=c.symbol or type(q.get('query_ms')) is not int
                or type(q.get('received_ms')) is not int or not 0<=q['query_ms']<=q['received_ms']<=now_ms
                or now_ms-q['query_ms']>3600000):raise InvalidState('合约名义限额缺少实际账户配置来源')
        mark_values={}
        if mark is not None:
            if (not isinstance(mark,MarkPrice) or mark.symbol!=c.symbol or not 0<=mark.event_ms<=mark.received_ms<=now_ms
                    or now_ms-mark.event_ms>5000):raise InvalidState('开仓价格限制缺少新鲜标记价格')
            mark_values=dict(percent_up=self.percent_up,percent_down=self.percent_down,mark_price=mark.price,
                mark_event_ms=mark.event_ms,mark_received_ms=mark.received_ms)
        # An absent mark cannot authorize new risk. Quantity/tick rules still
        # support risk-reducing work; entry has a separate allow flag.
        return ContractRules(key,self.step,self.minimum_quantity,self.maximum_quantity,self.minimum_notional,
            positive(q.get('maximum_notional')),self.tick,brackets.maximum_leverage,
            min(c.known_ms,brackets.query_ms,q['query_ms']),tradable=c.tradable(now_ms=now_ms),
            minimum_price=self.minimum_price,maximum_price=self.maximum_price,
            entry_allowed=self.entry_allowed,requires_mark=True,maximum_orders=self.maximum_orders,
            source_digest=digest(dict(exchange=self.source_digest,brackets=brackets.source_digest,configuration=q,
                mark=mark.source_digest if mark is not None else None)),**mark_values)


def parse_execution_filters(reply):
    receipt(reply);universe=parse_exchange_info(reply)
    metadata={c.symbol:c for c in universe.contracts if c.category!='other'};result={}
    required={'PRICE_FILTER','LOT_SIZE','MARKET_LOT_SIZE','MIN_NOTIONAL','PERCENT_PRICE','MAX_NUM_ORDERS'}
    allowed=required|{'MAX_NUM_ALGO_ORDERS','POSITION_RISK_CONTROL'}
    try:
        for r in reply.body['symbols']:
            s=r['symbol']
            if s not in metadata:continue
            rows=r['filters']
            if type(rows) is not list or any(type(f) is not dict or type(f.get('filterType')) is not str for f in rows):
                raise InvalidState('合约过滤器格式错误')
            f={x['filterType']:x for x in rows}
            if len(f)!=len(rows) or not required<=set(f) or set(f)-allowed:raise InvalidState('合约过滤器重复、缺失或新增类型尚未支持')
            price=f['PRICE_FILTER'];lo=nonnegative(price['minPrice']);hi=nonnegative(price['maxPrice']);tick=positive(price['tickSize'])
            if hi and hi<lo:raise InvalidState('合约价格范围矛盾')
            lot,market=f['LOT_SIZE'],f['MARKET_LOT_SIZE']
            left,right=positive(lot['stepSize']),positive(market['stepSize'])
            minimum=max(positive(lot['minQty']),positive(market['minQty']))
            maximum=min(positive(lot['maxQty']),positive(market['maxQty']));step=common_step(left,right)
            if minimum>maximum or any(positive(x['minQty'])%positive(x['stepSize']) for x in (lot,market)):
                raise InvalidState('限价与市价数量范围不相容或不支持偏移数量网格')
            up,down=positive(f['PERCENT_PRICE']['multiplierUp']),positive(f['PERCENT_PRICE']['multiplierDown'])
            if not down<=1<=up:raise InvalidState('价格振幅限制矛盾')
            count=f['MAX_NUM_ORDERS']['limit'];algos=f.get('MAX_NUM_ALGO_ORDERS',{}).get('limit')
            if type(count) is not int or count<1 or algos is not None and (type(algos) is not int or algos<1):raise InvalidState('合约挂单数量限制错误')
            types=r['orderTypes'];tif=r['timeInForce']
            if type(types) is not list or type(tif) is not list or not {'LIMIT','MARKET','STOP_MARKET','TAKE_PROFIT_MARKET'}<=set(types) or not {'GTC','IOC'}<=set(tif):
                raise InvalidState('合约不支持策略所需的订单类型与有效方式')
            control=f.get('POSITION_RISK_CONTROL',{}).get('positionControlSide','NONE')
            if type(control) is not str or not control:raise InvalidState('合约持仓风险限制字段错误')
            result[s]=SymbolFilters(metadata[s],lo,hi or None,tick,step,minimum,maximum,
                positive(f['MIN_NOTIONAL']['notional']),up,down,count,algos,control=='NONE',digest(r))
    except (KeyError,TypeError):raise InvalidState('合约执行规则字段不完整') from None
    return result

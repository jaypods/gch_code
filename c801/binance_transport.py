"""Fixed-origin USD-M REST transport plus public-only spot klines.

Owner-run service only. No order/retry/ownership decisions live in this layer.
POST/DELETE are attempted exactly once; even an HTTP error is not a terminal
order report. BrokerGateway commits an execution transition before each call.
Official request shapes checked 2026-11-09 against Binance's REST Trade and
General Info documentation. No credentials, signed URLs or response messages
are included in returned errors or audit metadata.
"""
from dataclasses import dataclass, field
from collections import deque
import hashlib
import hmac
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from .account import InvalidState, PositionKey, positive
from .credentials import SecretPair
from .wire_handoff import PrewireExpired


ROUTES = {
    'QUERY_ORDER': ('GET', '/fapi/v1/order'),
    'SEND_ORDER': ('POST', '/fapi/v1/order'),
    'CANCEL_ORDER': ('DELETE', '/fapi/v1/order'),
    'QUERY_PROTECTION': ('GET', '/fapi/v1/algoOrder'),
    'SEND_PROTECTION': ('POST', '/fapi/v1/algoOrder'),
    'CANCEL_PROTECTION': ('DELETE', '/fapi/v1/algoOrder'),
    'QUERY_SYMBOL_CONFIG': ('GET', '/fapi/v1/symbolConfig'),
    'CHANGE_LEVERAGE': ('POST', '/fapi/v1/leverage'),
    'QUERY_ACCOUNT_CONFIG': ('GET', '/fapi/v1/accountConfig'),
    'QUERY_ACCOUNT': ('GET', '/fapi/v3/account'),
    'QUERY_POSITIONS': ('GET', '/fapi/v3/positionRisk'),
    'QUERY_OPEN_ORDERS': ('GET', '/fapi/v1/openOrders'),
    'QUERY_OPEN_PROTECTIONS': ('GET', '/fapi/v1/openAlgoOrders'),
    'QUERY_TRADES': ('GET', '/fapi/v1/userTrades'),
    'QUERY_INCOME': ('GET', '/fapi/v1/income'),
    'START_ACCOUNT_STREAM': ('POST', '/fapi/v1/listenKey'),
    'KEEP_ACCOUNT_STREAM': ('PUT', '/fapi/v1/listenKey'),
    'QUERY_MARKET_RULES': ('GET', '/fapi/v1/exchangeInfo'),
    'QUERY_CANDLES': ('GET', '/fapi/v1/klines'),
    'QUERY_SPOT_CANDLES': ('GET', '/api/v3/klines'),
    'QUERY_SPOT_MARKETS': ('GET', '/api/v3/exchangeInfo'),
    'QUERY_TRADING_SCHEDULE': ('GET', '/fapi/v1/tradingSchedule'),
    'QUERY_FUNDING_HISTORY': ('GET', '/fapi/v1/fundingRate'),
    'QUERY_FUNDING_INFO': ('GET', '/fapi/v1/fundingInfo'),
    'QUERY_BOOK_TICKER': ('GET', '/fapi/v1/ticker/bookTicker'),
    'QUERY_CONTRACT_PRICE': ('GET', '/fapi/v2/ticker/price'),
    'QUERY_MARK_PRICE': ('GET', '/fapi/v1/premiumIndex'),
    'QUERY_LEVERAGE_BRACKETS': ('GET', '/fapi/v1/leverageBracket'),
}
PUBLIC_OPERATIONS=frozenset({'QUERY_MARKET_RULES','QUERY_CANDLES','QUERY_SPOT_CANDLES','QUERY_SPOT_MARKETS','QUERY_TRADING_SCHEDULE',
    'QUERY_FUNDING_HISTORY','QUERY_FUNDING_INFO','QUERY_BOOK_TICKER','QUERY_CONTRACT_PRICE','QUERY_MARK_PRICE'})
SYMBOL_READS=frozenset({'QUERY_BOOK_TICKER','QUERY_CONTRACT_PRICE','QUERY_MARK_PRICE','QUERY_LEVERAGE_BRACKETS'})
CANDLE_PERIODS={'1m':60000,'5m':300000,'15m':900000,'1h':3600000,'4h':14400000,'1d':86400000}

# Stream creation/renewal changes notification lifetime only. It never enables
# orders or changes account funds. No DELETE: the key may be shared by another
# owner process. Closing our socket must not invalidate that process's stream.
STREAM_OPERATIONS = frozenset({'START_ACCOUNT_STREAM', 'KEEP_ACCOUNT_STREAM'})
EMPTY_OPERATIONS = STREAM_OPERATIONS | frozenset({'QUERY_ACCOUNT_CONFIG', 'QUERY_ACCOUNT',
    'QUERY_POSITIONS', 'QUERY_OPEN_ORDERS', 'QUERY_OPEN_PROTECTIONS','QUERY_MARKET_RULES','QUERY_TRADING_SCHEDULE','QUERY_FUNDING_INFO'})
HISTORY_OPERATIONS = frozenset({'QUERY_TRADES', 'QUERY_INCOME'})
LIST_PATHS = frozenset({'/fapi/v1/symbolConfig', '/fapi/v1/openOrders', '/fapi/v1/openAlgoOrders',
    '/fapi/v3/positionRisk', '/fapi/v1/userTrades', '/fapi/v1/income','/fapi/v1/klines','/fapi/v1/fundingRate','/fapi/v1/fundingInfo','/api/v3/klines'})
WEIGHTS = {path: 1 for method, path in ROUTES.values()}
WEIGHTS.update({'/fapi/v1/time': 1, '/fapi/v1/accountConfig': 5, '/fapi/v3/account': 5,
    '/fapi/v3/positionRisk': 5,
    '/fapi/v1/symbolConfig': 5, '/fapi/v1/openOrders': 40, '/fapi/v1/openAlgoOrders': 40,
    '/fapi/v1/userTrades': 5, '/fapi/v1/income': 30,'/fapi/v1/tradingSchedule':5,'/api/v3/klines':2,'/api/v3/exchangeInfo':20})
WEIGHTS['/fapi/v1/ticker/bookTicker']=2
DUAL_PATHS=frozenset({'/fapi/v1/leverageBracket'})
UNRELIABLE_WEIGHT_PATHS=frozenset({'/fapi/v1/ticker/bookTicker','/fapi/v2/ticker/price'})


class TransportError(RuntimeError):
    def __init__(self, reason, *, http_status=None, exchange_code=None, retry_after_ms=0):
        self.reason = reason
        self.http_status = http_status
        self.exchange_code = exchange_code
        self.retry_after_ms = retry_after_ms
        super().__init__(f'{reason}; HTTP={http_status}; code={exchange_code}')

    def summary(self):
        return dict(reason=self.reason, http_status=self.http_status,
                    exchange_code=self.exchange_code, retry_after_ms=self.retry_after_ms)


@dataclass(frozen=True)
class WireReply:
    body: object = field(repr=False)
    sent_ms: int
    received_ms: int
    http_status: int = 200


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise TransportError('REDIRECT_REFUSED')


def validate_parameters(operation, params):
    if operation not in ROUTES or type(params) is not dict:
        raise InvalidState('通信操作或参数格式不受支持')
    if any(not isinstance(k, str) or type(v) not in (str, int) or len(str(v)) > 128 for k, v in params.items()):
        raise InvalidState('通信参数必须是有限的明确标量')
    if operation in SYMBOL_READS:
        if set(params)!={'symbol'}:raise InvalidState('报价与风险档位读取需要明确的单一合约')
        PositionKey(params['symbol'],'BOTH')
        return dict(params)
    if operation=='QUERY_SPOT_MARKETS':
        if params!=dict(permissions='SPOT',showPermissionSets='false',symbolStatus='TRADING'):
            raise InvalidState('现货目录读取必须是完整可交易现货范围')
        return dict(params)
    if operation in EMPTY_OPERATIONS:
        if params:
            raise InvalidState('全账户读取或通知订阅不接受附加参数')
        return {}
    if operation in ('QUERY_CANDLES','QUERY_SPOT_CANDLES','QUERY_FUNDING_HISTORY'):
        candles=operation in ('QUERY_CANDLES','QUERY_SPOT_CANDLES')
        required={'symbol','startTime','endTime','limit'} | ({'interval'} if candles else set())
        if params.keys()!=required:raise InvalidState('公开行情读取需要明确的合约、周期和完整时间范围')
        PositionKey(params['symbol'],'BOTH')
        begin,end,limit=(params[k] for k in ('startTime','endTime','limit'))
        maximum=1500 if operation=='QUERY_CANDLES' else 1000
        if any(type(v) is not int for v in (begin,end,limit)) or not 0<=begin<=end or not 1<=limit<=maximum:
            raise InvalidState('公开行情范围或条数错误')
        if candles:
            period=CANDLE_PERIODS.get(params['interval'])
            if period is None or begin%period or (end+1)%period or (end+1-begin)//period>limit:
                raise InvalidState('K 线范围未对齐或超过单页容量')
        elif end-begin>31*86400000:raise InvalidState('资金费查询窗口超过一个月，需要分页')
        return dict(params)
    if operation in HISTORY_OPERATIONS:
        required = {'startTime', 'endTime', 'limit'} | ({'symbol'} if operation == 'QUERY_TRADES' else set())
        if params.keys() != required:
            raise InvalidState('历史读取需要明确的完整时间窗口')
        begin, end, limit = (params[k] for k in ('startTime', 'endTime', 'limit'))
        if any(type(x) is not int for x in (begin, end, limit)) or not 0 <= begin <= end or end - begin > 7 * 86400000 or not 1 <= limit <= 1000:
            raise InvalidState('历史读取时间或条数越界')
        if 'symbol' in params:
            PositionKey(params['symbol'], 'BOTH')
        return dict(params)
    if operation in ('QUERY_SYMBOL_CONFIG', 'CHANGE_LEVERAGE'):
        required = {'symbol'} | ({'leverage'} if operation == 'CHANGE_LEVERAGE' else set())
        if params.keys() != required:
            raise InvalidState('账户合约配置只接受明确的单一合约参数')
        PositionKey(params['symbol'], 'BOTH')
        if operation == 'CHANGE_LEVERAGE' and (type(params['leverage']) is not int or not 1 <= params['leverage'] <= 125):
            raise InvalidState('账户杠杆参数无效')
        return dict(params)
    if operation in ('QUERY_ORDER', 'CANCEL_ORDER'):
        allowed = {'symbol', 'orderId', 'origClientOrderId'}
        required = {'symbol'}
        identities = {'orderId', 'origClientOrderId'}
    elif operation in ('QUERY_PROTECTION', 'CANCEL_PROTECTION'):
        allowed = identities = {'algoId', 'clientAlgoId'}
        required = set()
    elif operation == 'SEND_ORDER':
        required = {'symbol', 'positionSide', 'side', 'type', 'quantity', 'newClientOrderId'}
        allowed = required | {'reduceOnly', 'newOrderRespType', 'price', 'timeInForce'}
        identities = {'newClientOrderId'}
        if params.get('type') == 'LIMIT':
            required |= {'price', 'timeInForce'}
            if params.get('timeInForce') not in ('GTC', 'IOC'):
                raise InvalidState('限价有效方式不受支持')
        elif params.get('type') != 'MARKET' or {'price', 'timeInForce'} & params.keys():
            raise InvalidState('普通委托类型或价格字段不受支持')
        if params.get('newOrderRespType', 'ACK') not in ('ACK', 'RESULT'):
            raise InvalidState('委托响应方式错误')
    else:
        required = {'algoType', 'symbol', 'positionSide', 'side', 'type', 'quantity',
                    'triggerPrice', 'workingType', 'priceProtect', 'closePosition', 'clientAlgoId'}
        allowed = required | {'reduceOnly'}
        identities = {'clientAlgoId'}
        if (params.get('algoType'), params.get('workingType'), params.get('priceProtect'),
                params.get('closePosition')) != ('CONDITIONAL', 'CONTRACT_PRICE', 'false', 'false'):
            raise InvalidState('保护单触发与平仓范围不符合已实现策略')
        if params.get('type') not in ('STOP_MARKET', 'TAKE_PROFIT_MARKET'):
            raise InvalidState('保护委托类型不受支持')
    if not required <= params.keys() or params.keys() - allowed or not identities & params.keys():
        raise InvalidState('通信缺少必要字段或包含未授权字段')
    if 'symbol' in params:
        PositionKey(params['symbol'], params.get('positionSide', 'BOTH'))
    for key in ('orderId', 'algoId'):
        if key in params and (not re.fullmatch(r'[0-9]{1,20}', str(params[key])) or int(params[key]) <= 0):
            raise InvalidState('交易所订单标识错误')
    for key in ('origClientOrderId', 'clientAlgoId', 'newClientOrderId'):
        if key in params and (type(params[key]) is not str or not re.fullmatch(r'[.A-Za-z0-9:/_-]{1,36}', params[key])):
            raise InvalidState('客户委托标识错误')
    for key in ('quantity', 'price', 'triggerPrice'):
        if key in params:
            positive(params[key])
    if operation.startswith('SEND_'):
        if params.get('side') not in ('BUY', 'SELL'):
            raise InvalidState('买卖方向错误')
        if 'reduceOnly' in params and (params['reduceOnly'] != 'true' or params['positionSide'] != 'BOTH'):
            raise InvalidState('只减仓标记与账户持仓模式冲突')
    return dict(params)


class BinanceTransport:
    BASE = 'https://fapi.binance.com'
    MAX_BYTES = 1_000_000

    def __init__(self, credentials: SecretPair | None = None, *, mutations_enabled=False, opener=None,spot_opener=None,
                 monotonic=None, minimum_interval_ms=100, clock_max_age_ms=60_000,
                 maximum_weight_per_minute=1800, maximum_orders_per_minute=100, maximum_orders_per_ten_seconds=20):
        if credentials is not None and not isinstance(credentials, SecretPair) or type(mutations_enabled) is not bool or mutations_enabled and credentials is None:
            raise InvalidState('通信需要本机凭据与明确的实盘开关')
        if any(type(v) is not int or v < 0 for v in (minimum_interval_ms, clock_max_age_ms)) or not clock_max_age_ms:
            raise InvalidState('通信时间限制无效')
        if any(type(v) is not int or not 1 <= v <= cap for v,cap in ((maximum_weight_per_minute,1800),
                (maximum_orders_per_minute,100),(maximum_orders_per_ten_seconds,20))):
            raise InvalidState('通信额度必须在保守本机上限以内')
        self._credentials = credentials
        self.mutations_enabled = mutations_enabled
        if opener is None:
            from .persistent_http import PersistentHTTPS
            opener=PersistentHTTPS()
        self._opener = opener
        self._spot_opener=spot_opener
        self._mono = monotonic or time.monotonic_ns
        self._base_ms = self._base_mono = None
        self._next_mono = 0
        self._disabled = False
        self.minimum_interval_ms = minimum_interval_ms
        self.clock_max_age_ms = clock_max_age_ms
        self.maximum_weight_per_minute = maximum_weight_per_minute
        self.maximum_orders_per_minute = maximum_orders_per_minute
        self.maximum_orders_per_ten_seconds = maximum_orders_per_ten_seconds
        self._weights = deque(); self._orders = deque(); self._reported_limits = {}
        self._funding = deque()
        self._network_attempts = 0

    @property
    def network_attempts(self):
        """Actual HTTP attempts, including failures and clock synchronization."""
        return self._network_attempts

    @property
    def clock_age_ms(self):
        return None if self._base_mono is None else (self._mono()-self._base_mono)//1_000_000

    def close(self):
        if hasattr(self._opener,'close'):self._opener.close()
        if hasattr(self._spot_opener,'close'):self._spot_opener.close()
        self._disabled=True

    def _budget(self, method, path, *, consume=False, params=None):
        now=self._mono(); minute=60_000_000_000; cost=WEIGHTS[path]
        scope='spot' if path.startswith('/api/v3/') else 'futures'
        if path=='/fapi/v1/klines':
            limit=params['limit'];cost=1 if limit<100 else 2 if limit<500 else 5 if limit<=1000 else 10
        while self._weights and now-self._weights[0][0]>=minute:self._weights.popleft()
        while self._orders and now-self._orders[0]>=minute:self._orders.popleft()
        waits=[]
        funding=path in ('/fapi/v1/fundingRate','/fapi/v1/fundingInfo')
        while self._funding and now-self._funding[0]>=5*minute:self._funding.popleft()
        # A separate conservative shared cap, in addition to request weights.
        if funding and len(self._funding)>=400:waits.append(self._funding[-400]+5*minute-now)
        if cost>self.maximum_weight_per_minute:
            raise TransportError('REQUEST_EXCEEDS_CONFIGURED_WEIGHT_BUDGET')
        total=sum(w for _,w in self._weights)
        if total+cost>self.maximum_weight_per_minute:
            for at,weight in self._weights:
                total-=weight
                if total+cost<=self.maximum_weight_per_minute:
                    waits.append(at+minute-now);break
        sending=method=='POST' and path in ('/fapi/v1/order','/fapi/v1/algoOrder')
        if sending:
            if len(self._orders)>=self.maximum_orders_per_minute:
                waits.append(self._orders[-self.maximum_orders_per_minute]+minute-now)
            recent=[x for x in self._orders if now-x<10_000_000_000]
            if len(recent)>=self.maximum_orders_per_ten_seconds:
                waits.append(recent[-self.maximum_orders_per_ten_seconds]+10_000_000_000-now)
        for (reported_scope,kind),(used,until) in self._reported_limits.items():
            if reported_scope!=scope:continue
            limit=self.maximum_weight_per_minute if kind=='weight' else self.maximum_orders_per_minute if kind=='orders_1m' else self.maximum_orders_per_ten_seconds
            needed=cost if kind=='weight' else int(sending)
            if needed and now<until and used+needed>limit:waits.append(until-now)
        if waits:
            raise TransportError('REQUEST_BUDGET_COOLDOWN',retry_after_ms=max(1,(max(waits)+999999)//1000000))
        if consume:
            self._weights.append((now,cost))
            if funding:self._funding.append(now)
            if sending:self._orders.append(now)
            for (reported_scope,kind),(used,until) in list(self._reported_limits.items()):
                if reported_scope==scope and now<until:self._reported_limits[reported_scope,kind]=(used+(cost if kind=='weight' else int(sending)),until)

    def _observe_budget_headers(self, headers,*,scope='futures',ignore_weight=False):
        if headers is None:return
        values={str(k).lower():str(v) for k,v in headers.items()}
        for name,kind,period in (('x-mbx-used-weight-1m','weight',60000),
                ('x-mbx-order-count-1m','orders_1m',60000),('x-mbx-order-count-10s','orders_10s',10000)):
            if kind=='weight' and ignore_weight:continue
            raw=values.get(name,'')
            if not re.fullmatch(r'[0-9]{1,10}',raw):continue
            # Headers include other software on the same account/IP. Keep a
            # conservative boundary cushion rather than resetting on a local
            # request count or assuming our process is the only API client.
            delay=period if self._base_ms is None else period-self.now_ms()%period+500
            self._reported_limits[scope,kind]=(int(raw),self._mono()+delay*1_000_000)

    def now_ms(self):
        if self._base_ms is None:
            raise TransportError('CLOCK_NOT_SYNCHRONIZED')
        elapsed = (self._mono() - self._base_mono) // 1_000_000
        if elapsed < 0:
            raise TransportError('MONOTONIC_CLOCK_REVERSED')
        return self._base_ms + elapsed

    def _admission(self, *, synchronized=True):
        now = self._mono()
        if self._disabled:
            raise TransportError('CONNECTION_REQUIRES_OWNER_REVIEW')
        if now < self._next_mono:
            raise TransportError('REQUEST_COOLDOWN', retry_after_ms=(self._next_mono - now + 999_999) // 1_000_000)
        if synchronized:
            self.now_ms()
            if (now - self._base_mono) // 1_000_000 > self.clock_max_age_ms:
                raise TransportError('CLOCK_CALIBRATION_EXPIRED')

    def preflight(self, operation, params):
        validate_parameters(operation, params)
        if operation not in PUBLIC_OPERATIONS and self._credentials is None:
            raise TransportError('LOCAL_CREDENTIALS_REQUIRED')
        if ROUTES[operation][0] != 'GET' and operation not in STREAM_OPERATIONS and not self.mutations_enabled:
            raise TransportError('LIVE_SWITCH_OFF')
        self._admission()
        self._budget(*ROUTES[operation],params=params)
        if operation in HISTORY_OPERATIONS and (params['endTime'] >= self.now_ms() or self.now_ms() - params['startTime'] > 89 * 86400000):
            raise InvalidState('历史窗口尚未结束或超出保守保留期')
        if operation in ('QUERY_CANDLES','QUERY_SPOT_CANDLES','QUERY_FUNDING_HISTORY') and params['endTime']>=self.now_ms():
            raise InvalidState('行情查询范围尚未结束')

    def sync_clock(self):
        self._admission(synchronized=False)
        start = self._mono()
        data, _ = self._exchange('GET', '/fapi/v1/time', {}, signed=False)
        finish = self._mono()
        if type(data) is not dict or type(data.get('serverTime')) is not int or data['serverTime'] <= 0:
            raise TransportError('INVALID_SERVER_CLOCK')
        if not 0 <= finish - start <= 500_000_000:
            raise TransportError('CLOCK_ROUND_TRIP_TOO_WIDE')
        # The server timestamp is conservatively anchored at actual receipt.
        # RTT <= 500ms bounds the possible lag; never invent a future timestamp.
        proposed = data['serverTime']
        if self._base_ms is not None:
            elapsed=(finish-self._base_mono)//1_000_000
            if elapsed<0:
                raise TransportError('MONOTONIC_CLOCK_REVERSED')
            at_receipt=self._base_ms+elapsed
            if proposed<at_receipt:
                if at_receipt-proposed>500:
                    raise TransportError('SERVER_CLOCK_REVERSED')
                # Compare both anchors at the SAME captured receipt instant.
                # Reading now_ms here and anchoring that later value at finish
                # would add processing time again on every successful refresh.
                proposed=at_receipt
        self._base_ms, self._base_mono = proposed, finish
        return self.now_ms()

    def request(self, operation, params, *, before_attempt=None):
        self.preflight(operation, params)
        sent = None
        def at_wire():
            nonlocal sent
            if before_attempt is not None:before_attempt()
            sent = self.now_ms()
        method, path = ROUTES[operation]
        stream = operation in STREAM_OPERATIONS
        data, status = self._exchange(method, path, dict(params), signed=not stream and operation not in PUBLIC_OPERATIONS, api_key=stream,before_attempt=at_wire)
        if sent is None:raise TransportError('REQUEST_ATTEMPT_NOT_OBSERVED')
        return WireReply(data, sent, self.now_ms(), status)

    def _exchange(self, method, path, params, *, signed, api_key=False, before_attempt=None):
        spot=path.startswith('/api/v3/');scope='spot' if spot else 'futures'
        if spot and (method!='GET' or signed or api_key):raise InvalidState('现货行情接口拒绝凭据或写操作')
        origin='https://api.binance.com' if spot else self.BASE
        if spot and self._spot_opener is None:
            from .persistent_http import PersistentHTTPS
            self._spot_opener=PersistentHTTPS(origin=origin)
        opener=self._spot_opener if spot else self._opener
        self._budget(method,path,consume=True,params=params)
        headers = {'User-Agent': 'C801-owner-service/0.4', 'Accept': 'application/json'}
        if signed:
            params.update(timestamp=self.now_ms(), recvWindow=5000)
        encoded = urllib.parse.urlencode(params)
        if signed:
            sig = hmac.new(self._credentials.api_secret.encode(), encoded.encode(), hashlib.sha256).hexdigest()
            encoded += '&signature=' + sig
        if signed or api_key:
            headers['X-MBX-APIKEY'] = self._credentials.api_key
        url = origin + path
        payload = None
        if method in ('POST', 'PUT'):
            # An unsigned stream start/keepalive has no fields. requests uses
            # None for its zero-byte body; represent it identically before
            # the exact signed-byte check in the persistent adapter.
            payload = encoded.encode() if encoded else None
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
        elif encoded:
            url += '?' + encoded
        request = urllib.request.Request(url, data=payload, headers=headers, method=method)
        self._next_mono = self._mono() + self.minimum_interval_ms * 1_000_000
        def attempt():
            if before_attempt is not None:before_attempt()
            self._network_attempts += 1
        try:
            # This is the only write. No retry, redirect or fallback origin.
            from .persistent_http import PersistentHTTPS
            if isinstance(opener,PersistentHTTPS):
                response=opener.open(request,timeout=5,before_attempt=attempt)
            else:
                attempt()
                response=opener.open(request,timeout=5)
            with response:
                self._observe_budget_headers(getattr(response,'headers',None),scope=scope,ignore_weight=path in UNRELIABLE_WEIGHT_PATHS)
                status = response.status
                if not 200 <= status < 300 or response.geturl().split('?', 1)[0] != origin + path:
                    raise TransportError('UNEXPECTED_RESPONSE_ORIGIN_OR_STATUS', http_status=status)
                maximum=8_000_000 if path in ('/fapi/v1/exchangeInfo','/api/v3/exchangeInfo') else self.MAX_BYTES
                raw = response.read(maximum + 1)
                if len(raw) > maximum:
                    raise TransportError('RESPONSE_TOO_LARGE', http_status=status)
                data = json.loads(raw)
                if path in LIST_PATHS|DUAL_PATHS and method == 'GET' and type(data) is list:
                    return data, status
                if type(data) is not dict:
                    raise TransportError('INVALID_RESPONSE_SHAPE', http_status=status)
                code = data.get('code')
                if type(code) is int and code < 0:
                    raise TransportError('EXCHANGE_REJECTED_REQUEST', http_status=status, exchange_code=code)
                if path in LIST_PATHS:
                    raise TransportError('INVALID_RESPONSE_SHAPE', http_status=status)
                return data, status
        except urllib.error.HTTPError as exc:
            self._observe_budget_headers(exc.headers,scope=scope,ignore_weight=path in UNRELIABLE_WEIGHT_PATHS)
            code = None
            try:
                data = json.loads(exc.read(8192))
                if type(data) is dict and type(data.get('code')) is int:
                    code = data['code']
            except Exception:
                pass
            delay = 0
            if exc.code in (418, 429):
                # Missing/invalid Retry-After never creates a tight retry loop.
                delay = 60_000 if exc.code == 429 else 120_000
                header = exc.headers.get('Retry-After', '') if exc.headers else ''
                if re.fullmatch(r'[0-9]{1,7}', header):
                    delay = max(delay, int(header) * 1000)
                self._next_mono = max(self._next_mono, self._mono() + delay * 1_000_000)
            if exc.code in (401, 403, 451):
                self._disabled = True
            raise TransportError('HTTP_REQUEST_FAILED', http_status=exc.code,
                                 exchange_code=code, retry_after_ms=delay) from None
        except (TransportError, PrewireExpired):
            raise
        except OSError as exc:
            reason=exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else None
            if reason in ('HTTP_CONNECTION_CLOSED','HTTP_ORIGIN_OR_METHOD_REFUSED','HTTP_ENCODING_CHANGED'):
                raise TransportError(reason) from None
            raise TransportError('NETWORK_OR_RESPONSE_FAILED') from None
        except Exception:
            # DNS, TLS, timeout, decoding and vendor messages stay out of logs.
            raise TransportError('NETWORK_OR_RESPONSE_FAILED') from None

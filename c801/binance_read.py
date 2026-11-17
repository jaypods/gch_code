"""Strict GET-only account observation. No test-order or live-order endpoint.

Official interface reference checked against Binance's maintained connector:
https://github.com/binance/binance-connector-python/tree/master/clients/derivatives_trading_usds_futures

A multi-request REST observation is not an atomic reconciled trading snapshot.
It remains complete=False until the event-stream reconciler certifies it.
"""
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from .account import AccountSnapshot, InvalidState, Position, PositionKey, WorkingOrder, number
from .credentials import SecretPair


class ApiReadError(RuntimeError):
    def __init__(self, reason, http_status=None, exchange_code=None):
        self.reason, self.http_status, self.exchange_code = reason, http_status, exchange_code
        super().__init__(f"{reason}; HTTP={http_status}; code={exchange_code}")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ApiReadError("拒绝把账户请求重定向到其他地址")


def strict_bool(value):
    if type(value) is not bool:
        raise InvalidState("交易所布尔字段缺失或类型错误")
    return value


@dataclass(frozen=True)
class AccountObservation:
    snapshot: AccountSnapshot
    non_usdt_positions: int
    non_usdt_orders: int
    requests_completed: tuple[str, ...]

    def summary(self):
        s = self.snapshot
        return {"read_only": True, "real_orders_sent": 0, "requests_completed": len(self.requests_completed),
                "hedge_mode": s.hedge_mode, "can_trade": s.can_trade, "multi_asset_mode": s.multi_asset_mode,
                "usdt_equity_positive": s.equity > 0, "usdt_available_positive": s.available > 0,
                "nonzero_positions": sum(bool(p.amount) for p in s.positions),
                "working_orders": len(s.orders), "non_usdt_positions": self.non_usdt_positions,
                "non_usdt_orders": self.non_usdt_orders, "reconciled_for_trading": s.complete,
                "live_enabled": False}


class ReadOnlyBinance:
    BASE = "https://fapi.binance.com"
    PATHS = frozenset({"/fapi/v1/time", "/fapi/v1/accountConfig", "/fapi/v3/account",
                       "/fapi/v3/positionRisk", "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders",
                       "/fapi/v1/userTrades", "/fapi/v1/income"})

    def __init__(self, credentials: SecretPair, *, opener=None, monotonic=None):
        self._credentials = credentials
        self._opener = opener or urllib.request.build_opener(NoRedirect())
        self._clock = monotonic or time.monotonic_ns
        self._base_ms = None
        self._base_mono = None
        self._halted = False
        self.completed = []

    def now_ms(self):
        if self._base_ms is None:
            raise ApiReadError("服务器时钟尚未校准")
        return self._base_ms + (self._clock() - self._base_mono) // 1_000_000

    def get(self, path, params=None):
        if self._halted or path not in self.PATHS:
            raise ApiReadError("只读接口未授权或连接已停止")
        params={} if params is None else params
        if type(params) is not dict:raise ApiReadError('只读请求参数格式错误')
        if path in ('/fapi/v1/userTrades','/fapi/v1/income'):
            required={'startTime','endTime','limit'}|({'symbol'} if path.endswith('userTrades') else set())
            if set(params)!=required:raise ApiReadError('历史只读请求必须使用明确的完整时间窗口')
            begin,end,limit=params['startTime'],params['endTime'],params['limit']
            if any(type(v) is not int for v in (begin,end,limit)) or not 0<=begin<=end or end-begin>7*86400000 or not 1<=limit<=1000:
                raise ApiReadError('历史只读请求超出时间范围或条数限制')
            if 'symbol' in params:PositionKey(params['symbol'],'BOTH')
        elif params:raise ApiReadError('该只读接口不接受附加参数')
        headers = {"User-Agent": "C801-account-observation/0.2"}
        query = ""
        if path != "/fapi/v1/time":
            timestamp = self.now_ms()
            if params and (params['endTime']>=timestamp or timestamp-params['startTime']>89*86400000):
                raise ApiReadError('历史窗口尚未结束或已超过保守保留期')
            query = urllib.parse.urlencode(params|{"timestamp": timestamp, "recvWindow": 5000})
            signature = hmac.new(self._credentials.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += "&signature=" + signature
            headers["X-MBX-APIKEY"] = self._credentials.api_key
        request = urllib.request.Request(self.BASE + path + ("?" + query if query else ""), headers=headers, method="GET")
        try:
            with self._opener.open(request, timeout=12) as response:
                if response.geturl().split("?", 1)[0] != self.BASE + path:
                    raise ApiReadError("账户响应地址不符合固定官方接口")
                raw = response.read(4_000_001)
                if len(raw) > 4_000_000:
                    raise ApiReadError("账户响应过大，无法确认完整性")
                result = json.loads(raw)
            if isinstance(result, dict) and isinstance(result.get("code"), int) and result["code"] < 0:
                raise ApiReadError("交易所拒绝只读请求", 200, result["code"])
            if path == "/fapi/v1/time":
                server = result["serverTime"]
                if type(server) is not int or server <= 0:
                    raise ApiReadError("服务器时间格式错误")
                self._base_ms, self._base_mono = server, self._clock()
            self.completed.append(path)
            return result
        except urllib.error.HTTPError as exc:
            self._halted = True
            code = None
            try:
                failure = json.loads(exc.read(8192))
                if isinstance(failure.get("code"), int): code = failure["code"]
            except Exception:
                pass
            # Raw messages/URLs can contain credentials; report numeric codes only.
            raise ApiReadError("交易所拒绝只读请求，已停止", exc.code, code) from None
        except ApiReadError:
            self._halted = True
            raise
        except Exception:
            self._halted = True
            raise ApiReadError("只读网络请求或响应解析失败，已停止") from None

    def observe(self):
        self.get("/fapi/v1/time")
        started = self.now_ms()
        config = self.get("/fapi/v1/accountConfig")
        account = self.get("/fapi/v3/account")
        positions = self.get("/fapi/v3/positionRisk")
        orders = self.get("/fapi/v1/openOrders")
        algo_orders = self.get("/fapi/v1/openAlgoOrders")
        return parse_observation(config, account, positions, orders, algo_orders,
                                 exchange_ms=started, received_ms=self.now_ms(), completed=tuple(self.completed))


def parse_observation(config, account, positions, orders, algo_orders, *, exchange_ms, received_ms, completed=()):
    try:
        if not all(isinstance(rows, list) for rows in (positions, orders, algo_orders, account["assets"])):
            raise InvalidState("账户响应缺少完整列表")
        assets = [a for a in account["assets"] if a.get("asset") == "USDT"]
        if len(assets) != 1:
            raise InvalidState("无法唯一确定 USDT 账户")
        asset = assets[0]
        native = []
        foreign_positions = 0
        for row in positions:
            quantity = number(row["positionAmt"])
            if quantity == 0:
                continue
            if row["marginAsset"] != "USDT":
                foreign_positions += 1
                continue
            native.append(Position(PositionKey(row["symbol"], row["positionSide"]), quantity,
                                   number(row["positionInitialMargin"]), number(row["maintMargin"]),
                                   abs(number(row["notional"])), number(row["unRealizedProfit"])))
        working = []
        foreign_orders = 0
        for rows, algo in ((orders, False), (algo_orders, True)):
            for row in rows:
                if not row["symbol"].endswith("USDT"):
                    foreign_orders += 1
                    continue
                key = PositionKey(row["symbol"], row["positionSide"])
                if algo:
                    cid = row["clientAlgoId"] or "exchange_algo_" + str(row["algoId"])
                    # closePosition orders may legally have zero quantity.
                    qty = number(row["quantity"])
                    protective = strict_bool(row["reduceOnly"]) or strict_bool(row["closePosition"])
                else:
                    cid = row["clientOrderId"] or "exchange_order_" + str(row["orderId"])
                    qty = number(row["origQty"]) - number(row["executedQty"])
                    protective = strict_bool(row["reduceOnly"])
                working.append(WorkingOrder(cid, key, qty, None, protective))
        s = AccountSnapshot(version=str(uuid.uuid4()), exchange_ms=exchange_ms, received_ms=received_ms,
                            equity=number(asset["marginBalance"]), available=max(Decimal(0), number(asset["availableBalance"])),
                            initial_margin=number(asset["initialMargin"]), maintenance_margin=number(asset["maintMargin"]),
                            hedge_mode=strict_bool(config["dualSidePosition"]), can_trade=strict_bool(config["canTrade"]),
                            multi_asset_mode=strict_bool(config["multiAssetsMargin"]),
                            positions=tuple(native), orders=tuple(working), complete=False)
        return AccountObservation(s, foreign_positions, foreign_orders, completed)
    except (KeyError, TypeError, ValueError):
        raise InvalidState("账户响应缺字段或存在矛盾；不能用于新增交易") from None


def main():
    from .credentials import load_local_credentials
    try:
        observation = ReadOnlyBinance(load_local_credentials()).observe()
        print(json.dumps(observation.summary(), ensure_ascii=False))
        return 0
    except (ApiReadError, InvalidState) as exc:
        print(json.dumps({"passed": False, "reason": str(exc), "real_orders_sent": 0}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

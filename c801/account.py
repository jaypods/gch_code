"""Exchange observations and conservative allocation, without exchange mutations.

The initial equal allocation happens once. It is never recomputed from the wallet
on each tick: sleeve gains/losses stay in their sleeve, as required by C801.
Unknown positions/orders are reported and never silently adopted or liquidated.
"""
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from typing import Mapping


class InvalidState(ValueError):
    pass


def number(value) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception:
        raise InvalidState("金额或数量格式错误") from None
    if not result.is_finite():
        raise InvalidState("金额或数量不是有限数值")
    return result


def nonnegative(value) -> Decimal:
    result = number(value)
    if result < 0:
        raise InvalidState("金额或数量不能为负")
    return result


def positive(value) -> Decimal:
    result = number(value)
    if result <= 0:
        raise InvalidState("金额或数量必须大于零")
    return result


def sleeve_free_capital(cash, equity, held) -> Decimal:
    """Do not spend unrealized gains or capital already committed to orders."""
    return max(Decimal(0), min(number(cash), number(equity)) - nonnegative(held))


def margin_quantity_cap(price, margin_cap, leverage, fee_rate) -> Decimal:
    if type(leverage) is not int or leverage < 1:
        raise InvalidState('杠杆必须为正整数')
    return nonnegative(margin_cap) / (positive(price) * (Decimal(1) / leverage + nonnegative(fee_rate)))


SLEEVES = ("M784", "A787", "S800")


@dataclass(frozen=True, order=True)
class PositionKey:
    symbol: str
    side: str

    def __post_init__(self):
        # Exchange symbols are identifiers, including Chinese token names.
        # Preserve exact spelling; existence/tradability is checked against
        # exchange metadata. Never normalize distinct symbols into one key.
        if not isinstance(self.symbol,str) or not 4 < len(self.symbol) <= 80 or not self.symbol.isalnum() or not self.symbol.endswith("USDT"):
            raise InvalidState("合约标识错误")
        if self.side not in ("BOTH", "LONG", "SHORT"):
            raise InvalidState("持仓方向错误")


@dataclass(frozen=True)
class Position:
    key: PositionKey
    amount: Decimal  # Binance signed amount: SHORT is negative.
    initial_margin: Decimal
    maintenance_margin: Decimal
    notional: Decimal  # absolute notional, covering both LONG and SHORT.
    unrealized_pnl: Decimal

    def __post_init__(self):
        for field in ("amount", "unrealized_pnl"):
            object.__setattr__(self, field, number(getattr(self, field)))
        for field in ("initial_margin", "maintenance_margin", "notional"):
            object.__setattr__(self, field, nonnegative(getattr(self, field)))
        if self.key.side == "LONG" and self.amount < 0 or self.key.side == "SHORT" and self.amount > 0:
            raise InvalidState("持仓方向与交易所数量不一致")
        if self.amount and not self.notional:
            raise InvalidState("非零持仓缺少名义金额")


@dataclass(frozen=True)
class WorkingOrder:
    client_id: str
    key: PositionKey
    remaining: Decimal
    reserved_margin: Decimal | None  # None when only the account aggregate is known.
    protective: bool = False

    def __post_init__(self):
        if not self.client_id:
            raise InvalidState("挂单缺少唯一标识")
        object.__setattr__(self, "remaining", nonnegative(self.remaining))
        if self.reserved_margin is not None:
            object.__setattr__(self, "reserved_margin", nonnegative(self.reserved_margin))


@dataclass(frozen=True)
class AccountSnapshot:
    version: str
    exchange_ms: int
    received_ms: int
    equity: Decimal
    available: Decimal
    initial_margin: Decimal
    maintenance_margin: Decimal
    hedge_mode: bool
    can_trade: bool
    multi_asset_mode: bool
    positions: tuple[Position, ...] = ()
    orders: tuple[WorkingOrder, ...] = ()
    complete: bool = False  # includes reconciliation of orders, algo orders and positions.

    def __post_init__(self):
        if not self.version or not 0 <= self.exchange_ms <= self.received_ms:
            raise InvalidState("账户快照标识或时间错误")
        object.__setattr__(self, "equity", number(self.equity))
        for field in ("available", "initial_margin", "maintenance_margin"):
            object.__setattr__(self, field, nonnegative(getattr(self, field)))
        for field in ("hedge_mode", "can_trade", "multi_asset_mode", "complete"):
            if type(getattr(self, field)) is not bool:
                raise InvalidState("账户状态必须为明确的布尔值")
        object.__setattr__(self, "positions", tuple(self.positions))
        object.__setattr__(self, "orders", tuple(self.orders))
        keys = [p.key for p in self.positions]
        order_keys = [o.client_id for o in self.orders]
        if len(keys) != len(set(keys)) or len(order_keys) != len(set(order_keys)):
            raise InvalidState("账户快照有重复持仓或重复订单")
        if any((p.key.side == "BOTH") == self.hedge_mode for p in self.positions if p.amount):
            raise InvalidState("账户持仓模式与持仓方向不一致")

    def free_capital(self, reserve=Decimal(0)) -> Decimal:
        """Do not double-subtract margin from exchange 'available'. Use the minimum
        of two independent bounds to avoid trusting either balance in isolation.
        """
        reserve = nonnegative(reserve)
        return max(Decimal(0), min(self.available, self.equity - self.initial_margin) - reserve)

    def initial_allocation(self, reserve=Decimal(0)) -> dict[str, Decimal]:
        if not self.complete:
            raise InvalidState("账户快照未完成对账")
        total = self.free_capital(reserve)
        share = total / Decimal(3)
        # Preserve exactly the available capital despite Decimal recurring division.
        return {"M784": share, "A787": share, "S800": total - share * 2}


@dataclass(frozen=True)
class Ownership:
    """Expected exchange amounts rebuilt from verified fills, never ID prefixes.

    A caller cannot create ownership by merely observing an exchange position.
    The fill ledger supplies these records and is independently reconciled.
    """
    positions: Mapping[PositionKey, tuple[str, Decimal]]
    orders: Mapping[str, str]

    def reconcile(self, snapshot: AccountSnapshot) -> tuple[str, ...]:
        issues = []
        seen = {}
        for p in snapshot.positions:
            if not p.amount:
                continue
            seen[p.key] = p.amount
            own = self.positions.get(p.key)
            if own is None:
                issues.append("UNOWNED_POSITION:" + p.key.symbol + ":" + p.key.side)
            elif own[0] not in SLEEVES or number(own[1]) != p.amount:
                issues.append("POSITION_MISMATCH:" + p.key.symbol + ":" + p.key.side)
        for key, (owner, amount) in self.positions.items():
            if owner not in SLEEVES or number(amount) != seen.get(key, Decimal(0)):
                issue = "POSITION_MISMATCH:" + key.symbol + ":" + key.side
                if issue not in issues:
                    issues.append(issue)
        live_orders = {o.client_id: o for o in snapshot.orders}
        for client_id in live_orders:
            if self.orders.get(client_id) not in SLEEVES:
                issues.append("UNOWNED_ORDER:" + client_id)
        for client_id, owner in self.orders.items():
            if owner not in SLEEVES or client_id not in live_orders:
                issues.append("ORDER_MISMATCH:" + client_id)
        return tuple(sorted(issues))


@dataclass(frozen=True)
class ContractRules:
    key: PositionKey
    step: Decimal
    minimum_quantity: Decimal
    maximum_quantity: Decimal
    minimum_notional: Decimal
    maximum_notional: Decimal
    tick: Decimal
    maximum_leverage: int
    observed_ms: int
    tradable: bool = True
    minimum_price: Decimal = Decimal(0)
    maximum_price: Decimal | None = None
    entry_allowed: bool = True
    percent_up: Decimal | None = None
    percent_down: Decimal | None = None
    mark_price: Decimal | None = None
    mark_event_ms: int | None = None
    mark_received_ms: int | None = None
    maximum_orders: int | None = None
    source_digest: str | None = None
    requires_mark: bool = False

    def __post_init__(self):
        for field in ("step", "minimum_quantity", "maximum_quantity", "minimum_notional", "maximum_notional", "tick"):
            object.__setattr__(self, field, positive(getattr(self, field)))
        if self.minimum_quantity > self.maximum_quantity or self.minimum_notional > self.maximum_notional:
            raise InvalidState("合约上下限矛盾")
        if type(self.maximum_leverage) is not int or self.maximum_leverage < 1 or type(self.observed_ms) is not int or self.observed_ms < 0:
            raise InvalidState("杠杆或合约规则时间错误")
        if not isinstance(self.key,PositionKey) or any(type(x) is not bool for x in (self.tradable,self.entry_allowed,self.requires_mark)):
            raise InvalidState('合约规则身份或交易状态错误')
        object.__setattr__(self,'minimum_price',nonnegative(self.minimum_price))
        if self.maximum_price is not None:
            object.__setattr__(self,'maximum_price',positive(self.maximum_price))
            if self.maximum_price<self.minimum_price:raise InvalidState('合约价格上下限矛盾')
        group=(self.percent_up,self.percent_down,self.mark_price,self.mark_event_ms,self.mark_received_ms)
        if any(v is not None for v in group):
            if any(v is None for v in group):raise InvalidState('价格振幅限制缺少实际标记价格')
            for name in ('percent_up','percent_down','mark_price'):object.__setattr__(self,name,positive(getattr(self,name)))
            if not self.percent_down<=1<=self.percent_up:raise InvalidState('价格振幅乘数错误')
            if type(self.mark_event_ms) is not int or type(self.mark_received_ms) is not int or not 0<=self.mark_event_ms<=self.mark_received_ms:
                raise InvalidState('标记价格时间错误')
        if self.maximum_orders is not None and (type(self.maximum_orders) is not int or self.maximum_orders<1):raise InvalidState('普通订单数量上限错误')
        if self.source_digest is not None and (type(self.source_digest) is not str or len(self.source_digest)!=64 or any(c not in '0123456789abcdef' for c in self.source_digest)):
            raise InvalidState('合约规则来源指纹无效')

    def price_allowed(self,value):
        value=positive(value)
        return (value>=self.minimum_price and (self.maximum_price is None or value<=self.maximum_price)
            and (value-self.minimum_price)%self.tick==0)

    def entry_price_issues(self,side,price,*,now_ms,max_age_ms):
        issues=[]
        if not self.entry_allowed:issues.append('EXCHANGE_ENTRY_RESTRICTED')
        if self.requires_mark and self.mark_price is None:issues.append('MARK_PRICE_REQUIRED')
        if self.mark_price is not None:
            if not 0<=self.mark_event_ms<=self.mark_received_ms<=now_ms or now_ms-self.mark_event_ms>max_age_ms:
                issues.append('MARK_PRICE_STALE_OR_FUTURE')
            # USD-M is asymmetric: a resting dip BUY may be well below mark,
            # and a resting rally SELL may be well above it.
            if side=='BUY' and price>self.mark_price*self.percent_up or side=='SELL' and price<self.mark_price*self.percent_down:
                issues.append('PERCENT_PRICE_FILTER')
        return tuple(issues)

    def floor_quantity(self, requested, price, margin_cap, leverage, fee_rate) -> Decimal:
        """Only size downward at decision time; never rescue an undersized order.
        The returned size must then remain fixed through submission and recovery.
        """
        price, requested = positive(price), nonnegative(requested)
        margin_cap, fee_rate = nonnegative(margin_cap), nonnegative(fee_rate)
        if type(leverage) is not int or not 1 <= leverage <= self.maximum_leverage:
            raise InvalidState("杠杆超过当前交易所档位")
        cap = margin_quantity_cap(price, margin_cap, leverage, fee_rate)
        q = min(requested, cap, self.maximum_quantity, self.maximum_notional / price)
        q = (q / self.step).to_integral_value(rounding=ROUND_FLOOR) * self.step
        if not self.tradable or not self.entry_allowed or self.requires_mark and self.mark_price is None or q < self.minimum_quantity or q * price < self.minimum_notional:
            return Decimal(0)
        return q

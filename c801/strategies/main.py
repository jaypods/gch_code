"""Receipt-driven M784 decisions, independent of exchange or replay execution.

The same book is used by a historical venue model and a user-operated broker.
It consumes only explicitly available observations and receipts. It never reads
the simulated/real venue's unreceived position state, and never sends an order.

M784 source admission (including its separate shadow book) is the caller's job.
The shadow variant below retains the original 10/5/2 DD policy and 25% budget;
it must receive unfiltered paired signals, and never weekly/refresh sources.
"""
from dataclasses import dataclass, asdict
from decimal import Decimal
from copy import deepcopy
import heapq
import math
import numpy as np
from ..account import InvalidState

MINUTE = 60_000
CLOCK = 300_000
FIFTEEN = 900_000
HOLD = 144 * 3_600_000
COOL = 6 * 3_600_000
SYMS = ("BTCUSDT", "ETHUSDT")


def valid_price(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


@dataclass(frozen=True)
class MainQuote:
    symbol: str
    open_ms: int
    close_ms: int
    received_ms: int
    close: float
    quote_volume: float

    def validate(self, now):
        if self.symbol not in SYMS or self.close_ms - self.open_ms != CLOCK or self.open_ms % CLOCK:
            raise InvalidState("主流币观察周期错误")
        if self.close_ms != now - CLOCK or not self.close_ms <= self.received_ms <= now:
            raise InvalidState("主流币观察未满足原策略延迟或尚未收到")
        if not valid_price(self.close) or not math.isfinite(self.quote_volume) or self.quote_volume < 0:
            raise InvalidState("主流币价格或成交额无效")


@dataclass(frozen=True)
class MainSource:
    symbol: str
    side: int
    source_close_ms: int
    available_ms: int
    reference: float
    fixed_limit: float
    source_id: str
    family: str = "ordinary"
    spot_limit: float | None = None

    @property
    def decision_ms(self):
        return self.source_close_ms + CLOCK

    def validate(self, now):
        if self.symbol not in SYMS or self.side not in (-1, 1) or not self.source_id:
            raise InvalidState("主流币信号标识错误")
        if self.family not in ("ordinary", "weekly_downcross", "weekly_reclaim", "refresh"):
            raise InvalidState("主流币信号类别错误")
        if self.decision_ms != now or not self.source_close_ms <= self.available_ms <= now:
            raise InvalidState("信号尚未收到或不属于当前决策时刻")
        if not valid_price(self.reference) or not valid_price(self.fixed_limit):
            raise InvalidState("信号价格无效")
        if self.family.startswith("weekly"):
            if self.side != 1 or not valid_price(self.spot_limit):
                raise InvalidState("周线信号方向或现货参考无效")
        elif self.source_close_ms % FIFTEEN:
            raise InvalidState("普通信号必须来自完整十五分钟窗口")


@dataclass(frozen=True)
class MainReceipt:
    event_id: str
    order_id: int
    kind: str
    event_ms: int
    received_ms: int
    available_ms: int
    quantity: float = 0.
    price: float = 0.
    raw_price: float = 0.
    cash_delta: float = 0.
    entry_finished: bool = False
    settled: bool = False

    def validate(self):
        if not self.event_id or self.kind not in ("entry", "exit", "funding", "entry_done", "settled"):
            raise InvalidState("成交回报标识或类型无效")
        if not 0 <= self.event_ms <= self.received_ms <= self.available_ms:
            raise InvalidState("成交回报时间无效")
        if not all(math.isfinite(x) for x in (self.quantity, self.price, self.raw_price, self.cash_delta)):
            raise InvalidState("成交回报含非有限数值")
        if self.quantity < 0:
            raise InvalidState("成交量不能为负")
        if self.kind in ("entry", "exit") and not (self.quantity > 0 and self.price > 0 and self.raw_price > 0):
            raise InvalidState("成交量或成交价缺失")


class MainBook:
    def __init__(self, *, start_ms: int, initial: float, cost=1, mode="M784"):
        if start_ms % CLOCK or not valid_price(initial) or cost not in (1, 2) or mode not in ("M784", "shadow"):
            raise InvalidState("主流币账户初始化参数错误")
        self.start_ms, self.initial, self.cost, self.mode = start_ms, float(initial), cost, mode
        self.known_cash = self.known_peak = self.retained_cash_peak = float(initial)
        self.risk_index = {s: 2 for s in SYMS}
        self.risk_counts = {s: 0 for s in SYMS}
        self.cool = {s: start_ms for s in SYMS}
        self.orders, self.positions, self.reservations = {}, {}, {}
        self.groups, self.group_cash, self.group_filled, self.group_done = {}, {}, set(), set()
        self.terminal_ids = set()
        self.queue, self.event_payloads, self.deliveries = [], {}, []
        self.risk_updates, self.requests, self.admissions = [], [], []
        self.serial = 0
        self.last_clock = start_ms - CLOCK
        self.consumed_sources = set()

    def receive(self, receipt: MainReceipt):
        receipt.validate()
        if receipt.order_id not in self.orders:
            raise InvalidState("成交回报没有对应的本策略订单")
        payload = asdict(receipt)
        prior = self.event_payloads.get(receipt.event_id)
        if prior is not None:
            if prior != payload:
                raise InvalidState("同一成交标识内容冲突")
            return False
        # Late receipts remain late. Never insert a receipt into a clock that
        # has already made decisions; the broker must preserve real arrival.
        if receipt.available_ms <= self.last_clock:
            raise InvalidState("回报可用时间不能回填到已执行的决策时刻")
        self.serial += 1
        heapq.heappush(self.queue, (receipt.available_ms, self.serial, payload))
        self.event_payloads[receipt.event_id] = payload
        return True

    def _deliver(self, payload, now):
        o = self.orders[payload["order_id"]]
        oid, group = o["id"], o["group"]
        if group in self.group_done:
            raise InvalidState("已结算交易组又收到新回报，必须停止并重新对账")
        kind, q = payload["kind"], payload["quantity"]
        pos = self.positions.get(oid)
        if kind == "entry":
            if o["entry_done"] or o["filled_quantity"] + q > o["qty"] + 1e-10:
                raise InvalidState("开仓成交量或订单终态矛盾")
            if pos is None:
                pos = dict(quantity=0., basis=0., raw_basis=0., entry_delivered_ms=now,
                           requested_stop=o["initial_stop"], profit_requested=False)
                self.positions[oid] = pos
            pos["quantity"] += q
            pos["basis"] += q * payload["price"]
            pos["raw_basis"] += q * payload["raw_price"]
            o["filled_quantity"] += q
            self.group_filled.add(group)
            if payload["entry_finished"]:
                o["entry_done"] = True
                self.reservations.pop(oid, None)
            else:
                fraction = max(0., 1 - o["filled_quantity"] / o["qty"])
                self.reservations[oid] = fraction
        elif kind == "exit":
            if pos is None or q > pos["quantity"] + 1e-10:
                raise InvalidState("平仓回报超过已收到的持仓")
            old = pos["quantity"]
            remaining = max(0., old - q)
            pos["basis"] *= remaining / old
            pos["raw_basis"] *= remaining / old
            pos["quantity"] = remaining
            if remaining <= 1e-10:
                self.positions.pop(oid)
                self.cool[o["symbol"]] = max(self.cool[o["symbol"]], payload["event_ms"] + COOL)
        elif kind == "entry_done":
            o["entry_done"] = True
            self.reservations.pop(oid, None)
        elif kind == "settled":
            if not o["entry_done"] or oid in self.positions:
                raise InvalidState("仍有挂单或持仓的订单不能结算")
        if payload["settled"] or kind == "settled":
            if not o["entry_done"] or oid in self.positions:
                raise InvalidState("结算回报与剩余订单状态矛盾")
            self.terminal_ids.add(oid)
        self.known_cash += payload["cash_delta"]
        self.group_cash[group] = self.group_cash.get(group, Decimal(0)) + Decimal(str(payload["cash_delta"]))
        self.deliveries.append(dict(delivered_ms=now, **payload))

    def known_state(self, quotes):
        equity, margin = self.known_cash, 0.
        for oid, p in self.positions.items():
            o = self.orders[oid]
            equity += o["side"] * (p["quantity"] * quotes[o["symbol"]].close - p["basis"])
            margin += p["raw_basis"] / o["lev"]
        return equity, margin

    def _settle_groups(self, now):
        ready = sorted(g for g, ids in self.groups.items() if g not in self.group_done and all(i in self.terminal_ids for i in ids))
        if self.mode == "M784":
            for symbol in SYMS:
                own = [g for g in ready if g in self.group_filled and self.orders[self.groups[g][0]]["symbol"] == symbol]
                if not own:
                    continue
                net = sum((self.group_cash[g] for g in own), Decimal(0))
                old = self.risk_index[symbol]
                new = max(0, min(2, old + (1 if net > 0 else -1 if net < 0 else 0)))
                self.risk_index[symbol] = new
                self.risk_counts[symbol] += 1
                self.risk_updates.append(dict(time_ms=now, symbol=symbol, groups=own, net_decimal=str(net),
                                             prior_index=old, index=new, leverage=(4, 10, 20)[new]))
        self.group_done.update(ready)
        self.retained_cash_peak = max(self.retained_cash_peak, self.known_cash)

    def _plan(self, source, now, quotes):
        equity, margin = self.known_state(quotes)
        friction = sum(self.orders[i]["reserved_friction"] * f for i, f in self.reservations.items())
        reserved_margin = sum(self.orders[i]["reserved_margin"] * f for i, f in self.reservations.items())
        available = equity - friction
        dd = 1 - equity / max(self.known_peak, 1e-12)
        symbol, side, ref = source.symbol, source.side, source.reference
        shadow = self.mode == "shadow"
        leverage = (2 if dd >= .30 else 5 if dd >= .15 else 10) if shadow else (4, 10, 20)[self.risk_index[symbol]]
        retained = 0. if shadow else .5 * max(0., self.retained_cash_peak - self.initial)
        base = max(0., available - retained)
        budget = .25 if shadow else .30
        q = base * budget * leverage / ref
        quote = quotes[symbol].quote_volume
        capacity = .01 * quote / ref
        unconstrained = q
        if not shadow and q * ref > .01 * quote:
            q = float(np.nextafter(capacity, 0.))
        slip, fee = .0004 * self.cost, .0005 * self.cost
        fc, required = q * ref * (slip + (1 + slip) * fee), q * ref / leverage
        reason = ("nonpositive_known_equity" if available <= 0 else "minimum_model_notional" if q * ref < 20
                  else "known_quote_capacity" if q * ref > .01 * quote
                  else "known_margin" if margin + reserved_margin + required > available - fc else None)
        weekly = source.family.startswith("weekly")
        if weekly and reason != "nonpositive_known_equity" and q * ref < 40:
            reason = "minimum_model_notional"
        group = ("weekly_" if weekly else "ordinary_") + source.source_id
        if group in self.groups:
            raise InvalidState("同一来源不能重复创建交易组")
        self.groups[group] = []
        legs = (("perpetual", source.fixed_limit), ("spot", source.spot_limit)) if weekly else (("ordinary", source.fixed_limit),)
        results = []
        for kind, limit in legs:
            share = .5 if weekly else 1.
            oid = len(self.orders)
            o = dict(id=oid, group=group, symbol=symbol, side=side, source_id=source.source_id,
                     family="weekly" if weekly else "ordinary", reference_kind=kind, share=share,
                     source_ms=source.source_close_ms, source_available_ms=source.available_ms,
                     decision_ms=now, planned_ms=now+CLOCK, expiry_ms=source.source_close_ms+4*3_600_000,
                     deadline_ms=now+CLOCK+HOLD, reference=ref, fixed_limit=limit,
                     initial_stop=ref*(1-side*.03), initial_target=ref*(1+side*.08), entry_cap_distance=.015*ref,
                     qty=q*share, lev=leverage, known_cash=self.known_cash, known_equity=equity, known_margin=margin,
                     known_peak=self.known_peak, known_dd=dd, risk_index=self.risk_index[symbol],
                     risk_event_count=self.risk_counts[symbol], group_margin_budget=budget,
                     retained_cash_peak=self.retained_cash_peak, profit_retention=retained, sizing_base=base,
                     reserved_margin=required*share, reserved_friction=fc*share, capacity_quote=quote,
                     unconstrained_qty=unconstrained*share, capacity_qty=capacity*share,
                     reject_reason=reason, entry_done=bool(reason), filled_quantity=0., refresh=None)
            self.orders[oid] = o
            self.groups[group].append(oid)
            if reason:
                self.terminal_ids.add(oid)
            else:
                self.reservations[oid] = 1.
                results.append(dict(action="ENTRY_PLAN", order=deepcopy(o)))
        return results

    def _protection(self, now, quotes, *, symbols=None):
        actions = []
        if self.mode == "shadow":
            return actions
        for oid, p in sorted(self.positions.items()):
            if symbols is not None and self.orders[oid]['symbol'] not in symbols:
                continue
            if p["profit_requested"]:
                continue
            o = self.orders[oid]
            q = quotes[o["symbol"]]
            if q.open_ms < p["entry_delivered_ms"] or q.quote_volume <= 0:
                continue
            entry = Decimal(str(p["basis"] / p["quantity"]))
            distance = abs(entry - Decimal(str(o["initial_stop"])))
            if distance <= 0 or o["side"] * (Decimal(str(q.close)) - entry) < 2 * distance:
                continue
            new = float(entry + o["side"] * distance)
            if not valid_price(new) or o["side"] * (new - p["requested_stop"]) <= 0:
                raise InvalidState("利润保护价格不合法")
            request = dict(action="STOP_AMEND", order_id=oid, decision_ms=now, effective_ms=now+CLOCK,
                           old_stop=p["requested_stop"], new_stop=new, quantity=p["quantity"],
                           observation_close_ms=q.close_ms, observation_received_ms=q.received_ms)
            p["profit_requested"], p["requested_stop"] = True, new
            self.requests.append(deepcopy(request))
            actions.append(request)
        return actions

    def _transaction_snapshot(self):
        """Rollback mutable working state without recopying settled history.

        Receipt registration happens outside advance. Existing group leg lists,
        event payloads and appended audit rows are never edited by advance.
        Settled groups reject further receipts before touching their orders.
        """
        state = self.__dict__.copy()
        tails = {name: len(state[name]) for name in
                 ('deliveries', 'risk_updates', 'requests', 'admissions')}
        for name in ('positions', 'reservations', 'risk_index', 'risk_counts', 'cool'):
            state[name] = deepcopy(state[name])
        state['orders'] = {oid: (o if o.get('group') in self.group_done else deepcopy(o))
                           for oid, o in self.orders.items()}
        for name in ('groups', 'group_cash'):
            state[name] = state[name].copy()
        for name in ('group_filled', 'group_done', 'terminal_ids', 'consumed_sources'):
            state[name] = state[name].copy()
        state['queue'] = list(self.queue)
        return state, tails

    def _transaction_restore(self, snapshot):
        state, tails = snapshot
        for name, length in tails.items():
            del state[name][length:]
        self.__dict__.clear()
        self.__dict__.update(state)

    def advance(self, now, quotes, *, ordinary=(), weekly=(), refresh=()):
        if now != self.last_clock + CLOCK or now % CLOCK:
            raise InvalidState("策略时钟必须逐个五分钟推进；缺口需要恢复")
        if set(quotes) != set(SYMS):
            raise InvalidState("BTC 和 ETH 的观察必须齐全")
        for symbol, q in quotes.items():
            if q.symbol != symbol:
                raise InvalidState("行情币种映射错误")
            q.validate(now)
        if self.mode == "shadow" and (weekly or refresh):
            raise InvalidState("模拟机会账户不能接收周线或挂单更新来源")
        sources = list(ordinary) + list(weekly) + list(refresh)
        if len({s.source_id for s in sources}) != len(sources):
            raise InvalidState("同一时刻有重复来源标识")
        for s in sources:
            s.validate(now)
        if any(s.family != "ordinary" for s in ordinary) or any(not s.family.startswith("weekly") for s in weekly) or any(s.family != "refresh" for s in refresh):
            raise InvalidState("来源类别与入口不一致")
        before = self._transaction_snapshot()
        try:
            result = self._advance(now, quotes, ordinary, weekly, refresh)
            self.last_clock = now
            return result
        except BaseException:
            self._transaction_restore(before)
            raise

    def _advance(self, now, quotes, ordinary, weekly, refresh):
        while self.queue and self.queue[0][0] <= now:
            _, _, payload = heapq.heappop(self.queue)
            self._deliver(payload, now)
        self._settle_groups(now)
        equity, _ = self.known_state(quotes)
        self.known_peak = max(self.known_peak, equity)
        actions = self._protection(now, quotes)
        for source in sorted(refresh, key=lambda s:(s.symbol, s.source_id)):
            for oid in sorted(self.reservations):
                o = self.orders[oid]
                if o["refresh"] is not None or now <= o["decision_ms"] or now+CLOCK >= o["expiry_ms"] or source.symbol != o["symbol"] or source.side != o["side"]:
                    continue
                limit, side = source.fixed_limit, o["side"]
                eligible = side*(limit-o["fixed_limit"]) > 0 and side*(limit-o["initial_stop"]) > 0 and side*(o["initial_target"]-limit) > 0
                change = dict(action="ENTRY_AMEND", order_id=oid, source_id=source.source_id, request_ms=now,
                              effect_ms=now+CLOCK, model_ack_ms=now+2*CLOCK, new_limit=limit, eligible=eligible)
                o["refresh"] = deepcopy(change)
                actions.append(change)
        for source in sorted(ordinary, key=lambda s:(s.symbol,s.source_id)) + sorted(weekly, key=lambda s:(s.symbol,s.source_id)):
            if source.source_id in self.consumed_sources:
                raise InvalidState("来源标识已经处理")
            self.consumed_sources.add(source.source_id)
            symbol = source.symbol
            reason = ("known_pending" if any(self.orders[i]["symbol"] == symbol for i in self.reservations)
                      else "known_position" if any(self.orders[i]["symbol"] == symbol for i in self.positions)
                      else "cooldown" if now < self.cool[symbol] else None)
            self.admissions.append(dict(source_id=source.source_id, decision_ms=now, symbol=symbol, reason=reason))
            if reason is None:
                actions.extend(self._plan(source, now, quotes))
        return actions

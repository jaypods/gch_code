"""Causal ordinary-source predicate used by M784.

Source lineage: run476.features -> run581.corrected -> run546.observation.
This module covers the paired source predicate, NOT the opportunity/shadow clock,
weekly reclaim population, pending refresh, entry lifecycle or portfolio engine.
Those require separate ports before the M784 migration flag can be true.
"""
from dataclasses import dataclass
from decimal import Decimal, localcontext
from math import isfinite
import numpy as np
from ..account import InvalidState
from ..market import Bar, CausalMarket

FIVE = 300_000
FIFTEEN = 900_000
SYMBOLS = ("BTCUSDT", "ETHUSDT")


@dataclass(frozen=True)
class Feature:
    symbol: str
    end_ms: int
    available_ms: int
    side: int
    volatility: float
    reference: float
    prior_high: float
    prior_low: float
    quote_ratio: float
    valid: bool


def features(bars: tuple[Bar, ...], *, now_ms: int) -> Feature:
    """Exactly 97 completed 15m bars, current plus the preceding 96.
    Threshold arithmetic preserves the original NumPy float implementation.
    """
    if len(bars) != 97:
        raise InvalidState("主流币信号需要连续 97 根十五分钟 K 线")
    first = bars[0]
    if any(b.symbol != first.symbol or b.period_ms != FIFTEEN or b.start_ms != first.start_ms + i * FIFTEEN for i, b in enumerate(bars)):
        raise InvalidState("主流币信号窗口不连续")
    available = max(b.received_ms for b in bars)
    if available > now_ms:
        raise InvalidState("主流币信号窗口尚未收到")
    closes = np.array([float(b.close) for b in bars])
    highs = np.array([float(b.high) for b in bars])
    lows = np.array([float(b.low) for b in bars])
    quotes = np.array([float(b.quote_volume) for b in bars])
    valid = bool(np.all(np.isfinite(closes + highs + lows + quotes)) and np.all(quotes > 0))
    upper, lower = float(np.max(highs[-17:-1])), float(np.min(lows[-17:-1]))
    mean_quote = float(np.mean(quotes[-21:-1]))
    ratio = float(quotes[-1] / (mean_quote if mean_quote > 0 else 1))
    volatility = float(np.std(np.diff(np.log(closes))))
    side = (1 if closes[-1] > upper else -1 if closes[-1] < lower else 0) if valid and ratio >= 1.5 else 0
    return Feature(first.symbol, bars[-1].end_ms, available, side, volatility, float(closes[-1]), upper, lower, ratio, valid)


def accepted_average(parts, boundary: float, side: int, *, end_ms: int, now_ms: int):
    if side not in (-1, 1) or not isfinite(boundary) or boundary <= 0 or len(parts) != 3:
        raise InvalidState("突破均价检查参数错误")
    if any(b.period_ms != FIVE or b.end_ms != end_ms - (2 - i) * FIVE or b.symbol != parts[0].symbol for i, b in enumerate(parts)):
        raise InvalidState("突破均价检查窗口不连续")
    available = max(end_ms + FIVE, *(b.received_ms for b in parts))
    if available > now_ms:
        raise InvalidState("突破均价观察尚未可用")
    # Original predicate intentionally uses decimal representations of float
    # inputs. Retain that convention for regression parity at the boundary.
    with localcontext() as context:
        context.prec = 50
        base = sum((Decimal(str(float(b.volume))) for b in parts), Decimal(0))
        quote = sum((Decimal(str(float(b.quote_volume))) for b in parts), Decimal(0))
        if base <= 0:
            raise InvalidState("突破窗口没有真实成交")
        signed = side * (quote - Decimal(str(float(boundary))) * base)
        return {"passed": signed > 0, "signed_notional_margin": str(signed), "available_ms": available}


def paired_observation(market: CausalMarket, *, source_end_ms: int, now_ms: int):
    if source_end_ms % FIFTEEN or now_ms < source_end_ms + FIVE:
        raise InvalidState("普通信号的十五分钟收盘及五分钟延迟尚未满足")
    rows = {}
    acceptance = True
    for symbol in SYMBOLS:
        native = market.window(symbol, FIVE, source_end_ms, 97 * 3, now_ms=now_ms, lag_ms=FIVE)
        grouped = []
        for i in range(0, len(native), 3):
            parts = native[i:i+3]
            grouped.append(Bar(symbol, parts[0].start_ms, parts[-1].end_ms, max(b.received_ms for b in parts),
                               parts[0].open, max(b.high for b in parts), min(b.low for b in parts), parts[-1].close,
                               sum((b.volume for b in parts), Decimal(0)), sum((b.quote_volume for b in parts), Decimal(0))))
        f = features(tuple(grouped), now_ms=now_ms)
        # Original validity tests EACH 5m quote, not merely a positive 15m sum.
        native_valid = all(float(b.quote_volume) > 0 and isfinite(float(b.quote_volume)) for b in native)
        rows[symbol] = {"features": f, "parts": native[-3:], "native_valid": native_valid}
    first, second = (rows[s]["features"] for s in SYMBOLS)
    if not (all(rows[s]["native_valid"] for s in SYMBOLS) and first.valid and second.valid and first.side == second.side and first.side != 0
            and first.volatility <= .0045 and second.volatility <= .0045):
        return None
    for symbol in SYMBOLS:
        f = rows[symbol]["features"]
        bound = f.prior_high if f.side == 1 else f.prior_low
        result = accepted_average(rows[symbol]["parts"], bound, f.side, end_ms=source_end_ms, now_ms=now_ms)
        acceptance = acceptance and result["passed"]
    return {"source_close_ms": source_end_ms, "available_ms": max(source_end_ms + FIVE, first.available_ms, second.available_ms),
            "side": first.side, "features": {symbol: rows[symbol]["features"] for symbol in SYMBOLS},
            "acceptance_pass": acceptance,
            "admission_stage": "ordinary_candidate_before_shadow_clock"}


def ordinary_candidate(market: CausalMarket, *, source_end_ms: int, now_ms: int):
    observation = paired_observation(market, source_end_ms=source_end_ms, now_ms=now_ms)
    return observation if observation is not None and observation["acceptance_pass"] else None

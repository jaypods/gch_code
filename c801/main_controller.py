"""Causal M784 source routing; no exchange writes or access to venue state.

The opportunity account is a fixed 1000-unit model, independent of the user's
capital. Its original occupancy/cooldown clock is part of the strategy signal.
Its current submission must not block the same instant's funded admission.
"""
from dataclasses import replace
from .account import InvalidState
from .strategies.main import MainBook, MainQuote, MainSource, CLOCK, FIFTEEN, SYMS
from .strategies.main_sources import paired_observation
from .strategies.main_weekly import WeeklyReferences,ORIGIN,WEEK


class MainController:
    def __init__(self, *, start_ms, initial, cost=1):
        self.funded = MainBook(start_ms=start_ms, initial=initial, cost=cost)
        self.shadow = MainBook(start_ms=start_ms, initial=1000., cost=cost, mode='shadow')
        self.weekly = WeeklyReferences()
        self.source_audit = []

    def advance(self, now, perpetual, spot):
        end = now - CLOCK
        quotes = {}
        for symbol in SYMS:
            b = perpetual.window(symbol, CLOCK, end, 1, now_ms=now, lag_ms=CLOCK)[0]
            quotes[symbol] = MainQuote(symbol, b.start_ms, b.end_ms, b.received_ms, float(b.close), float(b.quote_volume))
        observation = paired_observation(perpetual, source_end_ms=end, now_ms=now) if end % FIFTEEN == 0 else None
        raw = []
        if observation:
            for symbol in SYMS:
                f = observation['features'][symbol]
                raw.append(MainSource(symbol, f.side, end, observation['available_ms'], f.reference,
                                      f.prior_high if f.side > 0 else f.prior_low, f'{symbol}:{end}:ordinary'))
        if end>ORIGIN+CLOCK:
            week_end=ORIGIN+((end-CLOCK-ORIGIN)//WEEK)*WEEK
            self.weekly.observe_completed_week(perpetual,spot,week_end_ms=week_end,now_ms=now)
        weekly = self.weekly.sources(perpetual, spot, source_end_ms=end, now_ms=now)
        # A refresh source is drawn from the full accepted signal population,
        # not from the opportunity-gated admission calendar.
        accepted = raw if observation and observation['acceptance_pass'] else []
        refresh = [replace(s, source_id=f'{s.symbol}:{end}:refresh', family='refresh') for s in accepted]
        previous_shadow, previous_funded = self.shadow._transaction_snapshot(), self.funded._transaction_snapshot()
        try:
            offset = len(self.shadow.admissions)
            shadow_actions = self.shadow.advance(now, quotes, ordinary=raw)
            admissions = self.shadow.admissions[offset:]
            allowed = {r['source_id'] for r in admissions if r['reason'] is None}
            funded_sources = [s for s in accepted if s.source_id in allowed]
            funded_actions = self.funded.advance(now, quotes, ordinary=funded_sources, weekly=weekly, refresh=refresh)
        except BaseException:
            for book, state in ((self.shadow, previous_shadow), (self.funded, previous_funded)):
                book._transaction_restore(state)
            raise
        rows = [dict(source_id=s.source_id, symbol=s.symbol, decision_ms=now,
                     average_pass=bool(observation['acceptance_pass']), shadow_allowed=s.source_id in allowed,
                     shadow_reason=next(r['reason'] for r in admissions if r['source_id']==s.source_id),
                     routed_to_funded=s in funded_sources) for s in raw]
        self.source_audit.extend(rows)
        return dict(funded_actions=funded_actions, shadow_actions=shadow_actions, source_audit=rows)

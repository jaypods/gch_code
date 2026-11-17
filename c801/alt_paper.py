"""Original A787 full-size admission; no simulator or supervisor service."""
def entry_check(o, raw, known_equity, cash, positions, opens, quote, cost):
    """A480 full-size admission at execution; no current high/low/close input."""
    fee, slip = .001*cost, .002*cost
    fill = raw*(1+slip)
    reason = ('open_exceeds_precommitted_limit' if fill > o['fill_price_cap']+1e-12*o['fill_price_cap']
              else 'open_at_or_below_precommitted_stop' if raw <= o['stop']
              else 'open_at_or_above_installed_target' if raw >= o['tp2']
              else 'adverse_fill_at_or_above_installed_target' if fill >= o['tp2'] else None)
    if reason is None and any(p['symbol'] == o['symbol'] and (not o.get('is_add') or p['id'] != o['parent_id']) for p in positions):
        reason = 'actual_other_same_symbol_lot' if o.get('is_add') else 'actual_symbol_occupied'
    if reason:
        return dict(reason=reason, raw=raw, fill=fill, quantity=o['quantity'])
    actual = cash+sum(p['quantity']*(opens[p['symbol']]-p['entry_fill']) for p in positions)
    capital = min(max(0., known_equity), max(0., actual))
    margin = sum(p['quantity']*opens[p['symbol']]/p['leverage'] for p in positions)
    gross = sum(p['quantity']*opens[p['symbol']] for p in positions)
    same_margin = sum(p['quantity']*opens[p['symbol']]/p['leverage'] for p in positions if p['symbol'] == o['symbol']) if o.get('is_add') else 0.
    def current_risk(p):
        return p['quantity']*max(0.,opens[p['symbol']]-p['stop']*(1-slip)+fee*p['stop']*(1-slip))
    risk = sum(map(current_risk,positions))
    same = sum(current_risk(p) for p in positions if p['leadership_event_hour'] == o['leadership_event_hour'])
    rb, lev = o['effective_risk_fraction'], o['leverage']
    exit_fill = o['stop']*(1-slip)
    loss = fill-exit_fill+fee*(fill+exit_fill)
    unit_fee = fill*fee
    caps = dict(frozen_quantity=o['quantity'], stop_budget=rb*capital/(loss+rb*unit_fee),
                single_margin=max(0.,(.25*actual-same_margin)/(fill/lev+.25*unit_fee)),
                total_margin=max(0.,(.75*actual-margin)/(fill/lev+.75*unit_fee)),
                gross=max(0.,(7.5*actual-gross)/(fill+7.5*unit_fee)),
                open_stop_risk=max(0.,(.10*actual-risk)/(loss+.10*unit_fee)),
                same_event_open_risk=max(0.,(rb*actual-same)/(loss+rb*unit_fee)),
                frozen_event_commitment=o['event_reserved_loss']/loss, delayed_quote_participation=quote*.01/fill)
    maximum = max(0.,min(caps.values()))
    if maximum*fill < 20:
        reason = 'below_research_minimum_after_actual_open_resize'
    elif loss/fill+.02 > 1/lev+1e-10:
        reason = 'actual_stop_margin_buffer_violation'
    elif maximum < o['quantity']-1e-12:
        reason = 'fixed_quantity_not_fully_admissible'
    return dict(reason=reason, raw=raw, fill=fill, quantity=o['quantity'], maximum_admissible_quantity=maximum,
                size_limits=caps, binding_limit=min(caps,key=caps.get), known_equity=known_equity, actual_open_equity=actual,
                min_risk_capital=capital, costed_unit_stop_risk=loss, frozen_quantity_unchanged=True)

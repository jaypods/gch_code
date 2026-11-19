# M784 R3

M784 R3 trades BTCUSDT and ETHUSDT perpetual futures in a simulated USDT account. This directory archives research version 1.0.0 (`M784_R3_NG_20261117`), the 3% risk variant with the original entry rules. The C801 application uses its own original M784 implementation.

## Rules

- **Ordinary entry.** Both instruments must break their respective prior 16-bar boundaries in the same direction on completed 15-minute bars. The signal bar needs at least 1.5 times the prior 20-bar average quote volume, a full-bar VWAP beyond the boundary, and a 96-return standard deviation of at most 0.0045. R3 takes qualifying signals without the original shadow-account admission filter.
- **Weekly entry.** Use the prior 29 completed UTC weekly closes and the latest completed 5-minute close to calculate a 30-week mean. Either crossing direction generates a long setup under the frozen rule. Spot and perpetual references supply separate limits for two equal legs that share one risk budget and execute in the perpetual contract.
- **Orders.** For a signal ending at T, size at T+5 minutes. Start checking for a fill at T+10, then every five minutes until T+4 hours. Ordinary orders use the breakout boundary for their initial limit. Submitted quantities stay fixed. The original policy allows one qualifying limit adjustment.
- **Position sizing.** Budget 3% of the risk base per instrument and order group. Start from known marked equity, deduct pending entry costs, then reserve half the received cash high-water mark above the initial 1,000 USDT. Divide the budget by the modeled loss per unit at the stop, including adverse entry and exit costs. Cap the order using 1% of the last known five-minute quote volume and require enough margin at 4×. Concurrent BTC and ETH positions each consume their own risk budget. Gap losses and funding can exceed the planned risk.
- **Exits.** Place a 3% stop and an 8% target relative to the signal reference. Let R be the distance between the modeled entry fill and the original stop. Once an eligible completed five-minute close reaches +2R, request one stop adjustment to +1R. The update takes effect five minutes later. The hold limit is 144 hours from the planned entry and the instrument cooldown is six hours from exit. Same-minute conflicts use the stop first.

## Saved backtests

Each simulation starts with 1,000 USDT. Standard costs apply 0.05% fees and 0.04% slippage per side. Double costs multiply both by two. Funding is booked separately using each instrument's historical rates. End dates are exclusive. Drawdown uses minute equity including open P&L.

| UTC window | Costs | Final USDT | Net CAGR | Max drawdown | Filled legs |
|---|---|---:|---:|---:|---:|
| 2024-01-01 to 2026-10-01 | Standard | 4,768.49 | 76.52% | 23.98% | 561 |
| 2024-01-01 to 2026-10-01 | Double | 3,019.35 | 49.48% | 25.01% | 558 |
| 2023-01-01 to 2024-01-01 | Standard | 1,424.62 | 42.50% | 30.13% | 181 |
| 2023-01-01 to 2024-01-01 | Double | 1,113.33 | 11.34% | 30.34% | 182 |

The variant was selected after inspecting earlier 2023 results. Both periods are retrospective. The simulation approximates execution from minute bars. The fills are simulated, and exchange queue position, historical lot filters, and the full liquidation process were not independently validated.

## Files and use

Download [trade_atlas.html](trade_atlas.html) and open it locally in a recent browser to inspect the 1,482 simulated trade legs across the four scenarios. The atlas includes linear equity, a separate drawdown chart, 15-minute candles, entry and exit prices, protection changes, and CSV export.

[Parameters](strategy.json) · [Account results](results.json) · [Research protocol](research_protocol.json) · [Recorded synthetic checks](synthetic_checks.json) · [Comparison of entry and exit paths](path_attribution.json)

[Execution kernel](execution_engine.py) and [audit kernel](audit_engine.py) are byte-for-byte copies of the archived research code. A new replay requires NumPy and a caller supplying aligned 1-minute and 5-minute data, funding and mark prices, timestamped signals, warm-up values, and weekly setups. Bind REFRESH_SOURCES for execution and EXPECTED_REFRESH, EXPECTED_RETENTION and EXPECTED_RISK for the independent audit. Set the window start in both kernels and pass the matching exclusive end. The saved results and atlas can be inspected offline without those inputs.

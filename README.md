# C801 Trading Workbench

A Windows desktop-hosted Binance USD-M futures trading application with a local web dashboard. C801 combines **M784 (BTC/ETH), A787 (altcoins), and S800 (stock-proxy contracts)** using separate strategy budgets.

This private repository contains a source snapshot of the installed application, with English documentation and an English dashboard. It is not a new strategy or a replacement for an already-running instance. The installed source includes the dependency-aware scheduler. A process started before that update may still be running its earlier in-memory version.

## What is included

- Original strategy signals, position sizing, lifecycle controllers, and dynamic protection rules.
- Binance public market data, private account access, order execution, fill reconciliation, and funding accounting.
- Separate strategy budgets, persisted runtime state, restart recovery, and an exclusive runtime owner lock.
- Dependency-aware scheduling: missing data blocks the computations that require it. BTC/ETH confirmation and complete candidate rankings remain required; late data does not create retroactive orders.
- A loopback-only dashboard with account equity, available margin, positions, actual fills, and linear 1-minute, 5-minute, and 15-minute candlestick charts.
- Windows Credential Manager integration. Credential values are not stored in repository files or exposed by the dashboard.

No API keys, account identifiers, live positions, trade databases, runtime locks, private logs, historical market caches, or research datasets are included.

## Requirements

- Windows and Python 3.12 with Tkinter.
- Dependencies in `requirements.txt` (versions recorded from the source environment).
- Network access and an account permitted to use the required Binance futures services.

The built-in credential provider is Windows-specific. This snapshot is not a Linux deployment package. The dashboard is a local HTTP service, not a static GitHub Pages application.

## Installation

From the repository directory:

```powershell
py -3.12 -m pip install -r requirements.txt
```

Review the source and configuration before running it. On a separate installation, configure credentials through the local native dialog:

```powershell
py -3.12 configure_api.py
```

The credential target is `C801/Binance/Futures/HMAC/v1` for the current Windows user. **All copies using that target share the same local credential entry. Saving a replacement can affect other copies when they next read it.** Do not run credential setup merely to inspect this repository, or replace credentials used by an existing trading installation.

## Starting the dashboard

Use `start_dashboard.cmd`, or:

```powershell
py -3.12 -X utf8 -B launch_simple.py --runtime-root ./runtime --port 58446
```

Open `http://127.0.0.1:58446/`. The supplied launcher uses a repository-local runtime directory and an alternate port. It does not point to the original installation's database or desktop shortcuts.

Starting the process leaves trading disabled, but background workers can read public market data and query the configured account. The **Start strategy** control enables order execution; it is not a paper-trading button. **Pause new entries** prevents new entries while existing strategy positions continue to be managed. It is not a complete shutdown or an emergency liquidation control.

Do not enable this copy against an account already controlled by another instance. A local owner lock protects one runtime directory, not the exchange account across different directories or computers. Do not use `--restart` against an installation managing live positions without a separate, deliberate migration procedure.

## Strategy composition

| Component | Market | Main mechanism |
| --- | --- | --- |
| M784 | BTC and ETH perpetuals | Joint 15-minute breakouts and completed-data weekly-mean setups; per-symbol 4/10/20 leverage states; retained-profit budget; initial and delayed protective stops |
| A787 | Eligible altcoin perpetuals | Leadership events and ranked strength; 2/3/5/10 leverage selection under risk constraints; one permitted add-on; dynamic protection and lifecycle exits |
| S800 | Eligible stock-proxy contracts | Completed 5-minute momentum; pre-entry signal-fade cancellation; received-result sizing; partial-position protection and time-based exit requests |

The original C801 reference starts each sleeve with one third of total capital. Sleeve profits remain in that sleeve, without cross-sleeve borrowing or periodic rebalancing. Strategy details are implemented under `c801/strategies/` and the corresponding controllers. The source snapshot does not claim new return targets, new out-of-sample evidence, or guaranteed future performance.

## Repository layout

```text
c801/                  Strategy, market data, execution, persistence, and recovery
c801/strategies/       M784, A787, and S800 signal and risk rules
simple_app.py          Application entry point
launch_simple.py       Local launcher and explicitly requested restart support
simple_index.html      English web dashboard
configure_api.py       Local credential configuration window
credential_store.py    Windows Credential Manager integration
assets/                Local application images
```

## Language and source fidelity

The dashboard, setup-window labels, launch messages, and this README are in English. Strategy and execution modules are copied unchanged from the installed source to preserve trading behavior. Some internal diagnostic messages and comments therefore remain in Chinese; this is deliberate rather than an untested rewrite of the execution engine. Technical diagnostics shown by the dashboard retain their original wording when no presentation translation is provided.

## Data and recovery

Use one private runtime directory per installation. The SQLite database contains financial state and must never be committed. Back up or migrate a live database through a coordinated procedure; copying only the main SQLite file while its WAL is active is not a complete backup. A fresh runtime directory is not a migration of existing positions or budget history.

## Snapshot verification

`source_snapshot.json` records the installed-source hashes and the source-only export scope. The upload process checks copied engine files against those hashes, parses Python files, and checks dashboard JavaScript syntax. These checks do not connect to Binance, place orders, or restart the existing application. They are not an execution certification or a fresh backtest.

This repository intentionally contains no CI/CD deployment hooks and no production credentials. Uploading it does not change the existing running installation.

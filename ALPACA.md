# Alpaca data adapter

The workstation now supports Alpaca as a second market-data provider while keeping the same `QuoteEvent` / `TradeEvent` contract used by IBKR.

## Architecture

```text
LIVE
├── IBKR TWS / Gateway -> IBKRAdapter --------┐
└── Alpaca WebSocket   -> AlpacaAdapter ------┤
                                             v
                                  QuoteEvent / TradeEvent
                                             |
                                      OrderFlowEngine
                                             |
                                      SQLite + UI / WS

REPLAY
Alpaca Historical REST -> local recording SQLite -> ReplayRuntime(mode=ro)
IBKR live recording    -> local recording SQLite -> ReplayRuntime(mode=ro)
```

Replay never calls Alpaca over the network. Historical data is downloaded first and becomes an immutable local recording source. This keeps seek, speed changes and repeated tests deterministic and prevents API timing from changing the replay.

## Credentials

Put credentials in the repository-local `.env` file so they never reach browser JavaScript or source control:

```dotenv
APCA_API_KEY_ID=your_alpaca_api_key
APCA_API_SECRET_KEY=your_alpaca_api_secret
```

The application loads `/Users/pt/DEV/stock_orderflow/.env` automatically. The file is ignored by Git. Existing shell environment variables take precedence. The longer `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` names are also accepted.

You can also use environment variables directly:

```powershell
$env:APCA_API_KEY_ID="..."
$env:APCA_API_SECRET_KEY="..."
```

Command-line `--alpaca-key` and `--alpaca-secret` exist for local testing, but environment variables are preferred.

## LIVE: free IEX WebSocket

```powershell
.\.venv\Scripts\python.exe -m orderflow_ibkr.workstation `
  --source live `
  --provider alpaca `
  --alpaca-feed iex `
  --symbols XE SNDK `
  --db data/alpaca_live.sqlite `
  --http-port 8765
```

The adapter subscribes to both `trades` and `quotes` and maps them into the same runtime event model as IBKR.

## LIVE: SIP WebSocket

If the account has the required SIP market-data entitlement:

```powershell
.\.venv\Scripts\python.exe -m orderflow_ibkr.workstation `
  --source live `
  --provider alpaca `
  --alpaca-feed sip `
  --symbols XE SNDK `
  --db data/alpaca_sip_live.sqlite `
  --http-port 8765
```

Entitlements are not hard-coded. Alpaca's authentication/subscription response is the source of truth, and subscription errors are surfaced instead of silently falling back to another feed.

## Download a full market day for replay

Regular session, 09:30-16:00 ET:

```powershell
.\.venv\Scripts\python.exe -m orderflow_ibkr.alpaca_history `
  --symbols XE `
  --date 2026-08-13 `
  --feed sip `
  --session regular `
  --db data/alpaca_replay.sqlite
```

Extended session, 04:00-20:00 ET:

```powershell
.\.venv\Scripts\python.exe -m orderflow_ibkr.alpaca_history `
  --symbols XE SNDK `
  --date 2026-08-13 `
  --feed sip `
  --session extended `
  --db data/alpaca_replay.sqlite
```

The downloader stores both historical quotes and historical trades. Trades alone are not sufficient for this order-flow engine because aggressor classification uses contemporaneous bid/ask context.

Each Alpaca response page is requested with `limit=10000`, pagination continues until `next_page_token` is exhausted, and the client has configurable request pacing plus 429 retry/backoff:

```powershell
--requests-per-minute 180
```

Raise this only when the active Alpaca plan permits a higher request rate.

## Replay the downloaded day

The downloader prints the generated `session_id`. Use it directly:

```powershell
.\.venv\Scripts\python.exe -m orderflow_ibkr.workstation `
  --source replay `
  --symbols XE `
  --replay-db data/alpaca_replay.sqlite `
  --replay-session alpaca-sip-2026-08-13-XXXXXXXX `
  --replay-duration 390 `
  --replay-speed 1 `
  --http-port 8765
```

`ReplayRuntime` opens that database using SQLite `mode=ro` and `PRAGMA query_only=ON`. It preserves the recorded source quality label, for example `ALPACA_SIP_TRADES_QUOTES`, instead of relabeling Alpaca recordings as IBKR TBT.

## Data semantics

- Alpaca trade messages provide exchange, price, size, conditions and nanosecond RFC-3339 timestamps.
- Alpaca quote messages provide bid/ask prices, bid/ask sizes, exchange codes, conditions and nanosecond timestamps; the current common event model persists the price/size fields used by the engine.
- Historical rows are stored with `aggressor=UNKNOWN`. During replay the existing engine classifies each print from the most recent recorded BBO, then falls back to midpoint/tick-rule logic exactly as it does for live events.
- The SQLite schema remains the compatibility layer between providers and replay.

## Validation

Tests cover:

- nanosecond timestamp parsing
- Alpaca trade/quote -> common event mapping
- IEX/SIP quality/source labeling
- New York DST-aware regular-session boundaries
- Alpaca historical rows -> `ReplayRuntime`
- replay source remains read-only

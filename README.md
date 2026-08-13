# OrderFlowMap // IBKR

A local-first Interactive Brokers realtime radar and executed order-flow workstation.

This fork replaces the original OpenAlgo/NSE live adapter with an **IBKR TWS / IB Gateway** backend while preserving the MIT-licensed OrderFlowMap visualization concept. The production design deliberately separates wide-universe scanning from scarce tick-by-tick capacity.

## Two realtime modes

| Mode | Intended universe | IBKR source | What is trustworthy |
|---|---:|---|---|
| **FOCUS** | up to 5 symbols with the default 100 market-data lines | `reqTickByTickData("AllLast")` + `reqMktData()` BBO context | True execution prints, executed footprint, Delta/CVD; BBO remains L1 market-data context |
| **RADAR** | larger universe, typically 10–100 symbols | `reqMktData()` + generic ticks `233,293,294,295,375` | Price/BBO/activity/volume-rate scan; reconstructed trade flow is explicitly a proxy |
| **AUTO** | automatic | <=5 → FOCUS, >5 → RADAR | Same quality rules as selected mode |

The product intentionally caps FOCUS at five symbols even if an account has more capacity. With 100 market-data lines, IBKR's specialized tick-by-tick allocation is normally 5 simultaneous requests, so five `AllLast` subscriptions fit cleanly while quotes continue through `reqMktData()`.

### Important quality boundary

`RADAR` does **not** pretend that a 250 ms-ish L1 market-data stream is true Time & Sales. If cumulative volume rises between snapshots, the application can reconstruct a volume-delta trade proxy, but every such row and every derived metric is labeled:

`MKTDATA_SNAPSHOT_PROXY`

FOCUS rows are labeled:

`TBT_TRADES_MKTDATA_QUOTES`

The UI surfaces these labels continuously.

## Current analytics

- aggressive buy/sell classification using BBO, midpoint, then tick-rule fallback
- 15-second executed/proxy Delta
- session CVD
- executed/proxy price-level footprint
- trade/activity rate
- volume rate
- quote imbalance
- bid absorption
- offer absorption
- seller exhaustion
- buyer exhaustion
- directional price impact
- robust large-trade score
- cross-symbol radar ranking
- signal persistence/cooldown

### Absorption interpretation

The engine does not define support as "a large bid is visible". It looks for realized pressure plus muted price response. For example, heavy aggressive selling with little downside progress raises **Bid Absorption**. This is deliberately based on executed flow rather than claiming knowledge of deeper resting liquidity.

## No fake L2

This version does not yet call `reqMktDepth()`. Therefore it does **not** claim to observe:

- deeper resting liquidity walls
- queue position
- multi-level order-book imbalance
- cancellation dynamics
- spoofing
- true Bookmap-style historical L2 heatmap

Those features belong to a later L2 module and require the corresponding IBKR market-depth entitlement.

## SQLite is the fact layer

The browser UI is only one consumer. All raw and derived data is stored locally in SQLite using **WAL mode**, so another process or an AI agent can open the database concurrently in strict read-only mode while the market-data writer continues running.

Default path:

```text
data/orderflow.sqlite
```

Stored tables:

| Table | Purpose |
|---|---|
| `sessions` | run metadata and mode |
| `subscriptions` | per-symbol source/quality contract |
| `raw_quotes` | event-level reqMktData quote/generic-tick snapshots |
| `raw_trades` | true TBT executions or explicitly marked RADAR proxies |
| `orderflow_metrics` | 1 Hz derived microstructure state |
| `signals` | scored absorption/exhaustion/activity events |
| `radar_rankings` | cross-symbol ranking snapshots every ~2 s |
| `latest_state` | reserved compact state cache |

Raw events and derived metrics are deliberately separate so an agent can audit how a signal was produced instead of seeing only a final score.

High-frequency inserts are written by a dedicated batching thread. SQLite runs with `journal_mode=WAL`, `synchronous=NORMAL`, and a busy timeout so read-only queries do not block the market-data callback path.

## Read-only Agent access

Use the included helper. It opens SQLite with URI `mode=ro` and `PRAGMA query_only=ON`.

```bash
python -m orderflow_ibkr.agent_read --query latest
python -m orderflow_ibkr.agent_read --query latest --symbols TER KLAC
python -m orderflow_ibkr.agent_read --query signals --symbols TER --limit 50
python -m orderflow_ibkr.agent_read --query trades --symbols TER --limit 200
python -m orderflow_ibkr.agent_read --query quotes --symbols TER --limit 200
python -m orderflow_ibkr.agent_read --query rankings --limit 30
```

An external agent can also connect directly:

```python
import sqlite3

conn = sqlite3.connect(
    "file:data/orderflow.sqlite?mode=ro",
    uri=True,
)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA query_only=ON")
```

Example question the SQL layer can answer during the session:

```sql
WITH x AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY ts_ns DESC) rn
  FROM orderflow_metrics
)
SELECT symbol, last_price, delta, cvd,
       bid_absorption, offer_absorption,
       seller_exhaustion, buyer_exhaustion,
       activity_score, confidence, quality
FROM x
WHERE rn=1
ORDER BY activity_score DESC;
```

## Install

Python 3.11+ is recommended.

```bash
git clone https://github.com/pttangent/OrderFlowMap_IBKR.git
cd OrderFlowMap_IBKR
git checkout agent/ibkr-dual-mode-adapter
pip install -e .
```

Enable API connections in TWS or IB Gateway and make sure your market-data subscriptions are available to the API session.

Paper TWS commonly uses port `7497`; live TWS commonly uses `7496`. Verify your own TWS/Gateway settings rather than assuming the defaults.

The backend connects with `readonly=True` and uses a separate default client ID (`4712`).

## Run

### Five-symbol FOCUS

```bash
orderflow-ibkr \
  --symbols TER KLAC AEHR COHU FORM \
  --mode focus
```

or:

```bash
python -m orderflow_ibkr.server \
  --symbols TER KLAC AEHR COHU FORM \
  --mode auto
```

AUTO selects FOCUS for five or fewer symbols.

### Wider reqMktData RADAR

```bash
python -m orderflow_ibkr.server \
  --symbols AAPL MSFT NVDA AMD AVGO MU TER KLAC LRCX AMAT ONTO COHU FORM AEHR PDFS \
  --mode radar
```

AUTO selects RADAR when the symbol count exceeds five.

### Explicit connection/settings

```bash
python -m orderflow_ibkr.server \
  --symbols TER KLAC \
  --mode focus \
  --ib-host 127.0.0.1 \
  --ib-port 7497 \
  --client-id 4712 \
  --market-data-lines 100 \
  --db data/orderflow.sqlite \
  --http-port 8765
```

Open:

```text
http://127.0.0.1:8765
```

The frontend connects to the local backend at `/ws`; no broker credential or API key is exposed to browser JavaScript.

## Architecture

```text
                   IBKR TWS / IB Gateway
                           |
              +------------+------------+
              |                         |
         FOCUS <= 5                   RADAR > 5
       AllLast true TBT              reqMktData
       + L1 BBO context        + RTVolume/rate ticks
              |                         |
              +------------+------------+
                           |
                    Common event model
                 QuoteEvent / TradeEvent
                           |
                    OrderFlow Engine
            Delta / CVD / absorption / etc.
                           |
             +-------------+-------------+
             |                           |
        SQLite WAL                    WebSocket
    raw + metrics + signals              |
             |                           |
      read-only Agent                   UI
```

## Storage cadence

- raw quotes: event-level packets carrying reqMktData changes
- raw TBT trades: every received AllLast execution in FOCUS
- RADAR proxy trades: only when cumulative volume increases between market-data snapshots
- derived metrics: throttled to ~1 Hz per symbol
- cross-symbol rankings: ~2-second cadence
- duplicate persistent signals: 15-second cooldown

The SQLite writer batches inserts off the market-data callback path.

## Development

```bash
pip install -e ".[dev]"
pytest -q
python -m compileall -q orderflow_ibkr
```

CI also extracts the inline frontend JavaScript and runs `node --check`.

## Roadmap

1. dynamic TBT allocator: wide RADAR automatically promotes the most abnormal symbols into scarce TBT slots
2. optional `BidAsk` tick-by-tick mode for 2-symbol deep focus
3. `reqMktDepth()` L2 adapter and real historical liquidity heatmap
4. replay from SQLite without reconnecting to IBKR
5. configurable session/price-level baselines and time-of-day normalization
6. optional desktop packaging with Tauri

## License and attribution

MIT License. This fork derives its visualization concept from `Azhagesan-dev/OrderFlowMap` and retains the original MIT license terms.

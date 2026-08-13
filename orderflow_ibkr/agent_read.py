from __future__ import annotations

import argparse
import json
from pathlib import Path

from .storage import open_readonly


QUERIES = {
    "latest": """
        WITH ranked AS (
          SELECT *, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY ts_ns DESC) AS rn
          FROM orderflow_metrics
          WHERE (? = '' OR instr(',' || ? || ',', ',' || symbol || ',') > 0)
        )
        SELECT symbol, ts_ns, last_price, delta, cvd, trades_per_sec, volume_per_sec,
               quote_imbalance, bid_absorption, offer_absorption,
               seller_exhaustion, buyer_exhaustion, activity_score, confidence, quality,
               details_json
        FROM ranked WHERE rn=1 ORDER BY activity_score DESC
    """,
    "signals": """
        SELECT symbol, ts_ns, signal_type, direction, score, price, quality, explanation, evidence_json
        FROM signals
        WHERE (? = '' OR instr(',' || ? || ',', ',' || symbol || ',') > 0)
        ORDER BY ts_ns DESC LIMIT ?
    """,
    "trades": """
        SELECT symbol, ts_ns, price, size, exchange, aggressor, aggressor_confidence, source, quality
        FROM raw_trades
        WHERE (? = '' OR instr(',' || ? || ',', ',' || symbol || ',') > 0)
        ORDER BY ts_ns DESC LIMIT ?
    """,
    "quotes": """
        SELECT symbol, ts_ns, bid, ask, bid_size, ask_size, last, last_size, volume, vwap,
               trade_count, trade_rate, volume_rate, source, quality
        FROM raw_quotes
        WHERE (? = '' OR instr(',' || ? || ',', ',' || symbol || ',') > 0)
        ORDER BY ts_ns DESC LIMIT ?
    """,
    "rankings": """
        SELECT symbol, ts_ns, rank, score, activity_score, flow_score, absorption_score, quality
        FROM radar_rankings
        WHERE ts_ns=(SELECT MAX(ts_ns) FROM radar_rankings)
        ORDER BY rank ASC LIMIT ?
    """,
}


def main() -> None:
    p = argparse.ArgumentParser(description="Read OrderFlowMap SQLite in strict read-only mode")
    p.add_argument("--db", default="data/orderflow.sqlite")
    p.add_argument("--query", choices=QUERIES, default="latest")
    p.add_argument("--symbols", nargs="*", default=[])
    p.add_argument("--limit", type=int, default=100)
    args = p.parse_args()

    symbols = ",".join(s.upper() for s in args.symbols)
    conn = open_readonly(Path(args.db))
    try:
        if args.query == "latest":
            params = (symbols, symbols)
        elif args.query in {"signals", "trades", "quotes"}:
            params = (symbols, symbols, args.limit)
        else:
            params = (args.limit,)
        rows = [dict(r) for r in conn.execute(QUERIES[args.query], params).fetchall()]
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()

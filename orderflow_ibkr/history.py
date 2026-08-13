from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .storage import open_readonly


def _bounded(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def load_history(
    path: str | Path,
    *,
    session_id: str,
    symbol: str,
    seconds: int = 900,
    quote_limit: int = 5000,
    trade_limit: int = 5000,
    metric_limit: int = 2000,
    signal_limit: int = 200,
) -> dict[str, Any]:
    """Load a bounded, read-only warm-start payload for the flow UI.

    The active session is intentionally used as the hard boundary. This keeps a
    browser refresh from silently mixing yesterday's/previous-run order flow
    into the current live session while still allowing the UI to restore all
    history already captured by the running process.
    """

    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("symbol is required")

    seconds = _bounded(seconds, 60, 7200)
    quote_limit = _bounded(quote_limit, 100, 20_000)
    trade_limit = _bounded(trade_limit, 100, 20_000)
    metric_limit = _bounded(metric_limit, 60, 10_000)
    signal_limit = _bounded(signal_limit, 10, 1000)

    conn = open_readonly(path)
    try:
        anchor = conn.execute(
            """
            SELECT MAX(ts_ns) AS ts_ns FROM (
              SELECT MAX(ts_ns) AS ts_ns FROM raw_quotes WHERE session_id=? AND symbol=?
              UNION ALL
              SELECT MAX(ts_ns) AS ts_ns FROM raw_trades WHERE session_id=? AND symbol=?
              UNION ALL
              SELECT MAX(ts_ns) AS ts_ns FROM orderflow_metrics WHERE session_id=? AND symbol=?
            )
            """,
            (session_id, symbol, session_id, symbol, session_id, symbol),
        ).fetchone()["ts_ns"]

        if anchor is None:
            return {
                "session_id": session_id,
                "symbol": symbol,
                "anchor_ns": None,
                "seconds": seconds,
                "quotes": [],
                "trades": [],
                "metrics": [],
                "signals": [],
            }

        cutoff = int(anchor) - seconds * 1_000_000_000

        # Query newest bounded rows first so a high-volume symbol cannot create
        # an unbounded response, then reverse to chronological order for charts.
        quotes = conn.execute(
            """
            SELECT * FROM (
              SELECT ts_ns,bid,ask,bid_size,ask_size,last,last_size,volume,vwap,
                     trade_count,trade_rate,volume_rate,source,quality
              FROM raw_quotes
              WHERE session_id=? AND symbol=? AND ts_ns>=?
              ORDER BY ts_ns DESC LIMIT ?
            ) ORDER BY ts_ns ASC
            """,
            (session_id, symbol, cutoff, quote_limit),
        ).fetchall()

        trades = conn.execute(
            """
            SELECT * FROM (
              SELECT ts_ns,price,size,exchange,conditions_json,aggressor,
                     aggressor_confidence,source,quality
              FROM raw_trades
              WHERE session_id=? AND symbol=? AND ts_ns>=?
              ORDER BY ts_ns DESC LIMIT ?
            ) ORDER BY ts_ns ASC
            """,
            (session_id, symbol, cutoff, trade_limit),
        ).fetchall()

        metrics = conn.execute(
            """
            SELECT * FROM (
              SELECT ts_ns,window_sec,last_price,spread_bps,buy_volume,sell_volume,
                     delta,cvd,trades_per_sec,volume_per_sec,quote_imbalance,
                     bid_absorption,offer_absorption,seller_exhaustion,buyer_exhaustion,
                     buy_price_impact_bps,sell_price_impact_bps,large_trade_score,
                     activity_score,confidence,quality,details_json
              FROM orderflow_metrics
              WHERE session_id=? AND symbol=? AND ts_ns>=?
              ORDER BY ts_ns DESC LIMIT ?
            ) ORDER BY ts_ns ASC
            """,
            (session_id, symbol, cutoff, metric_limit),
        ).fetchall()

        signals = conn.execute(
            """
            SELECT ts_ns,signal_type,direction,score,price,quality,explanation,evidence_json
            FROM signals
            WHERE session_id=? AND symbol=? AND ts_ns>=?
            ORDER BY ts_ns DESC LIMIT ?
            """,
            (session_id, symbol, cutoff, signal_limit),
        ).fetchall()

        def normalize(row):
            out = dict(row)
            if "conditions_json" in out:
                raw = out.pop("conditions_json")
                try:
                    out["conditions"] = json.loads(raw) if raw else []
                except Exception:
                    out["conditions"] = []
            return out

        return {
            "session_id": session_id,
            "symbol": symbol,
            "anchor_ns": int(anchor),
            "seconds": seconds,
            "quotes": [normalize(r) for r in quotes],
            "trades": [normalize(r) for r in trades],
            "metrics": [normalize(r) for r in metrics],
            "signals": [normalize(r) for r in signals],
        }
    finally:
        conn.close()

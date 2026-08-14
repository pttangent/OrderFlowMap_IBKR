from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from orderflow_ibkr.replay_runtime import ReplayRuntime
from orderflow_ibkr.storage import DDL


def _build_recording(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(DDL)
    session = "source-session"
    base = 1_700_000_000_000_000_000
    for i, symbol in enumerate(("XE", "SNDK")):
        for j in range(4):
            ts = base + j * 1_000_000_000 + i * 100_000_000
            conn.execute(
                """
                INSERT INTO raw_quotes(
                  session_id,symbol,ts_ns,bid,ask,bid_size,ask_size,last,last_size,
                  volume,vwap,trade_count,trade_rate,volume_rate,source,quality
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (session, symbol, ts, 10+j, 10.1+j, 100, 120, 10.05+j, 10, 1000+j*10,
                 10.0+j, 1, 2, 3, "REQ_MKT_DATA", "TBT_TRADES_MKTDATA_QUOTES"),
            )
            conn.execute(
                """
                INSERT INTO raw_trades(
                  session_id,symbol,ts_ns,price,size,exchange,conditions_json,
                  aggressor,aggressor_confidence,source,quality
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (session, symbol, ts + 10_000_000, 10.05+j, 5+j, "SIM", "[]",
                 "BUY" if j % 2 == 0 else "SELL", .9, "TBT_ALL_LAST", "TBT_TRADES_MKTDATA_QUOTES"),
            )
    conn.commit()
    conn.close()


def _counts(path: Path) -> tuple[int, int, int]:
    conn = sqlite3.connect(path)
    vals = (
        conn.execute("SELECT COUNT(*) FROM raw_quotes").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM raw_trades").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM orderflow_metrics").fetchone()[0],
    )
    conn.close()
    return vals


def test_replay_runtime_never_writes_source_db(tmp_path: Path) -> None:
    db = tmp_path / "recording.sqlite"
    _build_recording(db)
    before = _counts(db)

    async def run() -> None:
        r = ReplayRuntime(symbols=["XE", "SNDK"], db_path=db, duration_sec=60, speed=100)
        await r.start()
        assert r.status["runtime_mode"] == "REPLAY"
        assert r.status["read_only"] is True
        assert r.writer.written == 0
        await r.seek(r.end_ns)
        assert r.latest_quotes
        assert r.latest_trades
        await r.stop()

    asyncio.run(run())
    after = _counts(db)
    assert after == before


def test_replay_events_are_globally_timestamp_sorted(tmp_path: Path) -> None:
    db = tmp_path / "recording.sqlite"
    _build_recording(db)

    async def run() -> None:
        r = ReplayRuntime(symbols=["XE", "SNDK"], db_path=db, duration_sec=60)
        await r.start()
        times = [e[0] for e in r._events]
        assert times == sorted(times)
        assert {e[2] for e in r._events} == {"XE", "SNDK"}
        await r.stop()

    asyncio.run(run())

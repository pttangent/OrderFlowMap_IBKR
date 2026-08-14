from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

from orderflow_ibkr.alpaca_history import (
    _begin_recording,
    _bounds,
    _insert_quotes,
    _insert_trades,
    _open_db,
)
from orderflow_ibkr.replay_runtime import ReplayRuntime


def test_regular_session_bounds_follow_new_york_dst() -> None:
    start, end = _bounds(date(2026, 8, 14), "regular")
    assert start.isoformat() == "2026-08-14T13:30:00+00:00"
    assert end.isoformat() == "2026-08-14T20:00:00+00:00"


def test_alpaca_historical_rows_are_replay_runtime_compatible(tmp_path: Path) -> None:
    db = tmp_path / "alpaca.sqlite"
    conn = _open_db(db)
    session_id = "alpaca-sip-test"
    qrows = [
        {"t": "2026-08-14T13:30:00.000000100Z", "bp": 10.00, "ap": 10.02, "bs": 5, "as": 7},
        {"t": "2026-08-14T13:30:02.000000100Z", "bp": 10.01, "ap": 10.03, "bs": 8, "as": 6},
    ]
    trows = [
        {"t": "2026-08-14T13:30:01.000000200Z", "p": 10.02, "s": 100, "x": "Q", "c": ["@"]},
        {"t": "2026-08-14T13:30:01.500000200Z", "p": 10.01, "s": 50, "x": "P", "c": ["@"]},
    ]
    start_ns = 1_786_714_200_000_000_000
    end_ns = start_ns + 10_000_000_000
    _begin_recording(
        conn,
        session_id=session_id,
        symbols=["XE"],
        feed="sip",
        start_ns=start_ns,
        end_ns=end_ns,
        start_text="2026-08-14T13:30:00Z",
        end_text="2026-08-14T13:30:10Z",
    )
    assert _insert_quotes(conn, session_id=session_id, symbol="XE", feed="sip", rows=qrows) == 2
    assert _insert_trades(conn, session_id=session_id, symbol="XE", feed="sip", rows=trows) == 2
    conn.close()

    async def run() -> None:
        replay = ReplayRuntime(
            symbols=["XE"],
            db_path=db,
            source_session=session_id,
            duration_sec=10,
            speed=100,
        )
        await replay.start()
        assert replay.status["source_provider"] == "ALPACA"
        assert replay.plan.quality == "ALPACA_SIP_TRADES_QUOTES"
        assert len(replay._events) == 4
        await replay.seek(replay.end_ns)
        assert replay.latest_quotes["XE"]["source"].startswith("REPLAY:ALPACA_HIST_SIP_QUOTE")
        assert replay.latest_trades["XE"]["source"].startswith("REPLAY:ALPACA_HIST_SIP_TRADE")
        assert replay.writer.written == 0
        await replay.stop()

    asyncio.run(run())

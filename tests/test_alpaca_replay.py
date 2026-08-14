from datetime import date, datetime, timezone
import sqlite3

import pytest

from orderflow_ibkr.alpaca_history import (
    _begin_recording,
    _bounds,
    _insert_quotes,
    _insert_trades,
    _iso_z,
    _open_db,
)
from orderflow_ibkr.alpaca_replay import (
    REPLAY_SYMBOL_CAP,
    _cache_session_id,
    _cached_symbol_counts,
    _initial_candidate,
    _merge_temp_symbol_into_cache,
    clean_replay_symbols,
)


def test_clean_replay_symbols_normalizes_and_deduplicates():
    assert REPLAY_SYMBOL_CAP == 5
    assert clean_replay_symbols(["xe, sndk", "XE", " nvda "]) == ["XE", "SNDK", "NVDA"]
    assert clean_replay_symbols(["A", "B", "C", "D", "E"]) == ["A", "B", "C", "D", "E"]


def test_clean_replay_symbols_enforces_five_symbol_cap():
    with pytest.raises(ValueError):
        clean_replay_symbols(["A", "B", "C", "D", "E", "F"])


def test_candidate_uses_today_only_after_free_sip_delay():
    # 2026-08-14 is EDT (UTC-4).
    before = datetime(2026, 8, 14, 20, 10, tzinfo=timezone.utc)  # 16:10 ET
    after = datetime(2026, 8, 14, 20, 20, tzinfo=timezone.utc)   # 16:20 ET
    assert _initial_candidate(before).isoformat() == "2026-08-13"
    assert _initial_candidate(after).isoformat() == "2026-08-14"


def test_completed_symbol_is_promoted_to_reusable_day_cache(tmp_path):
    day = date(2026, 8, 13)
    db = tmp_path / "alpaca_replay.sqlite"
    temp_session = "alpaca-sip-temp"
    cache_session = _cache_session_id(day)
    start_dt, end_dt = _bounds(day, "regular")
    conn = _open_db(db)
    _begin_recording(
        conn,
        session_id=temp_session,
        symbols=["XE"],
        feed="sip",
        start_ns=int(start_dt.timestamp() * 1_000_000_000),
        end_ns=int(end_dt.timestamp() * 1_000_000_000),
        start_text=_iso_z(start_dt),
        end_text=_iso_z(end_dt),
    )
    qrows = [
        {"t": "2026-08-13T13:30:00.000000100Z", "bp": 10.00, "ap": 10.02, "bs": 5, "as": 7},
        {"t": "2026-08-13T19:59:59.000000100Z", "bp": 10.10, "ap": 10.12, "bs": 8, "as": 6},
    ]
    trows = [
        {"t": "2026-08-13T13:30:01.000000200Z", "p": 10.02, "s": 100, "x": "Q", "c": ["@"]},
        {"t": "2026-08-13T19:59:58.000000200Z", "p": 10.11, "s": 50, "x": "P", "c": ["@"]},
    ]
    assert _insert_quotes(conn, session_id=temp_session, symbol="XE", feed="sip", rows=qrows) == 2
    assert _insert_trades(conn, session_id=temp_session, symbol="XE", feed="sip", rows=trows) == 2
    conn.close()

    assert _merge_temp_symbol_into_cache(
        db,
        temp_session_id=temp_session,
        cache_session_id=cache_session,
        trading_day=day,
        symbol="XE",
    ) == (2, 2)
    assert _cached_symbol_counts(db, session_id=cache_session, symbol="XE") == (2, 2)

    with sqlite3.connect(db) as check:
        assert check.execute("SELECT COUNT(*) FROM sessions WHERE session_id=?", (temp_session,)).fetchone()[0] == 0
        assert check.execute("SELECT COUNT(*) FROM sessions WHERE session_id=?", (cache_session,)).fetchone()[0] == 1

from datetime import datetime, timezone

import pytest

from orderflow_ibkr.alpaca_replay import REPLAY_SYMBOL_CAP, _initial_candidate, clean_replay_symbols


def test_clean_replay_symbols_normalizes_and_deduplicates():
    assert clean_replay_symbols(["xe, sndk", "XE", " nvda "]) == ["XE", "SNDK", "NVDA"]


def test_clean_replay_symbols_enforces_product_cap():
    with pytest.raises(ValueError):
        clean_replay_symbols([f"S{i}" for i in range(REPLAY_SYMBOL_CAP + 1)])


def test_candidate_uses_today_only_after_free_sip_delay():
    # 2026-08-14 is EDT (UTC-4).
    before = datetime(2026, 8, 14, 20, 10, tzinfo=timezone.utc)  # 16:10 ET
    after = datetime(2026, 8, 14, 20, 20, tzinfo=timezone.utc)   # 16:20 ET
    assert _initial_candidate(before).isoformat() == "2026-08-13"
    assert _initial_candidate(after).isoformat() == "2026-08-14"

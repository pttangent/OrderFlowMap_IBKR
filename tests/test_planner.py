import pytest

from orderflow_ibkr.ibkr_adapter import IBKRAdapter


def test_auto_uses_focus_for_five_symbols_at_default_lines():
    a = IBKRAdapter(symbols=["AAPL", "MSFT", "NVDA", "AMD", "TER"], mode="auto", market_data_lines=100)
    assert a.plan.active_mode == "focus"
    assert a.plan.tbt_capacity == 5
    assert a.plan.quality == "TBT_TRADES_MKTDATA_QUOTES"


def test_auto_uses_radar_for_more_than_five_symbols():
    a = IBKRAdapter(symbols=["AAPL", "MSFT", "NVDA", "AMD", "TER", "KLAC"], mode="auto", market_data_lines=100)
    assert a.plan.active_mode == "radar"
    assert a.plan.quality == "MKTDATA_SNAPSHOT_PROXY"


def test_explicit_focus_rejects_six_symbols_at_default_lines():
    with pytest.raises(ValueError):
        IBKRAdapter(symbols=["AAPL", "MSFT", "NVDA", "AMD", "TER", "KLAC"], mode="focus", market_data_lines=100)


def test_radar_rejects_more_symbols_than_configured_lines():
    with pytest.raises(ValueError):
        IBKRAdapter(symbols=[f"S{i}" for i in range(11)], mode="radar", market_data_lines=10)

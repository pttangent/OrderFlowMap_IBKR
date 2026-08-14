from __future__ import annotations

from pathlib import Path

from orderflow_ibkr.alpaca_adapter import AlpacaAdapter, _quote_size_shares, _rfc3339_to_ns
from orderflow_ibkr.alpaca_runtime import AlpacaOrderFlowRuntime


def test_rfc3339_parser_preserves_nanoseconds() -> None:
    value = _rfc3339_to_ns("2026-08-14T13:30:00.123456789Z")
    assert value % 1_000_000_000 == 123_456_789


def test_alpaca_quote_sizes_normalize_round_lots_to_shares() -> None:
    assert _quote_size_shares(9) == 900.0
    assert _quote_size_shares(None) is None


def test_alpaca_trade_and_quote_map_to_common_event_contract() -> None:
    quotes = []
    trades = []
    adapter = AlpacaAdapter(
        symbols=["XE"], feed="sip", api_key="k", api_secret="s",
        on_quote=lambda symbol, event: quotes.append((symbol, event)),
        on_trade=lambda symbol, event: trades.append((symbol, event)), reconnect=False,
    )
    adapter._handle_item(
        {"T":"q","S":"XE","bp":45.10,"bs":12,"ap":45.12,"as":9,
         "t":"2026-08-14T13:30:00.000000111Z"}
    )
    adapter._handle_item(
        {"T":"t","S":"XE","x":"Q","p":45.12,"s":300,"c":["@"],
         "t":"2026-08-14T13:30:00.000000222Z"}
    )
    assert quotes[0][0] == "XE"
    assert quotes[0][1].bid == 45.10
    assert quotes[0][1].bid_size == 1200
    assert quotes[0][1].ask_size == 900
    assert quotes[0][1].quality == "ALPACA_SIP_TRADES_QUOTES"
    assert trades[0][0] == "XE"
    assert trades[0][1].price == 45.12
    assert trades[0][1].size == 300
    assert trades[0][1].exchange == "Q"
    assert trades[0][1].conditions == ["@"]


def test_free_live_feed_plan_is_explicitly_iex() -> None:
    adapter = AlpacaAdapter(symbols=["XE", "SNDK"], feed="iex", api_key="k", api_secret="s")
    assert adapter.url.endswith("/v2/iex")
    assert adapter.plan.active_mode == "alpaca-iex"
    assert adapter.plan.trade_source == "ALPACA_WS_IEX_TRADE"
    assert adapter.plan.quality == "ALPACA_IEX_TRADES_QUOTES"


def test_alpaca_runtime_accepts_five_symbol_universe(tmp_path: Path) -> None:
    runtime = AlpacaOrderFlowRuntime(
        symbols=["XE", "SNDK", "NVDA", "VST", "GEV"],
        db_path=tmp_path / "alpaca.sqlite",
        feed="iex",
        api_key="k",
        api_secret="s",
    )
    assert runtime.adapter.plan.symbols == ["XE", "SNDK", "NVDA", "VST", "GEV"]
    assert runtime.adapter.plan.quality == "ALPACA_IEX_TRADES_QUOTES"

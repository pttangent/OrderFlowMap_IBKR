from orderflow_ibkr.engine import OrderFlowEngine, QuoteEvent, TradeEvent


def q(ts, bid=99.99, ask=100.01, bs=1000, a_s=800, quality="TBT_TRADES_MKTDATA_QUOTES"):
    return QuoteEvent(ts_ns=ts, bid=bid, ask=ask, bid_size=bs, ask_size=a_s, last=100.0, quality=quality)


def test_bid_absorption_from_heavy_sells_without_downside_progress():
    e = OrderFlowEngine()
    s = e.state("TER")
    base = 1_000_000_000_000
    s.add_quote(q(base))
    for i in range(20):
        s.add_trade(
            TradeEvent(
                ts_ns=base + (i + 1) * 100_000_000,
                price=100.0,
                size=100,
                aggressor="SELL",
                aggressor_confidence=0.95,
            )
        )
    m = s.metrics(now_ns=base + 3_000_000_000, quality="TBT_TRADES_MKTDATA_QUOTES")
    assert m["sell_volume"] == 2000
    assert m["delta"] == -2000
    assert m["bid_absorption"] >= 95
    assert m["offer_absorption"] == 0


def test_proxy_quote_reconstructs_proxy_trade_but_marks_quality():
    e = OrderFlowEngine()
    s = e.state("KLAC")
    base = 2_000_000_000_000
    s.add_quote(QuoteEvent(base, 199.9, 200.1, 500, 500, last=200.0, volume=1000, quality="MKTDATA_SNAPSHOT_PROXY"))
    tr = s.add_quote(QuoteEvent(base + 250_000_000, 199.9, 200.1, 500, 500, last=200.1, volume=1150, quality="MKTDATA_SNAPSHOT_PROXY"))
    assert tr is not None
    assert tr.size == 150
    assert tr.quality == "MKTDATA_SNAPSHOT_PROXY"
    assert tr.aggressor_confidence <= 0.55


def test_footprint_aggregates_by_price_and_side():
    e = OrderFlowEngine()
    s = e.state("AEHR")
    base = 3_000_000_000_000
    s.add_trade(TradeEvent(base, 10.00, 100, aggressor="BUY", aggressor_confidence=1))
    s.add_trade(TradeEvent(base + 1, 10.00, 40, aggressor="SELL", aggressor_confidence=1))
    s.add_trade(TradeEvent(base + 2, 10.01, 50, aggressor="BUY", aggressor_confidence=1))
    fp = s.footprint(now_ns=base + 10, sec=300, tick_size=0.01)
    row = next(r for r in fp if abs(r["price"] - 10.00) < 1e-9)
    assert row["buy"] == 100
    assert row["sell"] == 40
    assert row["delta"] == 60

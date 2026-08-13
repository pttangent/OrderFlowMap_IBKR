from orderflow_ibkr.history import load_history
from orderflow_ibkr.storage import SQLiteStore


def test_history_loader_is_current_session_and_chronological(tmp_path):
    db = tmp_path / "flow.sqlite"
    store = SQLiteStore(db)
    store.begin_session(
        session_id="s1",
        mode="focus",
        symbols=["TER"],
        ib_host="127.0.0.1",
        ib_port=7497,
        client_id=1,
        market_data_lines=100,
    )
    for ts, px in [(1_000_000_000, 100.0), (2_000_000_000, 100.1)]:
        store.insert_quote(
            {
                "session_id": "s1",
                "symbol": "TER",
                "ts_ns": ts,
                "bid": px - 0.01,
                "ask": px + 0.01,
                "last": px,
                "source": "REQ_MKT_DATA",
                "quality": "TBT_TRADES_MKTDATA_QUOTES",
            }
        )
        store.insert_trade(
            {
                "session_id": "s1",
                "symbol": "TER",
                "ts_ns": ts,
                "price": px,
                "size": 10,
                "exchange": "Q",
                "conditions_json": "[]",
                "aggressor": "BUY",
                "aggressor_confidence": 0.95,
                "source": "TBT_ALL_LAST",
                "quality": "TBT_TRADES_MKTDATA_QUOTES",
            }
        )
    store.close()

    data = load_history(db, session_id="s1", symbol="TER", seconds=60)
    assert [x["ts_ns"] for x in data["quotes"]] == [1_000_000_000, 2_000_000_000]
    assert [x["ts_ns"] for x in data["trades"]] == [1_000_000_000, 2_000_000_000]
    assert data["trades"][0]["conditions"] == []
    assert data["symbol"] == "TER"

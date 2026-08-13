from orderflow_ibkr.storage import SQLiteStore, open_readonly
from orderflow_ibkr.writer import SQLiteEventWriter


def test_wal_writer_and_readonly_reader(tmp_path):
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
    writer = SQLiteEventWriter(db, flush_ms=10, batch_size=2)
    writer.start()
    writer.submit(
        "trade",
        {
            "session_id": "s1",
            "symbol": "TER",
            "ts_ns": 123,
            "price": 100.0,
            "size": 10,
            "exchange": "Q",
            "conditions_json": "[]",
            "aggressor": "BUY",
            "aggressor_confidence": 0.95,
            "source": "TBT_ALL_LAST",
            "quality": "TBT_TRADES_MKTDATA_QUOTES",
        },
    )
    writer.submit(
        "latest",
        {
            "session_id": "s1",
            "symbol": "TER",
            "ts_ns": 124,
            "mode": "focus",
            "quality": "TBT_TRADES_MKTDATA_QUOTES",
            "state_json": '{"delta":10,"bid_absorption":80}',
        },
    )
    writer.stop()

    ro = open_readonly(db)
    try:
        row = ro.execute("SELECT symbol,price,size,quality FROM raw_trades").fetchone()
        assert dict(row)["symbol"] == "TER"
        latest = ro.execute("SELECT symbol,ts_ns,state_json FROM latest_state").fetchone()
        assert dict(latest)["ts_ns"] == 124
        assert "bid_absorption" in dict(latest)["state_json"]
        assert ro.execute("PRAGMA query_only").fetchone()[0] == 1
    finally:
        ro.close()
        store.close()

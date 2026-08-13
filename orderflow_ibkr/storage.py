from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  started_ns INTEGER NOT NULL,
  ended_ns INTEGER,
  mode TEXT NOT NULL,
  symbols_json TEXT NOT NULL,
  ib_host TEXT,
  ib_port INTEGER,
  client_id INTEGER,
  market_data_lines INTEGER,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
  session_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  mode TEXT NOT NULL,
  trade_source TEXT NOT NULL,
  quote_source TEXT NOT NULL,
  quality TEXT NOT NULL,
  started_ns INTEGER NOT NULL,
  ended_ns INTEGER,
  PRIMARY KEY(session_id, symbol),
  FOREIGN KEY(session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS raw_quotes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ns INTEGER NOT NULL,
  bid REAL,
  ask REAL,
  bid_size REAL,
  ask_size REAL,
  last REAL,
  last_size REAL,
  volume REAL,
  vwap REAL,
  trade_count REAL,
  trade_rate REAL,
  volume_rate REAL,
  source TEXT NOT NULL,
  quality TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_quotes_symbol_ts ON raw_quotes(symbol, ts_ns);
CREATE INDEX IF NOT EXISTS idx_raw_quotes_session_symbol_ts ON raw_quotes(session_id, symbol, ts_ns);

CREATE TABLE IF NOT EXISTS raw_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ns INTEGER NOT NULL,
  price REAL NOT NULL,
  size REAL NOT NULL,
  exchange TEXT,
  conditions_json TEXT,
  aggressor TEXT,
  aggressor_confidence REAL,
  source TEXT NOT NULL,
  quality TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_ts ON raw_trades(symbol, ts_ns);
CREATE INDEX IF NOT EXISTS idx_raw_trades_session_symbol_ts ON raw_trades(session_id, symbol, ts_ns);

CREATE TABLE IF NOT EXISTS orderflow_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ns INTEGER NOT NULL,
  window_sec INTEGER NOT NULL,
  last_price REAL,
  spread_bps REAL,
  buy_volume REAL,
  sell_volume REAL,
  delta REAL,
  cvd REAL,
  trades_per_sec REAL,
  volume_per_sec REAL,
  quote_imbalance REAL,
  bid_absorption REAL,
  offer_absorption REAL,
  seller_exhaustion REAL,
  buyer_exhaustion REAL,
  buy_price_impact_bps REAL,
  sell_price_impact_bps REAL,
  large_trade_score REAL,
  activity_score REAL,
  confidence REAL,
  quality TEXT NOT NULL,
  details_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol_ts ON orderflow_metrics(symbol, ts_ns);
CREATE INDEX IF NOT EXISTS idx_metrics_session_symbol_ts ON orderflow_metrics(session_id, symbol, ts_ns);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ns INTEGER NOT NULL,
  signal_type TEXT NOT NULL,
  direction TEXT,
  score REAL NOT NULL,
  price REAL,
  quality TEXT NOT NULL,
  explanation TEXT,
  evidence_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts_ns);
CREATE INDEX IF NOT EXISTS idx_signals_type_ts ON signals(signal_type, ts_ns);

CREATE TABLE IF NOT EXISTS radar_rankings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ns INTEGER NOT NULL,
  rank INTEGER,
  score REAL NOT NULL,
  activity_score REAL,
  flow_score REAL,
  absorption_score REAL,
  quality TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_radar_ts_rank ON radar_rankings(ts_ns, rank);

CREATE TABLE IF NOT EXISTS latest_state (
  symbol TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  ts_ns INTEGER NOT NULL,
  mode TEXT NOT NULL,
  quality TEXT NOT NULL,
  state_json TEXT NOT NULL
);
"""


class SQLiteStore:
    """Single-writer SQLite store configured for concurrent read-only agents.

    WAL mode allows the realtime writer and independent read-only SQLite
    connections to coexist.  Readers should use SQLite URI mode=ro.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=5.0)
        self.conn.row_factory = sqlite3.Row
        self._configure()
        self._init_schema()

    def _configure(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA busy_timeout=5000")
        cur.close()

    def _init_schema(self) -> None:
        with self._lock, self.conn:
            self.conn.executescript(DDL)
            self.conn.execute(
                "INSERT OR REPLACE INTO schema_meta(key,value) VALUES('schema_version',?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def begin_session(
        self,
        *,
        session_id: str,
        mode: str,
        symbols: list[str],
        ib_host: str,
        ib_port: int,
        client_id: int,
        market_data_lines: int,
    ) -> None:
        now = time.time_ns()
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO sessions(
                  session_id,started_ns,mode,symbols_json,ib_host,ib_port,client_id,market_data_lines
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    session_id,
                    now,
                    mode,
                    json.dumps(symbols),
                    ib_host,
                    ib_port,
                    client_id,
                    market_data_lines,
                ),
            )

    def end_session(self, session_id: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE sessions SET ended_ns=? WHERE session_id=?",
                (time.time_ns(), session_id),
            )

    def upsert_subscription(
        self,
        *,
        session_id: str,
        symbol: str,
        mode: str,
        trade_source: str,
        quote_source: str,
        quality: str,
    ) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO subscriptions(
                  session_id,symbol,mode,trade_source,quote_source,quality,started_ns
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    session_id,
                    symbol,
                    mode,
                    trade_source,
                    quote_source,
                    quality,
                    time.time_ns(),
                ),
            )

    def insert_quote(self, row: dict[str, Any]) -> None:
        cols = (
            "session_id symbol ts_ns bid ask bid_size ask_size last last_size volume vwap "
            "trade_count trade_rate volume_rate source quality"
        ).split()
        vals = [row.get(c) for c in cols]
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO raw_quotes({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                vals,
            )

    def insert_trade(self, row: dict[str, Any]) -> None:
        cols = (
            "session_id symbol ts_ns price size exchange conditions_json aggressor "
            "aggressor_confidence source quality"
        ).split()
        vals = [row.get(c) for c in cols]
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO raw_trades({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                vals,
            )

    def insert_metric(self, row: dict[str, Any]) -> None:
        cols = (
            "session_id symbol ts_ns window_sec last_price spread_bps buy_volume sell_volume "
            "delta cvd trades_per_sec volume_per_sec quote_imbalance bid_absorption "
            "offer_absorption seller_exhaustion buyer_exhaustion buy_price_impact_bps "
            "sell_price_impact_bps large_trade_score activity_score confidence quality details_json"
        ).split()
        vals = [row.get(c) for c in cols]
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO orderflow_metrics({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                vals,
            )

    def insert_signal(self, row: dict[str, Any]) -> None:
        cols = (
            "session_id symbol ts_ns signal_type direction score price quality explanation evidence_json"
        ).split()
        vals = [row.get(c) for c in cols]
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO signals({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                vals,
            )

    def replace_latest_state(
        self,
        *,
        symbol: str,
        session_id: str,
        mode: str,
        quality: str,
        state: dict[str, Any],
        ts_ns: int | None = None,
    ) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO latest_state(symbol,session_id,ts_ns,mode,quality,state_json)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                  session_id=excluded.session_id,
                  ts_ns=excluded.ts_ns,
                  mode=excluded.mode,
                  quality=excluded.quality,
                  state_json=excluded.state_json
                """,
                (
                    symbol,
                    session_id,
                    ts_ns or time.time_ns(),
                    mode,
                    quality,
                    json.dumps(state, separators=(",", ":"), default=str),
                ),
            )

    def insert_rankings(self, rows: Iterable[dict[str, Any]]) -> None:
        cols = (
            "session_id symbol ts_ns rank score activity_score flow_score absorption_score quality"
        ).split()
        payload = [[row.get(c) for c in cols] for row in rows]
        if not payload:
            return
        with self._lock, self.conn:
            self.conn.executemany(
                f"INSERT INTO radar_rankings({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
                payload,
            )


def open_readonly(path: str | Path) -> sqlite3.Connection:
    """Open a concurrent-safe read-only connection for an agent/tool."""
    resolved = Path(path).resolve().as_posix()
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=2000")
    return conn

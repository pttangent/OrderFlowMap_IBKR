from __future__ import annotations

import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class SQLiteEventWriter:
    """Batch high-frequency inserts so market-data callbacks never fsync per tick."""

    def __init__(self, path: str | Path, *, flush_ms: int = 100, batch_size: int = 500):
        self.path = str(path)
        self.flush_ms = flush_ms
        self.batch_size = batch_size
        self.q: queue.Queue[tuple[str, dict[str, Any]] | None] = queue.Queue(maxsize=100_000)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="sqlite-orderflow-writer", daemon=True)
        self.dropped = 0
        self.written = 0

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        try:
            self.q.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout=timeout)

    def submit(self, kind: str, row: dict[str, Any]) -> bool:
        try:
            self.q.put_nowait((kind, row))
            return True
        except queue.Full:
            self.dropped += 1
            return False

    @staticmethod
    def _sql(kind: str) -> tuple[str, list[str]]:
        mapping = {
            "quote": "session_id symbol ts_ns bid ask bid_size ask_size last last_size volume vwap trade_count trade_rate volume_rate source quality",
            "trade": "session_id symbol ts_ns price size exchange conditions_json aggressor aggressor_confidence source quality",
            "metric": "session_id symbol ts_ns window_sec last_price spread_bps buy_volume sell_volume delta cvd trades_per_sec volume_per_sec quote_imbalance bid_absorption offer_absorption seller_exhaustion buyer_exhaustion buy_price_impact_bps sell_price_impact_bps large_trade_score activity_score confidence quality details_json",
            "signal": "session_id symbol ts_ns signal_type direction score price quality explanation evidence_json",
            "ranking": "session_id symbol ts_ns rank score activity_score flow_score absorption_score quality",
        }
        table = {
            "quote": "raw_quotes",
            "trade": "raw_trades",
            "metric": "orderflow_metrics",
            "signal": "signals",
            "ranking": "radar_rankings",
        }[kind]
        cols = mapping[kind].split()
        sql = f"INSERT INTO {table}({','.join(cols)}) VALUES({','.join('?' for _ in cols)})"
        return sql, cols

    def _flush(self, conn: sqlite3.Connection, batch: list[tuple[str, dict[str, Any]]]) -> None:
        if not batch:
            return
        grouped: dict[str, list[dict[str, Any]]] = {}
        for kind, row in batch:
            grouped.setdefault(kind, []).append(row)
        with conn:
            for kind, rows in grouped.items():
                if kind == "latest":
                    conn.executemany(
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
                        [
                            [
                                r.get("symbol"),
                                r.get("session_id"),
                                r.get("ts_ns"),
                                r.get("mode"),
                                r.get("quality"),
                                r.get("state_json"),
                            ]
                            for r in rows
                        ],
                    )
                    continue
                sql, cols = self._sql(kind)
                conn.executemany(sql, [[r.get(c) for c in cols] for r in rows])
        self.written += len(batch)

    def _run(self) -> None:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        batch: list[tuple[str, dict[str, Any]]] = []
        deadline = time.monotonic() + self.flush_ms / 1000
        try:
            while not self._stop.is_set() or not self.q.empty():
                timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self.q.get(timeout=timeout)
                except queue.Empty:
                    item = None
                if item is not None:
                    batch.append(item)
                if len(batch) >= self.batch_size or time.monotonic() >= deadline or (item is None and batch):
                    self._flush(conn, batch)
                    batch.clear()
                    deadline = time.monotonic() + self.flush_ms / 1000
            if batch:
                self._flush(conn, batch)
        finally:
            conn.close()

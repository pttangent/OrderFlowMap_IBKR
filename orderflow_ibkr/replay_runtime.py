from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .engine import OrderFlowEngine, QuoteEvent, TradeEvent


class ReplayRuntime:
    """Read-only market-event player using the same OrderFlowEngine as LIVE.

    The recording database is always opened with SQLite URI mode=ro. Replay
    state lives only in memory and is published through the same websocket
    event contract as the live runtime. No raw events, metrics, rankings or
    latest_state rows are written back to the source recording.
    """

    def __init__(self, *, symbols: list[str], db_path: str | Path, source_session: str | None = None, start_ns: int | None = None, duration_sec: int = 1800, speed: float = 1.0) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.db_path = Path(db_path)
        self.source_session = source_session
        self.requested_start_ns = start_ns
        self.duration_sec = max(1, int(duration_sec))
        self.speed = max(0.01, float(speed))
        self.session_id = f"replay-{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self.event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
        self.engine = OrderFlowEngine()
        self.latest_metrics: dict[str, dict[str, Any]] = {}
        self.latest_quotes: dict[str, dict[str, Any]] = {}
        self.latest_trades: dict[str, dict[str, Any]] = {}
        self.status: dict[str, Any] = {"state": "starting", "runtime_mode": "REPLAY"}
        self._events: list[tuple[int, int, str, str, dict[str, Any]]] = []
        self._index = 0
        self._play_task: asyncio.Task | None = None
        self._playing = False
        self._last_metric_ns: dict[str, int] = {}
        self._last_ranking_ns = 0
        self.start_ns = 0
        self.end_ns = 0
        self.market_time_ns = 0
        self.plan = SimpleNamespace(active_mode="REPLAY", quality="TBT_TRADES_MKTDATA_QUOTES", market_data_lines=0, tbt_capacity=0, trade_source="RECORDED_TBT", quote_source="RECORDED_L1")

    @property
    def writer(self) -> SimpleNamespace:
        return SimpleNamespace(written=0, dropped=0)

    def _publish(self, payload: dict[str, Any]) -> None:
        payload.setdefault("session_id", self.session_id)
        payload.setdefault("runtime_mode", "REPLAY")
        try:
            self.event_queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    def _connect_ro(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def _choose_source_session(self, conn: sqlite3.Connection) -> str:
        if self.source_session:
            return self.source_session
        placeholders = ",".join("?" for _ in self.symbols)
        rows = conn.execute(
            f"SELECT session_id, COUNT(DISTINCT symbol) AS n_symbols, MIN(ts_ns) AS lo, MAX(ts_ns) AS hi FROM raw_trades WHERE symbol IN ({placeholders}) GROUP BY session_id HAVING n_symbols = ? ORDER BY (hi-lo) DESC, hi DESC",
            [*self.symbols, len(self.symbols)],
        ).fetchall()
        if not rows:
            raise RuntimeError(f"No recording session contains all symbols: {', '.join(self.symbols)}")
        return str(rows[0]["session_id"])

    def _load_events(self) -> None:
        with self._connect_ro() as conn:
            self.source_session = self._choose_source_session(conn)
            placeholders = ",".join("?" for _ in self.symbols)
            bounds = conn.execute(
                f"SELECT MAX(lo) AS common_lo, MIN(hi) AS common_hi FROM (SELECT symbol, MIN(ts_ns) lo, MAX(ts_ns) hi FROM raw_quotes WHERE session_id=? AND symbol IN ({placeholders}) GROUP BY symbol)",
                [self.source_session, *self.symbols],
            ).fetchone()
            common_lo = int(bounds["common_lo"] or 0)
            common_hi = int(bounds["common_hi"] or 0)
            if not common_lo or common_hi <= common_lo:
                raise RuntimeError("No common quote coverage for requested replay symbols")
            self.start_ns = max(common_lo, int(self.requested_start_ns or common_lo))
            self.end_ns = min(common_hi, self.start_ns + self.duration_sec * 1_000_000_000)
            if self.end_ns <= self.start_ns:
                raise RuntimeError("Replay interval has no data")
            args = [self.source_session, self.start_ns, self.end_ns, *self.symbols]
            qrows = conn.execute(f"SELECT * FROM raw_quotes WHERE session_id=? AND ts_ns BETWEEN ? AND ? AND symbol IN ({placeholders}) ORDER BY ts_ns, id", args).fetchall()
            trows = conn.execute(f"SELECT * FROM raw_trades WHERE session_id=? AND ts_ns BETWEEN ? AND ? AND symbol IN ({placeholders}) ORDER BY ts_ns, id", args).fetchall()
        events: list[tuple[int, int, str, str, dict[str, Any]]] = []
        for r in qrows:
            d = dict(r); events.append((int(d["ts_ns"]), 0, str(d["symbol"]), "quote", d))
        for r in trows:
            d = dict(r); events.append((int(d["ts_ns"]), 1, str(d["symbol"]), "trade", d))
        events.sort(key=lambda e: (e[0], e[1]))
        if not events:
            raise RuntimeError("Replay interval contains no events")
        self._events = events
        self.start_ns = events[0][0]
        self.end_ns = events[-1][0]
        self.market_time_ns = self.start_ns

    def _reset_state(self) -> None:
        self.engine = OrderFlowEngine()
        self.latest_metrics.clear(); self.latest_quotes.clear(); self.latest_trades.clear()
        self._last_metric_ns.clear(); self._last_ranking_ns = 0; self._index = 0; self.market_time_ns = self.start_ns

    def _emit_quote(self, symbol: str, row: dict[str, Any]) -> None:
        q = QuoteEvent(ts_ns=int(row["ts_ns"]), bid=row.get("bid"), ask=row.get("ask"), bid_size=row.get("bid_size"), ask_size=row.get("ask_size"), last=row.get("last"), last_size=row.get("last_size"), volume=row.get("volume"), vwap=row.get("vwap"), trade_count=row.get("trade_count"), trade_rate=row.get("trade_rate"), volume_rate=row.get("volume_rate"), source="REPLAY_RECORDED_L1", quality=str(row.get("quality") or self.plan.quality))
        self.engine.state(symbol).add_quote(q)
        out = {**asdict(q), "session_id": self.session_id, "symbol": symbol, "source_session": self.source_session}
        self.latest_quotes[symbol] = out
        self._publish({"type": "quote", "symbol": symbol, "data": out})
        self._maybe_compute(symbol, q.ts_ns)

    def _emit_trade(self, symbol: str, row: dict[str, Any]) -> None:
        try:
            conditions = json.loads(row.get("conditions_json") or "[]")
        except Exception:
            conditions = []
        t = TradeEvent(ts_ns=int(row["ts_ns"]), price=float(row["price"]), size=float(row["size"]), exchange=row.get("exchange"), conditions=conditions, aggressor=str(row.get("aggressor") or "UNKNOWN"), aggressor_confidence=float(row.get("aggressor_confidence") or 0.0), source="REPLAY_RECORDED_TBT", quality=str(row.get("quality") or self.plan.quality))
        self.engine.state(symbol).add_trade(t)
        out = {**asdict(t), "session_id": self.session_id, "symbol": symbol, "source_session": self.source_session}
        self.latest_trades[symbol] = out
        self._publish({"type": "trade", "symbol": symbol, "data": out})
        self._maybe_compute(symbol, t.ts_ns)

    def _maybe_compute(self, symbol: str, now_ns: int) -> None:
        if now_ns - self._last_metric_ns.get(symbol, 0) < 1_000_000_000:
            return
        self._last_metric_ns[symbol] = now_ns
        m = self.engine.state(symbol).metrics(now_ns=now_ns, window_sec=15, quality=self.plan.quality)
        self.latest_metrics[symbol] = m
        footprint = self.engine.state(symbol).footprint(now_ns=now_ns, sec=300, tick_size=0.01)
        self._publish({"type": "metrics", "symbol": symbol, "data": m, "footprint": footprint[:120]})
        if now_ns - self._last_ranking_ns >= 2_000_000_000:
            self._last_ranking_ns = now_ns
            items = sorted(self.latest_metrics.items(), key=lambda kv: kv[1].get("score", 0.0), reverse=True)
            self._publish({"type": "ranking", "data": [{"session_id": self.session_id, "symbol": s, "ts_ns": now_ns, "rank": i, "score": m.get("score", 0.0), "activity_score": m.get("activity_score", 0.0), "flow_score": m.get("flow_score", 50.0), "absorption_score": m.get("absorption_score", 0.0), "quality": m.get("quality", self.plan.quality)} for i, (s, m) in enumerate(items, 1)]})

    def _step_one(self) -> bool:
        if self._index >= len(self._events): return False
        ts_ns, _, symbol, kind, row = self._events[self._index]; self._index += 1; self.market_time_ns = ts_ns
        self._emit_quote(symbol, row) if kind == "quote" else self._emit_trade(symbol, row)
        return True

    async def _play_loop(self) -> None:
        try:
            while self._playing and self._index < len(self._events):
                current_ts = self._events[self._index][0]
                previous_ts = self.market_time_ns or current_ts
                delay = max(0.0, (current_ts - previous_ts) / 1_000_000_000 / self.speed)
                if delay:
                    # 1x means one market second equals one wall-clock second.
                    await asyncio.sleep(delay)
                if not self._playing: break
                self._step_one()
            if self._index >= len(self._events):
                self._playing = False; self._publish({"type": "replay", "data": self.replay_state()})
        except asyncio.CancelledError:
            raise

    async def play(self) -> None:
        self._playing = True
        if not self._play_task or self._play_task.done(): self._play_task = asyncio.create_task(self._play_loop())
        self._publish({"type": "replay", "data": self.replay_state()})

    async def pause(self) -> None:
        self._playing = False; self._publish({"type": "replay", "data": self.replay_state()})

    async def set_speed(self, speed: float) -> None:
        self.speed = max(0.01, min(float(speed), 1000.0)); self._publish({"type": "replay", "data": self.replay_state()})

    async def seek(self, target_ns: int) -> None:
        was_playing = self._playing; self._playing = False; self._reset_state()
        target_ns = max(self.start_ns, min(int(target_ns), self.end_ns))
        while self._index < len(self._events) and self._events[self._index][0] <= target_ns: self._step_one()
        self.market_time_ns = target_ns
        self._publish({"type": "snapshot", "data": self.snapshot()}); self._publish({"type": "replay", "data": self.replay_state()})
        if was_playing: await self.play()

    async def next_bar(self, seconds: int = 30) -> None: await self.seek(self.market_time_ns + seconds * 1_000_000_000)
    async def prev_bar(self, seconds: int = 30) -> None: await self.seek(self.market_time_ns - seconds * 1_000_000_000)

    async def start(self) -> None:
        self._load_events()
        self.status = {"state": "running", "runtime_mode": "REPLAY", "session_id": self.session_id, "source_session": self.source_session, "mode": "REPLAY", "quality": self.plan.quality, "symbols": self.symbols, "db_path": str(self.db_path.resolve()), "read_only": True, "start_ns": self.start_ns, "end_ns": self.end_ns, "market_time_ns": self.market_time_ns}
        self._publish({"type": "status", "data": self.status}); self._publish({"type": "replay", "data": self.replay_state()})

    async def stop(self) -> None:
        self._playing = False
        if self._play_task:
            self._play_task.cancel()
            try: await self._play_task
            except asyncio.CancelledError: pass

    def replay_state(self) -> dict[str, Any]:
        duration = max(1, self.end_ns - self.start_ns); progress = max(0.0, min(1.0, (self.market_time_ns - self.start_ns) / duration))
        return {"runtime_mode": "REPLAY", "playing": self._playing, "speed": self.speed, "market_time_ns": self.market_time_ns, "start_ns": self.start_ns, "end_ns": self.end_ns, "progress": progress, "index": self._index, "events": len(self._events), "source_session": self.source_session, "read_only": True}

    def snapshot(self) -> dict[str, Any]:
        return {"status": {**self.status, "market_time_ns": self.market_time_ns, "replay": self.replay_state()}, "quotes": self.latest_quotes, "trades": self.latest_trades, "metrics": self.latest_metrics, "writer": {"written": 0, "dropped": 0}, "replay": self.replay_state()}

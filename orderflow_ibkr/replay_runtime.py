from __future__ import annotations

import asyncio
from bisect import bisect_left
import json
import sqlite3
import threading
import time
import uuid
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

    def __init__(
        self,
        *,
        symbols: list[str],
        db_path: str | Path,
        source_session: str | None = None,
        start_ns: int | None = None,
        duration_sec: int = 1800,
        speed: float = 1.0,
    ) -> None:
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
        self.history_quotes: dict[str, list[dict[str, Any]]] = {}
        self.history_trades: dict[str, list[dict[str, Any]]] = {}
        self.history_metrics: dict[str, list[dict[str, Any]]] = {}
        self.status: dict[str, Any] = {"state": "starting", "runtime_mode": "REPLAY"}
        self._events: list[tuple[int, int, str, str, dict[str, Any]]] = []
        self._event_times: list[int] = []
        self._index = 0
        self._play_task: asyncio.Task | None = None
        self._seek_lock = asyncio.Lock()
        self._engine_lock = threading.RLock()
        self._playing = False
        self._last_metric_ns: dict[str, int] = {}
        self._last_ranking_ns = 0
        self.start_ns = 0
        self.end_ns = 0
        self.market_time_ns = 0
        self.source_provider = "RECORDED"
        self.plan = SimpleNamespace(
            active_mode="REPLAY",
            quality="RECORDED_MARKET_DATA",
            market_data_lines=0,
            tbt_capacity=0,
            trade_source="RECORDED_TRADE",
            quote_source="RECORDED_QUOTE",
        )

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

    @staticmethod
    def _provider_from_source(source: str) -> str:
        upper = source.upper()
        if upper.startswith("ALPACA"):
            return "ALPACA"
        if upper.startswith("TBT_") or upper.startswith("REQ_MKT_DATA"):
            return "IBKR"
        return "RECORDED"

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
            qrows = conn.execute(
                f"SELECT * FROM raw_quotes WHERE session_id=? AND ts_ns BETWEEN ? AND ? AND symbol IN ({placeholders}) ORDER BY ts_ns, id",
                args,
            ).fetchall()
            trows = conn.execute(
                f"SELECT * FROM raw_trades WHERE session_id=? AND ts_ns BETWEEN ? AND ? AND symbol IN ({placeholders}) ORDER BY ts_ns, id",
                args,
            ).fetchall()

        if not qrows or not trows:
            raise RuntimeError("Replay interval requires both quote and trade events")

        first_quote = dict(qrows[0])
        first_trade = dict(trows[0])
        source_quality = str(
            first_trade.get("quality") or first_quote.get("quality") or "RECORDED_MARKET_DATA"
        )
        raw_trade_source = str(first_trade.get("source") or "RECORDED_TRADE")
        raw_quote_source = str(first_quote.get("source") or "RECORDED_QUOTE")
        self.source_provider = self._provider_from_source(raw_trade_source)
        self.plan = SimpleNamespace(
            active_mode="REPLAY",
            quality=source_quality,
            market_data_lines=0,
            tbt_capacity=0,
            trade_source=f"REPLAY:{raw_trade_source}",
            quote_source=f"REPLAY:{raw_quote_source}",
        )

        events: list[tuple[int, int, str, str, dict[str, Any]]] = []
        for row in qrows:
            data = dict(row)
            events.append((int(data["ts_ns"]), 0, str(data["symbol"]), "quote", data))
        for row in trows:
            data = dict(row)
            events.append((int(data["ts_ns"]), 1, str(data["symbol"]), "trade", data))
        events.sort(key=lambda event: (event[0], event[1]))
        if not events:
            raise RuntimeError("Replay interval contains no events")
        self._events = events
        self._event_times = [event[0] for event in events]
        self.start_ns = events[0][0]
        self.end_ns = events[-1][0]
        self.market_time_ns = self.start_ns

    def _reset_state(self) -> None:
        self.engine = OrderFlowEngine()
        self.latest_metrics.clear()
        self.latest_quotes.clear()
        self.latest_trades.clear()
        self.history_quotes.clear()
        self.history_trades.clear()
        self.history_metrics.clear()
        self._last_metric_ns.clear()
        self._last_ranking_ns = 0
        self._index = 0
        self.market_time_ns = self.start_ns

    @staticmethod
    def _append_history(
        target: dict[str, list[dict[str, Any]]],
        symbol: str,
        row: dict[str, Any],
        limit: int,
    ) -> None:
        rows = target.setdefault(symbol, [])
        rows.append(row)
        if len(rows) > limit:
            del rows[:-limit]

    def _emit_quote(self, symbol: str, row: dict[str, Any], *, publish: bool = True) -> dict[str, Any]:
        raw_source = str(row.get("source") or "RECORDED_QUOTE")
        quote = QuoteEvent(
            ts_ns=int(row["ts_ns"]),
            bid=row.get("bid"),
            ask=row.get("ask"),
            bid_size=row.get("bid_size"),
            ask_size=row.get("ask_size"),
            last=row.get("last"),
            last_size=row.get("last_size"),
            volume=row.get("volume"),
            vwap=row.get("vwap"),
            trade_count=row.get("trade_count"),
            trade_rate=row.get("trade_rate"),
            volume_rate=row.get("volume_rate"),
            source=f"REPLAY:{raw_source}",
            quality=str(row.get("quality") or self.plan.quality),
        )
        self.engine.state(symbol).add_quote(quote)
        out = {
            "ts_ns": quote.ts_ns,
            "bid": quote.bid,
            "ask": quote.ask,
            "bid_size": quote.bid_size,
            "ask_size": quote.ask_size,
            "last": quote.last,
            "last_size": quote.last_size,
            "volume": quote.volume,
            "vwap": quote.vwap,
            "trade_count": quote.trade_count,
            "trade_rate": quote.trade_rate,
            "volume_rate": quote.volume_rate,
            "source": quote.source,
            "quality": quote.quality,
            "session_id": self.session_id,
            "symbol": symbol,
            "source_session": self.source_session,
        }
        self.latest_quotes[symbol] = out
        self._append_history(self.history_quotes, symbol, out, 7000)
        if publish:
            self._publish({"type": "quote", "symbol": symbol, "data": out})
        self._maybe_compute(symbol, quote.ts_ns, publish=publish)
        return out

    def _emit_trade(self, symbol: str, row: dict[str, Any], *, publish: bool = True) -> dict[str, Any]:
        try:
            conditions = json.loads(row.get("conditions_json") or "[]")
        except Exception:
            conditions = []
        raw_source = str(row.get("source") or "RECORDED_TRADE")
        trade = TradeEvent(
            ts_ns=int(row["ts_ns"]),
            price=float(row["price"]),
            size=float(row["size"]),
            exchange=row.get("exchange"),
            conditions=conditions,
            aggressor=str(row.get("aggressor") or "UNKNOWN"),
            aggressor_confidence=float(row.get("aggressor_confidence") or 0.0),
            source=f"REPLAY:{raw_source}",
            quality=str(row.get("quality") or self.plan.quality),
        )
        self.engine.state(symbol).add_trade(trade)
        out = {
            "ts_ns": trade.ts_ns,
            "price": trade.price,
            "size": trade.size,
            "exchange": trade.exchange,
            "conditions": trade.conditions,
            "aggressor": trade.aggressor,
            "aggressor_confidence": trade.aggressor_confidence,
            "source": trade.source,
            "quality": trade.quality,
            "session_id": self.session_id,
            "symbol": symbol,
            "source_session": self.source_session,
        }
        self.latest_trades[symbol] = out
        self._append_history(self.history_trades, symbol, out, 7000)
        if publish:
            self._publish({"type": "trade", "symbol": symbol, "data": out})
        self._maybe_compute(symbol, trade.ts_ns, publish=publish)
        return out

    def _maybe_compute(self, symbol: str, now_ns: int, *, publish: bool = True) -> None:
        # Keep metrics at a useful UI cadence. At high replay speeds, computing
        # every simulated second wastes CPU without producing a visible update.
        metric_interval_ns = max(1_000_000_000, int(self.speed * 100_000_000))
        if now_ns - self._last_metric_ns.get(symbol, 0) < metric_interval_ns:
            return
        self._last_metric_ns[symbol] = now_ns
        metrics = self.engine.state(symbol).metrics(
            now_ns=now_ns,
            window_sec=15,
            quality=self.plan.quality,
        )
        self.latest_metrics[symbol] = metrics
        self._append_history(self.history_metrics, symbol, metrics, 3500)
        if publish:
            footprint = self.engine.state(symbol).footprint(
                now_ns=now_ns,
                sec=300,
                tick_size=0.01,
            )
            self._publish(
                {
                    "type": "metrics",
                    "symbol": symbol,
                    "data": metrics,
                    "footprint": footprint[:120],
                }
            )
        if now_ns - self._last_ranking_ns >= 2_000_000_000:
            self._last_ranking_ns = now_ns
            items = sorted(
                self.latest_metrics.items(),
                key=lambda item: item[1].get("score", 0.0),
                reverse=True,
            )
            if publish:
                self._publish(
                    {
                        "type": "ranking",
                        "data": [
                            {
                                "session_id": self.session_id,
                                "symbol": current_symbol,
                                "ts_ns": now_ns,
                                "rank": rank,
                                "score": current.get("score", 0.0),
                                "activity_score": current.get("activity_score", 0.0),
                                "flow_score": current.get("flow_score", 50.0),
                                "absorption_score": current.get("absorption_score", 0.0),
                                "quality": current.get("quality", self.plan.quality),
                            }
                            for rank, (current_symbol, current) in enumerate(items, 1)
                        ],
                    }
                )

    def _step_one(self, *, publish: bool = True) -> tuple[str, str, dict[str, Any]] | None:
        with self._engine_lock:
            if self._index >= len(self._events):
                return None
            ts_ns, _, symbol, kind, row = self._events[self._index]
            self._index += 1
            self.market_time_ns = ts_ns
            if kind == "quote":
                return kind, symbol, self._emit_quote(symbol, row, publish=publish)
            return kind, symbol, self._emit_trade(symbol, row, publish=publish)

    def _rebuild_to_target(self, target_ns: int) -> None:
        with self._engine_lock:
            self._reset_state()
            # A seek only needs the recent window used by metrics/footprint.
            # Replaying the entire multi-million-event session made a slider
            # move effectively unusable.
            rebuild_from = max(self.start_ns, target_ns - 300 * 1_000_000_000)
            self._index = bisect_left(self._event_times, rebuild_from)
            while self._index < len(self._events) and self._events[self._index][0] <= target_ns:
                self._step_one(publish=False)
            self.market_time_ns = target_ns

    async def _play_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            wall_start = loop.time()
            market_start = self.market_time_ns
            next_flush = wall_start
            quotes: dict[str, dict[str, Any]] = {}
            trades: dict[str, list[dict[str, Any]]] = {}
            while self._playing and self._index < len(self._events):
                target_ns = market_start + int((loop.time() - wall_start) * 1_000_000_000 * self.speed)
                processed = 0
                while self._playing and self._index < len(self._events) and self._events[self._index][0] <= target_ns:
                    event = self._step_one(publish=False)
                    if event is None:
                        break
                    kind, symbol, data = event
                    if kind == "quote":
                        quotes[symbol] = data
                    else:
                        trades.setdefault(symbol, []).append(data)
                    processed += 1
                    # Let HTTP controls and the websocket writer run even at high speed.
                    if processed >= 2_000:
                        break

                now = loop.time()
                if now >= next_flush:
                    self._publish(
                        {
                            "type": "replay_batch",
                            "data": {
                                "quotes": quotes,
                                "trades": trades,
                                "metrics": {symbol: dict(data) for symbol, data in self.latest_metrics.items()},
                                "replay": self.replay_state(),
                            },
                        }
                    )
                    quotes = {}
                    trades = {}
                    next_flush = now + 0.10

                if self._index >= len(self._events) or not self._playing:
                    break
                next_ts = self._events[self._index][0]
                delay = (next_ts - target_ns) / 1_000_000_000 / self.speed
                if delay > 0:
                    await asyncio.sleep(min(0.05, max(0.001, delay)))
                else:
                    await asyncio.sleep(0)

            if quotes or trades:
                self._publish(
                    {
                        "type": "replay_batch",
                        "data": {
                            "quotes": quotes,
                            "trades": trades,
                            "metrics": {symbol: dict(data) for symbol, data in self.latest_metrics.items()},
                            "replay": self.replay_state(),
                        },
                    }
                )
            if self._index >= len(self._events):
                self._playing = False
                self._publish({"type": "replay", "data": self.replay_state()})
        except asyncio.CancelledError:
            raise

    async def play(self) -> None:
        async with self._seek_lock:
            self._start_playing()

    def _start_playing(self) -> None:
        self._playing = True
        if not self._play_task or self._play_task.done():
            self._play_task = asyncio.create_task(self._play_loop())
        self._publish({"type": "replay", "data": self.replay_state()})

    async def pause(self) -> None:
        async with self._seek_lock:
            self._playing = False
            self._publish({"type": "replay", "data": self.replay_state()})

    async def set_speed(self, speed: float) -> None:
        self.speed = max(0.01, min(float(speed), 1000.0))
        self._publish({"type": "replay", "data": self.replay_state()})

    async def seek(self, target_ns: int) -> None:
        async with self._seek_lock:
            was_playing = self._playing
            self._playing = False
            target_ns = max(self.start_ns, min(int(target_ns), self.end_ns))
            await asyncio.to_thread(self._rebuild_to_target, target_ns)
            self._publish({"type": "snapshot", "data": self.snapshot(include_history=True)})
            self._publish({"type": "replay", "data": self.replay_state()})
            if was_playing:
                self._start_playing()

    async def next_bar(self, seconds: int = 30) -> None:
        await self.seek(self.market_time_ns + seconds * 1_000_000_000)

    async def prev_bar(self, seconds: int = 30) -> None:
        await self.seek(self.market_time_ns - seconds * 1_000_000_000)

    async def start(self) -> None:
        await asyncio.to_thread(self._load_events)
        self.status = {
            "state": "running",
            "runtime_mode": "REPLAY",
            "session_id": self.session_id,
            "source_session": self.source_session,
            "source_provider": self.source_provider,
            "mode": "REPLAY",
            "quality": self.plan.quality,
            "symbols": self.symbols,
            "db_path": str(self.db_path.resolve()),
            "read_only": True,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "market_time_ns": self.market_time_ns,
        }
        self._publish({"type": "status", "data": self.status})
        self._publish({"type": "replay", "data": self.replay_state()})

    async def stop(self) -> None:
        self._playing = False
        if self._play_task:
            self._play_task.cancel()
            try:
                await self._play_task
            except asyncio.CancelledError:
                pass

    def replay_state(self) -> dict[str, Any]:
        with self._engine_lock:
            duration = max(1, self.end_ns - self.start_ns)
            progress = max(
                0.0,
                min(1.0, (self.market_time_ns - self.start_ns) / duration),
            )
            return {
                "runtime_mode": "REPLAY",
                "playing": self._playing,
                "speed": self.speed,
                "market_time_ns": self.market_time_ns,
                "start_ns": self.start_ns,
                "end_ns": self.end_ns,
                "progress": progress,
                "index": self._index,
                "events": len(self._events),
                "source_session": self.source_session,
                "source_provider": self.source_provider,
                "quality": self.plan.quality,
                "read_only": True,
            }

    def snapshot(self, *, include_history: bool = False) -> dict[str, Any]:
        with self._engine_lock:
            replay = self.replay_state()
            payload = {
                "status": {
                    **self.status,
                    "market_time_ns": self.market_time_ns,
                    "replay": replay,
                },
                "quotes": {symbol: dict(row) for symbol, row in self.latest_quotes.items()},
                "trades": {symbol: dict(row) for symbol, row in self.latest_trades.items()},
                "metrics": {symbol: dict(row) for symbol, row in self.latest_metrics.items()},
                "writer": {"written": 0, "dropped": 0},
                "replay": replay,
            }
            if include_history:
                payload["replay_history"] = {
                    "quotes": {symbol: [dict(row) for row in rows] for symbol, rows in self.history_quotes.items()},
                    "trades": {symbol: [dict(row) for row in rows] for symbol, rows in self.history_trades.items()},
                    "metrics": {symbol: [dict(row) for row in rows] for symbol, rows in self.history_metrics.items()},
                }
            return payload

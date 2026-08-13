from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .engine import OrderFlowEngine, QuoteEvent, TradeEvent
from .ibkr_adapter import IBKRAdapter
from .storage import SQLiteStore
from .writer import SQLiteEventWriter


class OrderFlowRuntime:
    def __init__(
        self,
        *,
        symbols: list[str],
        mode: str,
        db_path: str | Path,
        ib_host: str,
        ib_port: int,
        client_id: int,
        market_data_lines: int,
    ):
        self.symbols = [s.upper() for s in symbols]
        self.mode = mode
        self.db_path = Path(db_path)
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.client_id = client_id
        self.market_data_lines = market_data_lines
        self.session_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"

        self.store = SQLiteStore(self.db_path)
        self.writer = SQLiteEventWriter(self.db_path)
        self.engine = OrderFlowEngine()
        self.event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
        self.latest_metrics: dict[str, dict[str, Any]] = {}
        self.latest_quotes: dict[str, dict[str, Any]] = {}
        self.latest_trades: dict[str, dict[str, Any]] = {}
        self.status: dict[str, Any] = {"state": "starting"}
        self.plan = None
        self._last_metric_ns: dict[str, int] = {}
        self._last_signal_ns: dict[tuple[str, str], int] = {}
        self._last_ranking_ns = 0

        self.adapter = IBKRAdapter(
            symbols=self.symbols,
            mode=mode,
            host=ib_host,
            port=ib_port,
            client_id=client_id,
            market_data_lines=market_data_lines,
            on_quote=self._on_quote,
            on_trade=self._on_trade,
            on_status=self._on_status,
        )

    def _publish(self, payload: dict[str, Any]) -> None:
        payload.setdefault("session_id", self.session_id)
        try:
            self.event_queue.put_nowait(payload)
        except asyncio.QueueFull:
            # UI messages are lossy by design; the SQL event store remains primary.
            pass

    def _on_status(self, status: dict[str, Any]) -> None:
        self.status = {**status, "session_id": self.session_id}
        self._publish({"type": "status", "data": self.status})

    def _quote_row(self, symbol: str, quote: QuoteEvent) -> dict[str, Any]:
        d = asdict(quote)
        d.update({"session_id": self.session_id, "symbol": symbol})
        return d

    def _trade_row(self, symbol: str, trade: TradeEvent) -> dict[str, Any]:
        d = asdict(trade)
        d["conditions_json"] = json.dumps(d.pop("conditions") or [], separators=(",", ":"))
        d.update({"session_id": self.session_id, "symbol": symbol})
        return d

    def _on_quote(self, symbol: str, quote: QuoteEvent) -> None:
        state = self.engine.state(symbol)
        proxy_trade = state.add_quote(quote)
        row = self._quote_row(symbol, quote)
        self.writer.submit("quote", row)
        self.latest_quotes[symbol] = row
        self._publish({"type": "quote", "symbol": symbol, "data": row})

        if proxy_trade is not None:
            trade_row = self._trade_row(symbol, proxy_trade)
            self.writer.submit("trade", trade_row)
            self.latest_trades[symbol] = trade_row
            self._publish({"type": "trade", "symbol": symbol, "data": trade_row})
        self._maybe_compute(symbol, quote.ts_ns)

    def _on_trade(self, symbol: str, trade: TradeEvent) -> None:
        state = self.engine.state(symbol)
        state.add_trade(trade)
        row = self._trade_row(symbol, trade)
        self.writer.submit("trade", row)
        self.latest_trades[symbol] = row
        self._publish({"type": "trade", "symbol": symbol, "data": row})
        self._maybe_compute(symbol, trade.ts_ns)

    def _maybe_compute(self, symbol: str, now_ns: int) -> None:
        # Persist derived state at 1 Hz per symbol; raw data remains event-level.
        if now_ns - self._last_metric_ns.get(symbol, 0) < 1_000_000_000:
            return
        self._last_metric_ns[symbol] = now_ns
        quality = self.adapter.plan.quality
        m = self.engine.state(symbol).metrics(now_ns=now_ns, window_sec=15, quality=quality)
        metric_row = {**m, "session_id": self.session_id, "symbol": symbol}
        metric_row["details_json"] = json.dumps(
            {
                "score": m["score"],
                "flow_score": m["flow_score"],
                "absorption_score": m["absorption_score"],
                "move_bps": m["move_bps"],
            },
            separators=(",", ":"),
        )
        self.writer.submit("metric", metric_row)
        self.latest_metrics[symbol] = m

        # Compact O(1)-per-symbol current-state table for realtime read-only agents.
        latest = {
            **m,
            "symbol": symbol,
            "mode": self.adapter.plan.active_mode,
            "quote": {
                k: self.latest_quotes.get(symbol, {}).get(k)
                for k in ("bid", "ask", "bid_size", "ask_size", "last", "last_size", "volume", "vwap", "trade_rate", "volume_rate")
            },
            "last_trade": {
                k: self.latest_trades.get(symbol, {}).get(k)
                for k in ("ts_ns", "price", "size", "exchange", "aggressor", "aggressor_confidence", "source")
            },
        }
        self.writer.submit(
            "latest",
            {
                "symbol": symbol,
                "session_id": self.session_id,
                "ts_ns": now_ns,
                "mode": self.adapter.plan.active_mode,
                "quality": quality,
                "state_json": json.dumps(latest, separators=(",", ":"), default=str),
            },
        )

        footprint = self.engine.state(symbol).footprint(now_ns=now_ns, sec=300, tick_size=0.01)
        self._publish(
            {
                "type": "metrics",
                "symbol": symbol,
                "data": m,
                "footprint": footprint[:120],
            }
        )

        for sig in self.engine.signals_from_metrics(symbol, m):
            key = (symbol, sig["signal_type"])
            # Avoid storing the same persistent condition every second.
            if now_ns - self._last_signal_ns.get(key, 0) < 15_000_000_000:
                continue
            self._last_signal_ns[key] = now_ns
            sig_row = {**sig, "session_id": self.session_id}
            self.writer.submit("signal", sig_row)
            self._publish({"type": "signal", "symbol": symbol, "data": sig})

        if now_ns - self._last_ranking_ns >= 2_000_000_000:
            self._last_ranking_ns = now_ns
            self._rank(now_ns)

    def _rank(self, now_ns: int) -> None:
        items = sorted(
            self.latest_metrics.items(),
            key=lambda kv: kv[1].get("score", 0.0),
            reverse=True,
        )
        rows = []
        payload = []
        for i, (symbol, m) in enumerate(items, 1):
            row = {
                "session_id": self.session_id,
                "symbol": symbol,
                "ts_ns": now_ns,
                "rank": i,
                "score": m.get("score", 0.0),
                "activity_score": m.get("activity_score", 0.0),
                "flow_score": m.get("flow_score", 50.0),
                "absorption_score": m.get("absorption_score", 0.0),
                "quality": m.get("quality", "UNKNOWN"),
            }
            rows.append(row)
            payload.append(row)
        for row in rows:
            self.writer.submit("ranking", row)
        self._publish({"type": "ranking", "data": payload})

    async def start(self) -> None:
        self.writer.start()
        self.plan = await self.adapter.connect()
        self.store.begin_session(
            session_id=self.session_id,
            mode=self.plan.active_mode,
            symbols=self.symbols,
            ib_host=self.ib_host,
            ib_port=self.ib_port,
            client_id=self.client_id,
            market_data_lines=self.market_data_lines,
        )
        for symbol in self.symbols:
            self.store.upsert_subscription(
                session_id=self.session_id,
                symbol=symbol,
                mode=self.plan.active_mode,
                trade_source=self.plan.trade_source,
                quote_source=self.plan.quote_source,
                quality=self.plan.quality,
            )
        self.status = {
            "state": "running",
            "session_id": self.session_id,
            "mode": self.plan.active_mode,
            "quality": self.plan.quality,
            "symbols": self.symbols,
            "market_data_lines": self.plan.market_data_lines,
            "tbt_capacity": self.plan.tbt_capacity,
            "db_path": str(self.db_path.resolve()),
        }
        self._publish({"type": "status", "data": self.status})

    async def stop(self) -> None:
        await self.adapter.disconnect()
        self.store.end_session(self.session_id)
        self.writer.stop()
        self.store.close()

    def snapshot(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "quotes": self.latest_quotes,
            "trades": self.latest_trades,
            "metrics": self.latest_metrics,
            "writer": {"written": self.writer.written, "dropped": self.writer.dropped},
        }

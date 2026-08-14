from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import WSMsgType, web

from .alpaca_replay import (
    REPLAY_SYMBOL_CAP,
    build_previous_session_replay,
    clean_replay_symbols,
    latest_completed_trading_day,
    validate_replay_request,
)
from .alpaca_runtime import AlpacaOrderFlowRuntime
from .history import load_history
from .replay_runtime import ReplayRuntime
from .runtime import OrderFlowRuntime
from .storage import open_readonly

ASSETS = {
    "flow.css",
    "flow-dom.js",
    "flow-state.js",
    "flow-chart.js",
    "flow-render.js",
    "flow-init.js",
    "footprint-timeseries.js",
    "workstation-shell.js",
    "workstation-shell.css",
}


class WorkstationServer:
    def __init__(self, runtime: Any, root: Path, args: argparse.Namespace):
        self.runtime = runtime
        self.root = root
        self.args = args
        self.clients: set[web.WebSocketResponse] = set()
        self.broadcaster: asyncio.Task | None = None
        self.replay_prepare_task: asyncio.Task | None = None
        self.replay_speed = 1.0
        self.replay_transition = False
        self._runtime_stopped = False
        self.market_reference_cache: dict[tuple[str, date], dict[str, Any]] = {}
        self.replay_prepare_state: dict[str, Any] = {
            "state": "idle",
            "symbol_cap": REPLAY_SYMBOL_CAP,
        }

    async def page(self, request: web.Request) -> web.Response:
        html = (self.root / "flow_v2.html").read_text(encoding="utf-8")
        if "/static/workstation-shell.js" not in html:
            html = html.replace(
                "</head>",
                '<link rel="stylesheet" href="/static/workstation-shell.css"></head>',
            )
            html = html.replace(
                "</body>",
                '<script src="/static/workstation-shell.js"></script></body>',
            )
        return web.Response(
            text=html,
            content_type="text/html",
            headers={"Cache-Control": "no-store"},
        )

    async def radar(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.root / "index.html", headers={"Cache-Control": "no-store"})

    async def learn(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(
            self.root / "teaching" / "orderflow_timeseries_tutorial.html",
            headers={"Cache-Control": "no-store"},
        )

    async def asset(self, request: web.Request) -> web.FileResponse:
        name = request.match_info["name"]
        if name not in ASSETS:
            raise web.HTTPNotFound()
        return web.FileResponse(
            self.root / "static" / name,
            headers={"Cache-Control": "no-store"},
        )

    async def status(self, request: web.Request) -> web.Response:
        if self.replay_transition:
            status = dict(self.runtime.status)
            status.update(
                {
                    "state": "preparing",
                    "runtime_mode": "REPLAY",
                    "mode": "REPLAY",
                    "read_only": True,
                    "replay_prepare": dict(self.replay_prepare_state),
                }
            )
            return web.json_response(status)
        return web.json_response(self.runtime.status)

    def _snapshot_payload(self, *, include_history: bool = False) -> dict[str, Any]:
        if isinstance(self.runtime, ReplayRuntime):
            return self.runtime.snapshot(include_history=include_history)
        payload = self.runtime.snapshot()
        if not self.replay_transition:
            return payload
        replay = {
            "runtime_mode": "REPLAY",
            "preparing": True,
            "playing": False,
            "speed": self.replay_speed,
            "market_time_ns": 0,
            "start_ns": 0,
            "end_ns": 1,
            "progress": 0,
            "read_only": True,
            **self.replay_prepare_state,
        }
        status = {
            **payload.get("status", {}),
            "state": "preparing",
            "runtime_mode": "REPLAY",
            "mode": "REPLAY",
            "read_only": True,
            "replay": replay,
            "replay_prepare": dict(self.replay_prepare_state),
        }
        return {
            **payload,
            "status": status,
            "replay": replay,
            "writer": {"written": 0, "dropped": 0},
        }

    async def snapshot(self, request: web.Request) -> web.Response:
        include_history = request.query.get("include_history") == "1"
        return web.json_response(self._snapshot_payload(include_history=include_history))

    def _reference_day(self) -> date:
        eastern = ZoneInfo("America/New_York")
        if isinstance(self.runtime, ReplayRuntime):
            return datetime.fromtimestamp(self.runtime.start_ns / 1_000_000_000, tz=eastern).date()
        return datetime.now(eastern).date()

    async def _local_previous_close(self, symbol: str, target: date) -> dict[str, Any] | None:
        """Use the local recording as a read-only fallback for premarket references."""
        path = getattr(self.runtime, "db_path", None)
        if not path:
            return None
        eastern = ZoneInfo("America/New_York")
        target_start_ns = int(datetime.combine(target, dtime.min, tzinfo=eastern).timestamp() * 1_000_000_000)

        def query() -> dict[str, Any] | None:
            try:
                conn = open_readonly(path)
                try:
                    row = conn.execute(
                        """
                        SELECT price, ts_ns
                        FROM raw_trades
                        WHERE symbol=? AND ts_ns BETWEEN 0 AND ?
                        ORDER BY ts_ns DESC
                        LIMIT 1
                        """,
                        (symbol, target_start_ns),
                    ).fetchone()
                finally:
                    conn.close()
            except Exception:
                return None
            if row is None or row["price"] is None:
                return None
            return {
                "available": True,
                "symbol": symbol,
                "trading_day": target.isoformat(),
                "session_open": None,
                "previous_close": float(row["price"]),
                "source": "local_sqlite_previous_trade",
                "reference_ts_ns": int(row["ts_ns"]),
            }

        return await asyncio.to_thread(query)

    async def market_reference(self, request: web.Request) -> web.Response:
        symbol = request.query.get("symbol", "").strip().upper()
        if symbol not in self.runtime.symbols:
            raise web.HTTPBadRequest(text="symbol is not in the active universe")
        target = self._reference_day()
        cached = self.market_reference_cache.get((symbol, target))
        if cached is not None:
            return web.json_response(cached, headers={"Cache-Control": "no-store"})
        key = self.args.alpaca_key or os.getenv("APCA_API_KEY_ID")
        secret = self.args.alpaca_secret or os.getenv("APCA_API_SECRET_KEY")
        if not key or not secret:
            local = await self._local_previous_close(symbol, target)
            return web.json_response(local or {"available": False, "symbol": symbol, "trading_day": target.isoformat()})
        eastern = ZoneInfo("America/New_York")
        start = datetime.combine(target - timedelta(days=10), dtime.min, tzinfo=eastern).astimezone(timezone.utc)
        end = datetime.combine(target + timedelta(days=1), dtime.min, tzinfo=eastern).astimezone(timezone.utc)
        params = {
            "timeframe": "1Day",
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "adjustment": "raw",
            "feed": "sip",
            "limit": "20",
        }
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as client:
                async with client.get(
                    f"https://data.alpaca.markets/v2/stocks/{symbol}/bars",
                    params=params,
                    headers=headers,
                ) as response:
                    if response.status != 200:
                        local = await self._local_previous_close(symbol, target)
                        return web.json_response(local or {"available": False, "symbol": symbol, "trading_day": target.isoformat()})
                    rows = (await response.json()).get("bars", [])
        except Exception:
            local = await self._local_previous_close(symbol, target)
            return web.json_response(local or {"available": False, "symbol": symbol, "trading_day": target.isoformat()})
        parsed: list[tuple[date, dict[str, Any]]] = []
        for row in rows:
            try:
                stamp = str(row["t"]).replace("Z", "+00:00")
                day = datetime.fromisoformat(stamp).astimezone(eastern).date()
                parsed.append((day, row))
            except (KeyError, TypeError, ValueError):
                continue
        current = next((row for day, row in reversed(parsed) if day == target), None)
        previous = next((row for day, row in reversed(parsed) if day < target), None)
        if current is None or previous is None:
            local = await self._local_previous_close(symbol, target)
            return web.json_response(local or {"available": False, "symbol": symbol, "trading_day": target.isoformat()})
        payload = {
            "available": True,
            "symbol": symbol,
            "trading_day": target.isoformat(),
            "session_open": float(current["o"]),
            "previous_close": float(previous["c"]),
            "source": "alpaca_sip_daily",
        }
        self.market_reference_cache[(symbol, target)] = payload
        return web.json_response(payload, headers={"Cache-Control": "no-store"})

    @staticmethod
    def qint(request: web.Request, name: str, default: int) -> int:
        try:
            return int(request.query.get(name, default))
        except (TypeError, ValueError):
            return default

    async def history(self, request: web.Request) -> web.Response:
        symbol = request.query.get("symbol", "").strip().upper()
        if not symbol:
            raise web.HTTPBadRequest(text="symbol is required")
        if symbol not in self.runtime.symbols:
            raise web.HTTPBadRequest(text=f"symbol {symbol} is not in the active universe")
        if isinstance(self.runtime, ReplayRuntime) or self.replay_transition:
            return web.json_response(
                {
                    "session_id": self.runtime.session_id,
                    "symbol": symbol,
                    "runtime_mode": "REPLAY",
                    "quotes": [],
                    "trades": [],
                    "metrics": [],
                    "signals": [],
                    "note": "Replay history is intentionally not preloaded from future source rows.",
                },
                headers={"Cache-Control": "no-store"},
            )
        data = await asyncio.to_thread(
            load_history,
            self.runtime.db_path,
            session_id=self.runtime.session_id,
            symbol=symbol,
            seconds=self.qint(request, "seconds", 900),
            quote_limit=self.qint(request, "quotes", 5000),
            trade_limit=self.qint(request, "trades", 5000),
            metric_limit=self.qint(request, "metrics", 2500),
            signal_limit=self.qint(request, "signals", 200),
        )
        return web.json_response(data, headers={"Cache-Control": "no-store"})

    async def replay_state(self, request: web.Request) -> web.Response:
        if not isinstance(self.runtime, ReplayRuntime):
            if self.replay_transition:
                return web.json_response(
                    {
                        "available": True,
                        "runtime_mode": "REPLAY",
                        "preparing": True,
                        **self.replay_prepare_state,
                    }
                )
            return web.json_response({"runtime_mode": "LIVE", "available": False})
        return web.json_response({"available": True, **self.runtime.replay_state()})

    async def replay_control(self, request: web.Request) -> web.Response:
        if not isinstance(self.runtime, ReplayRuntime):
            raise web.HTTPConflict(text="Replay is not active yet. Use SIM REPLAY to prepare a session.")
        try:
            body = await request.json()
        except Exception:
            body = {}
        action = str(body.get("action", "")).lower()
        if action == "play":
            await self.runtime.play()
        elif action == "pause":
            await self.runtime.pause()
        elif action == "speed":
            await self.runtime.set_speed(float(body.get("speed", 1.0)))
        elif action == "seek":
            if "target_ns" in body:
                target_ns = int(body["target_ns"])
            else:
                progress = max(0.0, min(1.0, float(body.get("progress", 0.0))))
                target_ns = self.runtime.start_ns + int(
                    (self.runtime.end_ns - self.runtime.start_ns) * progress
                )
            await self.runtime.seek(target_ns)
        elif action == "next_bar":
            await self.runtime.next_bar(int(body.get("seconds", 30)))
        elif action == "prev_bar":
            await self.runtime.prev_bar(int(body.get("seconds", 30)))
        elif action == "restart":
            await self.runtime.seek(self.runtime.start_ns)
        else:
            raise web.HTTPBadRequest(text="unknown replay action")
        return web.json_response(self.runtime.replay_state())

    async def live_switch(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            body = {}
        provider = str(body.get("provider") or "").lower()
        raw_symbols = body.get("symbols") or []
        if isinstance(raw_symbols, str):
            raw_symbols = raw_symbols.replace(",", " ").split()
        symbols = list(dict.fromkeys(str(value).strip().upper() for value in raw_symbols if str(value).strip()))
        if provider not in {"ibkr", "alpaca"}:
            raise web.HTTPBadRequest(text="LIVE source must be IBKR TWS or Alpaca WebSocket")
        if not symbols or len(symbols) > 5:
            raise web.HTTPBadRequest(text="LIVE requires 1 to 5 symbols")
        if any(not re.fullmatch(r"[A-Z][A-Z.\-]{0,9}", symbol) for symbol in symbols):
            raise web.HTTPBadRequest(text="One or more symbols use an invalid US equity format")

        api_key = str(body.get("api_key") or self.args.alpaca_key or os.getenv("APCA_API_KEY_ID") or "") or None
        api_secret = str(body.get("api_secret") or self.args.alpaca_secret or os.getenv("APCA_API_SECRET_KEY") or "") or None

        # Alpaca's historical endpoint is only relevant when switching to
        # Alpaca.  IBKR/TWS must be able to switch with Alpaca credentials
        # unset; its readiness is determined by the TWS socket connection.
        if provider == "alpaca":
            try:
                validation_day = await latest_completed_trading_day(
                    api_key=api_key,
                    api_secret=api_secret,
                )
                for symbol in symbols:
                    await validate_replay_request(
                        symbol=symbol,
                        trading_day=validation_day,
                        api_key=api_key,
                        api_secret=api_secret,
                    )
            except (ValueError, RuntimeError) as exc:
                raise web.HTTPBadRequest(text=str(exc)) from exc

        if self.replay_prepare_task and not self.replay_prepare_task.done():
            self.replay_prepare_task.cancel()
            try:
                await self.replay_prepare_task
            except asyncio.CancelledError:
                pass
        old_runtime = self.runtime
        if not self._runtime_stopped:
            await old_runtime.stop()
            self._runtime_stopped = True
        if provider == "alpaca":
            runtime = AlpacaOrderFlowRuntime(
                symbols=symbols,
                db_path=self.args.db,
                feed=self.args.alpaca_feed,
                api_key=api_key,
                api_secret=api_secret,
            )
        else:
            runtime = OrderFlowRuntime(
                symbols=symbols,
                mode=self.args.mode,
                db_path=self.args.db,
                ib_host=self.args.ib_host,
                ib_port=self.args.ib_port,
                client_id=self.args.client_id,
                market_data_lines=self.args.market_data_lines,
            )
        try:
            await runtime.start()
        except Exception as exc:
            raise web.HTTPBadRequest(text=f"Could not start LIVE {provider.upper()}: {exc}") from exc
        if provider == "alpaca" and api_key and api_secret and not self._credentials_configured():
            await self._save_credentials(api_key, api_secret)
        self.runtime = runtime
        self.replay_transition = False
        self._runtime_stopped = False
        self.replay_prepare_state = {"state": "idle", "symbol_cap": REPLAY_SYMBOL_CAP}
        await self._restart_broadcaster()
        await self._push_snapshot_to_clients()
        return web.json_response(runtime.snapshot())

    def _credentials_configured(self) -> bool:
        key = self.args.alpaca_key or os.getenv("APCA_API_KEY_ID")
        secret = self.args.alpaca_secret or os.getenv("APCA_API_SECRET_KEY")
        return bool(key and secret)

    @staticmethod
    def _credential_env_path() -> Path:
        """Find the same local .env location used by package startup."""
        package_path = Path(__file__).resolve()
        candidates = [Path.cwd() / ".env"]
        candidates.extend(parent / ".env" for parent in package_path.parents)
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return package_path.parents[1] / ".env"

    async def _save_credentials(self, api_key: str, api_secret: str) -> None:
        """Persist Alpaca credentials locally without exposing them to Git."""
        path = self._credential_env_path()

        def write() -> None:
            text = path.read_text(encoding="utf-8") if path.is_file() else ""
            updates = {
                "APCA_API_KEY_ID": api_key,
                "APCA_API_SECRET_KEY": api_secret,
            }
            lines = text.splitlines(keepends=True)
            seen: set[str] = set()
            output: list[str] = []
            for line in lines:
                match = re.match(r"^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=", line)
                name = match.group(1) if match else ""
                if name in updates:
                    output.append(f"{name}={updates[name]}\n")
                    seen.add(name)
                else:
                    output.append(line)
            if output and not output[-1].endswith(("\n", "\r")):
                output[-1] += "\n"
            for name, value in updates.items():
                if name not in seen:
                    output.append(f"{name}={value}\n")
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_text("".join(output), encoding="utf-8", newline="")
            os.replace(temporary, path)

        await asyncio.to_thread(write)
        os.environ["APCA_API_KEY_ID"] = api_key
        os.environ["APCA_API_SECRET_KEY"] = api_secret
        self.args.alpaca_key = api_key
        self.args.alpaca_secret = api_secret

    async def credentials_reset(self, request: web.Request) -> web.Response:
        """Remove locally persisted Alpaca credentials and clear this process."""
        path = self._credential_env_path()

        def remove() -> None:
            if not path.is_file():
                return
            names = {"APCA_API_KEY_ID", "APCA_API_SECRET_KEY", "ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"}
            lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
            kept = []
            for line in lines:
                match = re.match(r"^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=", line)
                if match and match.group(1) in names:
                    continue
                kept.append(line)
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_text("".join(kept), encoding="utf-8", newline="")
            os.replace(temporary, path)

        await asyncio.to_thread(remove)
        for name in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY", "ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"):
            os.environ.pop(name, None)
        self.args.alpaca_key = None
        self.args.alpaca_secret = None
        return web.json_response({"configured": False})

    async def replay_prepare_info(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                **self.replay_prepare_state,
                "symbol_cap": REPLAY_SYMBOL_CAP,
                "credentials_configured": self._credentials_configured(),
                "current_symbols": list(self.runtime.symbols)[:REPLAY_SYMBOL_CAP],
                "feed": "sip",
                "session": "regular",
                "historical_rpm": 200,
            },
            headers={"Cache-Control": "no-store"},
        )

    async def replay_prepare(self, request: web.Request) -> web.Response:
        if self.replay_prepare_task and not self.replay_prepare_task.done():
            raise web.HTTPConflict(text="A replay download is already in progress")
        try:
            body = await request.json()
        except Exception:
            body = {}
        try:
            raw_symbols = body.get("symbols") or []
            if isinstance(raw_symbols, str):
                raw_symbols = [raw_symbols]
            symbols = clean_replay_symbols([str(value) for value in raw_symbols])
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc

        api_key = str(body.get("api_key") or self.args.alpaca_key or "") or None
        api_secret = str(body.get("api_secret") or self.args.alpaca_secret or "") or None
        try:
            trading_day = date.fromisoformat(str(body.get("date") or ""))
        except ValueError as exc:
            raise web.HTTPBadRequest(text="Replay date must use YYYY-MM-DD") from exc
        try:
            await validate_replay_request(
                symbol=symbols[0],
                trading_day=trading_day,
                api_key=api_key,
                api_secret=api_secret,
            )
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        except RuntimeError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        if not self._credentials_configured() and api_key and api_secret:
            await self._save_credentials(api_key, api_secret)
        # Stop LIVE as soon as Replay has passed validation.  The historical
        # download can take time; keeping the adapter alive during that gap
        # would continue market ingestion and leave the UI in LIVE mode.
        if not self._runtime_stopped:
            await self.runtime.stop()
            self._runtime_stopped = True
        self.replay_transition = True
        replay_db = Path(self.args.replay_db or "data/alpaca_replay.sqlite")
        self.replay_prepare_state = {
            "state": "preparing",
            "stage": "queued",
            "runtime_mode": "REPLAY",
            "preparing": True,
            "symbols": symbols,
            "symbol_cap": REPLAY_SYMBOL_CAP,
            "feed": "sip",
            "trading_day": trading_day.isoformat(),
        }
        await self._push_snapshot_to_clients()
        self.replay_prepare_task = asyncio.create_task(
            self._prepare_previous_session(
                symbols=symbols,
                api_key=api_key,
                api_secret=api_secret,
                replay_db=replay_db,
                trading_day=trading_day,
            )
        )
        return web.json_response(self.replay_prepare_state, status=202)

    async def _restart_broadcaster(self) -> None:
        old = self.broadcaster
        if old and old is not asyncio.current_task():
            old.cancel()
            try:
                await old
            except asyncio.CancelledError:
                pass
        self.broadcaster = asyncio.create_task(self.broadcast_loop())

    async def _activate_replay_symbols(self, payload: dict[str, Any]) -> None:
        symbols = [str(symbol).upper() for symbol in payload.get("ready_symbols", [])]
        if not symbols:
            return
        old_runtime = self.runtime
        previous = old_runtime.replay_state() if isinstance(old_runtime, ReplayRuntime) else None
        replay = ReplayRuntime(
            symbols=symbols,
            db_path=payload["db_path"],
            source_session=payload["session_id"],
            duration_sec=24 * 60 * 60,
            speed=self.replay_speed,
        )
        await replay.start()
        if previous is not None:
            await replay.seek(int(previous.get("market_time_ns") or replay.start_ns))
            if previous.get("playing"):
                await replay.play()
        old_runtime_was_stopped = self._runtime_stopped
        self.runtime = replay
        self.replay_transition = False
        self._runtime_stopped = False
        await self._restart_broadcaster()
        if old_runtime is not replay and not old_runtime_was_stopped:
            await old_runtime.stop()
        self.replay_prepare_state.update(
            {
                "active_symbols": symbols,
                "ready_symbols": symbols,
                "runtime_mode": "REPLAY",
                "session_id": payload["session_id"],
                "trading_day": payload["trading_day"],
                "replay_db": str(Path(payload["db_path"]).resolve()),
            }
        )
        await self._push_snapshot_to_clients()

    async def _push_snapshot_to_clients(self) -> None:
        data = self._snapshot_payload(include_history=True)
        payload = json.dumps(
            {"type": "snapshot", "data": data},
            separators=(",", ":"),
            default=str,
        )
        dead: list[web.WebSocketResponse] = []
        for ws in tuple(self.clients):
            try:
                await ws.send_str(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)

    async def _prepare_previous_session(
        self,
        *,
        symbols: list[str],
        api_key: str | None,
        api_secret: str | None,
        replay_db: Path,
        trading_day: date,
    ) -> None:
        new_runtime: ReplayRuntime | None = None

        def progress(payload: dict[str, Any]) -> None:
            self.replay_prepare_state.update(payload)
            self.replay_prepare_state["state"] = "preparing"
            self.replay_prepare_state["symbol_cap"] = REPLAY_SYMBOL_CAP

        try:
            prepared = await build_previous_session_replay(
                symbols=symbols,
                db_path=replay_db,
                api_key=api_key,
                api_secret=api_secret,
                speed=1.0,
                trading_day=trading_day,
                on_progress=progress,
                on_symbol_ready=self._activate_replay_symbols,
            )
            new_runtime = prepared.runtime
            self.replay_prepare_state.update(
                {
                    "state": "preparing",
                    "stage": "switching",
                    "trading_day": prepared.trading_day.isoformat(),
                    "quotes": prepared.stats.quotes,
                    "trades": prepared.stats.trades,
                    "requests": prepared.stats.requests,
                }
            )

            # The callback already activates each ready symbol incrementally.
            # The builder returns a final all-symbol runtime as a convenience;
            # keep the callback-managed runtime and dispose of this duplicate.
            if new_runtime is not self.runtime:
                await new_runtime.stop()
            self.replay_prepare_state.update(
                {
                    "state": "ready",
                    "stage": "ready",
                    "session_id": prepared.stats.session_id,
                    "trading_day": prepared.trading_day.isoformat(),
                    "symbols": symbols,
                    "quotes": prepared.stats.quotes,
                    "trades": prepared.stats.trades,
                    "requests": prepared.stats.requests,
                    "replay_db": str(prepared.db_path.resolve()),
                    "active_symbols": list(symbols),
                    "ready_symbols": list(symbols),
                    "runtime_mode": "REPLAY",
                }
            )
            new_runtime = None
        except asyncio.CancelledError:
            if new_runtime is not None:
                await new_runtime.stop()
            raise
        except Exception as exc:
            if new_runtime is not None:
                await new_runtime.stop()
            self.replay_prepare_state.update(
                {
                    "state": "error",
                    "stage": "error",
                    "message": str(exc),
                }
            )

    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20, max_msg_size=2 * 1024 * 1024)
        await ws.prepare(request)
        self.clients.add(ws)
        await ws.send_json({"type": "snapshot", "data": self._snapshot_payload()})
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                    except json.JSONDecodeError:
                        continue
                    if payload.get("action") == "ping":
                        await ws.send_json({"type": "pong"})
                elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            self.clients.discard(ws)
        return ws

    async def broadcast_loop(self) -> None:
        while True:
            payload = await self.runtime.event_queue.get()
            if not self.clients:
                continue
            text = json.dumps(payload, separators=(",", ":"), default=str)
            dead = []
            for ws in tuple(self.clients):
                try:
                    await ws.send_str(text)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self.clients.discard(ws)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/", self.page)
        app.router.add_get("/flow", self.page)
        app.router.add_get("/flow.html", self.page)
        app.router.add_get("/radar", self.radar)
        app.router.add_get("/radar.html", self.radar)
        app.router.add_get("/learn", self.learn)
        app.router.add_get("/teaching", self.learn)
        app.router.add_get("/static/{name}", self.asset)
        app.router.add_get("/api/status", self.status)
        app.router.add_get("/api/snapshot", self.snapshot)
        app.router.add_get("/api/market-reference", self.market_reference)
        app.router.add_get("/api/history", self.history)
        app.router.add_get("/api/replay", self.replay_state)
        app.router.add_post("/api/replay/control", self.replay_control)
        app.router.add_get("/api/replay/prepare", self.replay_prepare_info)
        app.router.add_post("/api/replay/prepare", self.replay_prepare)
        app.router.add_post("/api/credentials/reset", self.credentials_reset)
        app.router.add_post("/api/live/switch", self.live_switch)
        app.router.add_get("/ws", self.ws)
        return app


async def serve(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parents[1]
    if args.source == "replay":
        runtime = ReplayRuntime(
            symbols=args.symbols,
            db_path=args.replay_db or args.db,
            source_session=args.replay_session,
            start_ns=args.replay_start_ns,
            duration_sec=args.replay_duration * 60,
            speed=args.replay_speed,
        )
    elif args.provider == "alpaca":
        runtime = AlpacaOrderFlowRuntime(
            symbols=args.symbols,
            db_path=args.db,
            feed=args.alpaca_feed,
            api_key=args.alpaca_key,
            api_secret=args.alpaca_secret,
        )
    else:
        runtime = OrderFlowRuntime(
            symbols=args.symbols,
            mode=args.mode,
            db_path=args.db,
            ib_host=args.ib_host,
            ib_port=args.ib_port,
            client_id=args.client_id,
            market_data_lines=args.market_data_lines,
        )
    await runtime.start()
    server = WorkstationServer(runtime, root, args)
    runner = web.AppRunner(server.app(), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, args.http_host, args.http_port).start()
    server.broadcaster = asyncio.create_task(server.broadcast_loop())

    print(f"OrderFlowMap Workstation: http://{args.http_host}:{args.http_port}/")
    print(f"OrderFlowMap Radar:       http://{args.http_host}:{args.http_port}/radar")
    print(f"OrderFlowMap Learn:       http://{args.http_host}:{args.http_port}/learn")
    if isinstance(runtime, ReplayRuntime):
        print(f"GLOBAL MODE=REPLAY source_session={runtime.source_session} read_only=True")
        print(f"Replay range={runtime.start_ns}..{runtime.end_ns} speed={runtime.speed}x")
        print("Source SQLite is never written by ReplayRuntime.")
    elif isinstance(runtime, AlpacaOrderFlowRuntime):
        print(
            f"GLOBAL MODE=LIVE provider=ALPACA feed={runtime.alpaca_feed} "
            f"quality={runtime.plan.quality}"
        )
        print(f"SQLite={Path(args.db).resolve()}")
    else:
        print(
            f"GLOBAL MODE=LIVE provider=IBKR mode={runtime.plan.active_mode} "
            f"quality={runtime.plan.quality}"
        )
        print(f"SQLite={Path(args.db).resolve()}")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    try:
        await stop.wait()
    finally:
        if server.replay_prepare_task and not server.replay_prepare_task.done():
            server.replay_prepare_task.cancel()
            try:
                await server.replay_prepare_task
            except asyncio.CancelledError:
                pass
        if server.broadcaster:
            server.broadcaster.cancel()
            try:
                await server.broadcaster
            except asyncio.CancelledError:
                pass
        await runner.cleanup()
        await server.runtime.stop()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Order-flow workstation with IBKR/Alpaca LIVE and global SQLite REPLAY sources"
    )
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--source", choices=["live", "replay"], default="live")
    p.add_argument(
        "--provider",
        choices=["ibkr", "alpaca"],
        default="ibkr",
        help="LIVE market-data provider; ignored in REPLAY mode",
    )
    p.add_argument(
        "--mode",
        choices=["auto", "focus", "radar"],
        default="auto",
        help="IBKR LIVE mode",
    )
    p.add_argument("--ib-host", default="127.0.0.1")
    p.add_argument("--ib-port", type=int, default=7497)
    p.add_argument("--client-id", type=int, default=4712)
    p.add_argument("--market-data-lines", type=int, default=100)
    p.add_argument(
        "--alpaca-feed",
        choices=["iex", "sip", "delayed_sip"],
        default="iex",
        help="Alpaca LIVE stock feed. IEX is the normal free live feed; SIP requires entitlement.",
    )
    p.add_argument("--alpaca-key", default=None, help="Defaults to APCA_API_KEY_ID")
    p.add_argument("--alpaca-secret", default=None, help="Defaults to APCA_API_SECRET_KEY")
    p.add_argument("--db", default="data/orderflow.sqlite")
    p.add_argument(
        "--replay-db",
        default=None,
        help="Read-only startup replay SQLite; LIVE one-click replay uses data/alpaca_replay.sqlite when omitted",
    )
    p.add_argument(
        "--replay-session", default=None, help="Source recording session id; auto-select if omitted"
    )
    p.add_argument("--replay-start-ns", type=int, default=None)
    p.add_argument(
        "--replay-duration", type=int, default=30, help="Replay market minutes; default 30"
    )
    p.add_argument("--replay-speed", type=float, default=1.0)
    p.add_argument("--http-host", default="127.0.0.1")
    p.add_argument("--http-port", type=int, default=8765)
    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

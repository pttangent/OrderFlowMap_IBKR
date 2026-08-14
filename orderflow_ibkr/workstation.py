from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web

from .history import load_history
from .replay_runtime import ReplayRuntime
from .runtime import OrderFlowRuntime

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
    def __init__(self, runtime: Any, root: Path):
        self.runtime = runtime
        self.root = root
        self.clients: set[web.WebSocketResponse] = set()
        self.broadcaster: asyncio.Task | None = None

    async def page(self, request: web.Request) -> web.Response:
        html = (self.root / "flow_v2.html").read_text(encoding="utf-8")
        hooks = (
            '<link rel="stylesheet" href="/static/workstation-shell.css">'
            '<script src="/static/footprint-timeseries.js"></script>'
            '<script src="/static/workstation-shell.js"></script>'
        )
        if "/static/workstation-shell.js" not in html:
            html = html.replace("</head>", '<link rel="stylesheet" href="/static/workstation-shell.css"></head>')
            html = html.replace(
                "</body>",
                '<script src="/static/footprint-timeseries.js"></script>'
                '<script src="/static/workstation-shell.js"></script></body>',
            )
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store"})

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
        return web.FileResponse(self.root / "static" / name, headers={"Cache-Control": "no-store"})

    async def status(self, request: web.Request) -> web.Response:
        return web.json_response(self.runtime.status)

    async def snapshot(self, request: web.Request) -> web.Response:
        return web.json_response(self.runtime.snapshot())

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
        # LIVE warm-starts from the current session. REPLAY is reconstructed from
        # the read-only event source, so exposing source-session future rows here
        # would create look-ahead leakage. Return only the current snapshot state.
        if isinstance(self.runtime, ReplayRuntime):
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
            return web.json_response({"runtime_mode": "LIVE", "available": False})
        return web.json_response({"available": True, **self.runtime.replay_state()})

    async def replay_control(self, request: web.Request) -> web.Response:
        if not isinstance(self.runtime, ReplayRuntime):
            raise web.HTTPConflict(text="Workstation is running in LIVE mode. Restart with --source replay.")
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
                target_ns = self.runtime.start_ns + int((self.runtime.end_ns - self.runtime.start_ns) * progress)
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

    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20, max_msg_size=2 * 1024 * 1024)
        await ws.prepare(request)
        self.clients.add(ws)
        await ws.send_json({"type": "snapshot", "data": self.runtime.snapshot()})
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
        app.router.add_get("/api/history", self.history)
        app.router.add_get("/api/replay", self.replay_state)
        app.router.add_post("/api/replay/control", self.replay_control)
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
    server = WorkstationServer(runtime, root)
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
    else:
        print(f"GLOBAL MODE=LIVE mode={runtime.plan.active_mode} quality={runtime.plan.quality}")
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
        if server.broadcaster:
            server.broadcaster.cancel()
            try:
                await server.broadcaster
            except asyncio.CancelledError:
                pass
        await runner.cleanup()
        await runtime.stop()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="IBKR order-flow workstation with LIVE / global REPLAY sources")
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--source", choices=["live", "replay"], default="live")
    p.add_argument("--mode", choices=["auto", "focus", "radar"], default="auto")
    p.add_argument("--ib-host", default="127.0.0.1")
    p.add_argument("--ib-port", type=int, default=7497)
    p.add_argument("--client-id", type=int, default=4712)
    p.add_argument("--market-data-lines", type=int, default=100)
    p.add_argument("--db", default="data/orderflow.sqlite")
    p.add_argument("--replay-db", default=None, help="Read-only recording SQLite; defaults to --db")
    p.add_argument("--replay-session", default=None, help="Source recording session id; auto-select if omitted")
    p.add_argument("--replay-start-ns", type=int, default=None)
    p.add_argument("--replay-duration", type=int, default=30, help="Replay market minutes; default 30")
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

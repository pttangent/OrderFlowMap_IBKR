from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path

from aiohttp import WSMsgType, web

from .runtime import OrderFlowRuntime


# The radar dashboard uses Lightweight Charts line series and can receive many
# updates in the same second. Inject a small coalescing patch only into that
# legacy/dashboard page. The primary flow.html already handles this itself.
_SERIES_PATCH = """
<script>
pushSeries = function(map, sym, p, max=2400) {
  const arr = (map[sym] ??= []);
  if (arr.length && arr[arr.length - 1].time === p.time) arr[arr.length - 1] = p;
  else arr.push(p);
  if (arr.length > max) arr.splice(0, arr.length - max);
};
</script>
"""


class WorkstationServer:
    def __init__(self, runtime: OrderFlowRuntime, flow_frontend: Path, radar_frontend: Path):
        self.runtime = runtime
        self.flow_frontend = flow_frontend
        self.radar_frontend = radar_frontend
        self.clients: set[web.WebSocketResponse] = set()
        self.broadcaster: asyncio.Task | None = None

    async def flow(self, request: web.Request) -> web.Response:
        return web.Response(
            text=self.flow_frontend.read_text(encoding="utf-8"),
            content_type="text/html",
        )

    async def radar(self, request: web.Request) -> web.Response:
        html = self.radar_frontend.read_text(encoding="utf-8")
        html = html.replace("</body>", _SERIES_PATCH + "</body>")
        return web.Response(text=html, content_type="text/html")

    async def status(self, request: web.Request) -> web.Response:
        return web.json_response(self.runtime.status)

    async def snapshot(self, request: web.Request) -> web.Response:
        return web.json_response(self.runtime.snapshot())

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
            dead = []
            text = json.dumps(payload, separators=(",", ":"), default=str)
            for ws in tuple(self.clients):
                try:
                    await ws.send_str(text)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self.clients.discard(ws)

    def app(self) -> web.Application:
        app = web.Application()
        # Primary entry = visual order-flow workstation.
        app.router.add_get("/", self.flow)
        app.router.add_get("/flow", self.flow)
        app.router.add_get("/flow.html", self.flow)
        # Multi-symbol scanner / Agent dashboard remains available separately.
        app.router.add_get("/radar", self.radar)
        app.router.add_get("/radar.html", self.radar)
        app.router.add_get("/index.html", self.flow)
        app.router.add_get("/api/status", self.status)
        app.router.add_get("/api/snapshot", self.snapshot)
        app.router.add_get("/ws", self.ws)
        return app


async def _serve(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parents[1]
    flow_frontend = root / "flow.html"
    radar_frontend = root / "index.html"
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

    server = WorkstationServer(runtime, flow_frontend, radar_frontend)
    runner = web.AppRunner(server.app(), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, args.http_host, args.http_port)
    await site.start()
    server.broadcaster = asyncio.create_task(server.broadcast_loop())

    print(f"OrderFlowMap IBKR Flow:  http://{args.http_host}:{args.http_port}/")
    print(f"OrderFlowMap IBKR Radar: http://{args.http_host}:{args.http_port}/radar")
    print(f"Mode={runtime.plan.active_mode} quality={runtime.plan.quality} symbols={','.join(args.symbols)}")
    print(f"SQLite={Path(args.db).resolve()} (WAL; safe for concurrent mode=ro readers)")

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
    p = argparse.ArgumentParser(description="IBKR dual-mode order-flow workstation")
    p.add_argument("--symbols", nargs="+", required=True, help="US equity symbols")
    p.add_argument("--mode", choices=["auto", "focus", "radar"], default="auto")
    p.add_argument("--ib-host", default="127.0.0.1")
    p.add_argument("--ib-port", type=int, default=7497, help="Paper TWS default 7497; live TWS often 7496")
    p.add_argument("--client-id", type=int, default=4712)
    p.add_argument("--market-data-lines", type=int, default=100)
    p.add_argument("--db", default="data/orderflow.sqlite")
    p.add_argument("--http-host", default="127.0.0.1")
    p.add_argument("--http-port", type=int, default=8765)
    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        asyncio.run(_serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

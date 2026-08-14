from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Callable, Literal

import aiohttp
from aiohttp import WSMsgType

from .engine import QuoteEvent, TradeEvent
from .ibkr_adapter import Plan

AlpacaFeed = Literal["iex", "sip", "delayed_sip"]


def _rfc3339_to_ns(value: str) -> int:
    """Parse Alpaca RFC-3339 timestamps without losing nanosecond precision."""
    text = str(value).strip()
    if not text:
        raise ValueError("empty Alpaca timestamp")
    if not text.endswith("Z"):
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    body = text[:-1]
    whole, sep, frac = body.partition(".")
    sec = int(datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp())
    nanos = int(frac[:9].ljust(9, "0")) if sep else 0
    return sec * 1_000_000_000 + nanos


def _quote_size_shares(value: object) -> float | None:
    """Alpaca stock quote sizes are round lots; common runtime uses shares."""
    if value is None:
        return None
    return float(value) * 100.0


class AlpacaAdapter:
    """Read-only Alpaca stock WebSocket -> common QuoteEvent/TradeEvent adapter."""

    def __init__(
        self,
        *,
        symbols: list[str],
        feed: AlpacaFeed = "iex",
        api_key: str | None = None,
        api_secret: str | None = None,
        on_quote: Callable[[str, QuoteEvent], None] | None = None,
        on_trade: Callable[[str, TradeEvent], None] | None = None,
        on_status: Callable[[dict], None] | None = None,
        reconnect: bool = True,
    ) -> None:
        cleaned: list[str] = []
        for symbol in symbols:
            value = symbol.strip().upper()
            if value and value not in cleaned:
                cleaned.append(value)
        if not cleaned:
            raise ValueError("At least one symbol is required")
        if feed not in {"iex", "sip", "delayed_sip"}:
            raise ValueError(f"Unsupported Alpaca stock feed: {feed}")
        self.symbols = cleaned
        self.feed: AlpacaFeed = feed
        self.api_key = api_key or os.getenv("APCA_API_KEY_ID")
        self.api_secret = api_secret or os.getenv("APCA_API_SECRET_KEY")
        self.on_quote = on_quote or (lambda *_: None)
        self.on_trade = on_trade or (lambda *_: None)
        self.on_status = on_status or (lambda *_: None)
        self.reconnect = reconnect
        quality = {
            "sip": "ALPACA_SIP_TRADES_QUOTES",
            "iex": "ALPACA_IEX_TRADES_QUOTES",
            "delayed_sip": "ALPACA_DELAYED_SIP_TRADES_QUOTES",
        }[feed]
        self.plan = Plan(
            requested_mode=f"alpaca-{feed}",
            active_mode=f"alpaca-{feed}",
            symbols=self.symbols,
            market_data_lines=len(self.symbols),
            tbt_capacity=len(self.symbols),
            quality=quality,
            trade_source=f"ALPACA_WS_{feed.upper()}_TRADE",
            quote_source=f"ALPACA_WS_{feed.upper()}_QUOTE",
        )
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._reader_task: asyncio.Task | None = None
        self._closing = False

    @property
    def url(self) -> str:
        return f"wss://stream.data.alpaca.markets/v2/{self.feed}"

    async def _receive_batch(self, timeout: float = 10.0) -> list[dict]:
        if self._ws is None:
            raise RuntimeError("Alpaca websocket is not open")
        msg = await asyncio.wait_for(self._ws.receive(), timeout=timeout)
        if msg.type == WSMsgType.TEXT:
            payload = json.loads(msg.data)
        elif msg.type == WSMsgType.BINARY:
            payload = json.loads(msg.data.decode("utf-8"))
        elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING):
            raise ConnectionError("Alpaca websocket closed during handshake")
        elif msg.type == WSMsgType.ERROR:
            raise ConnectionError(f"Alpaca websocket error: {self._ws.exception()}")
        else:
            return []
        return [payload] if isinstance(payload, dict) else [x for x in payload if isinstance(x, dict)]

    @staticmethod
    def _raise_errors(batch: list[dict], stage: str) -> None:
        for item in batch:
            if item.get("T") == "error":
                raise RuntimeError(
                    f"Alpaca {stage} error {item.get('code')}: "
                    f"{item.get('msg') or item.get('message') or 'unknown error'}"
                )

    async def _open_and_subscribe(self) -> None:
        if not self.api_key or not self.api_secret:
            raise RuntimeError(
                "Alpaca credentials are required. Set APCA_API_KEY_ID and "
                "APCA_API_SECRET_KEY or pass --alpaca-key/--alpaca-secret."
            )
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        self.on_status({"state": "connecting", "provider": "alpaca", "feed": self.feed})
        self._ws = await self._session.ws_connect(
            self.url, heartbeat=20, autoping=True, max_msg_size=16 * 1024 * 1024
        )
        connected = await self._receive_batch()
        self._raise_errors(connected, "connect")
        if not any(x.get("T") == "success" and x.get("msg") == "connected" for x in connected):
            raise RuntimeError(f"Unexpected Alpaca connect response: {connected!r}")
        await self._ws.send_json({"action": "auth", "key": self.api_key, "secret": self.api_secret})
        auth = await self._receive_batch()
        self._raise_errors(auth, "auth")
        if not any(x.get("T") == "success" and x.get("msg") == "authenticated" for x in auth):
            raise RuntimeError(f"Unexpected Alpaca auth response: {auth!r}")
        await self._ws.send_json({"action": "subscribe", "trades": self.symbols, "quotes": self.symbols})
        subscribed = await self._receive_batch()
        self._raise_errors(subscribed, "subscribe")
        if not any(x.get("T") == "subscription" for x in subscribed):
            raise RuntimeError(f"Unexpected Alpaca subscription response: {subscribed!r}")
        self.on_status(
            {
                "state": "connected",
                "provider": "alpaca",
                "feed": self.feed,
                "mode": self.plan.active_mode,
                "quality": self.plan.quality,
                "symbols": self.symbols,
            }
        )

    def _handle_item(self, item: dict) -> None:
        kind = item.get("T")
        symbol = str(item.get("S") or "").upper()
        if symbol not in self.symbols:
            return
        if kind == "q":
            self.on_quote(
                symbol,
                QuoteEvent(
                    ts_ns=_rfc3339_to_ns(item["t"]),
                    bid=float(item["bp"]) if item.get("bp") is not None else None,
                    ask=float(item["ap"]) if item.get("ap") is not None else None,
                    bid_size=_quote_size_shares(item.get("bs")),
                    ask_size=_quote_size_shares(item.get("as")),
                    source=self.plan.quote_source,
                    quality=self.plan.quality,
                ),
            )
        elif kind == "t":
            self.on_trade(
                symbol,
                TradeEvent(
                    ts_ns=_rfc3339_to_ns(item["t"]),
                    price=float(item["p"]),
                    size=float(item["s"]),
                    exchange=str(item.get("x") or "") or None,
                    conditions=[str(x) for x in (item.get("c") or [])] or None,
                    source=self.plan.trade_source,
                    quality=self.plan.quality,
                ),
            )
        elif kind == "error":
            self.on_status(
                {
                    "state": "warning",
                    "provider": "alpaca",
                    "feed": self.feed,
                    "code": item.get("code"),
                    "message": item.get("msg") or item.get("message"),
                }
            )

    async def _reader_loop(self) -> None:
        backoff = 1.0
        while not self._closing:
            try:
                if self._ws is None:
                    await self._open_and_subscribe()
                assert self._ws is not None
                async for msg in self._ws:
                    if self._closing:
                        return
                    if msg.type == WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        payload = json.loads(msg.data.decode("utf-8"))
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING):
                        break
                    elif msg.type == WSMsgType.ERROR:
                        raise ConnectionError(str(self._ws.exception()))
                    else:
                        continue
                    for item in payload if isinstance(payload, list) else [payload]:
                        if isinstance(item, dict):
                            self._handle_item(item)
                if self._closing or not self.reconnect:
                    return
                raise ConnectionError("Alpaca websocket disconnected")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._closing or not self.reconnect:
                    if not self._closing:
                        self.on_status({"state": "error", "provider": "alpaca", "feed": self.feed, "message": str(exc)})
                    return
                self.on_status(
                    {"state": "reconnecting", "provider": "alpaca", "feed": self.feed, "message": str(exc), "retry_sec": backoff}
                )
                if self._ws is not None:
                    await self._ws.close()
                self._ws = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            else:
                backoff = 1.0

    async def connect(self) -> Plan:
        self._closing = False
        await self._open_and_subscribe()
        self._reader_task = asyncio.create_task(self._reader_loop())
        return self.plan

    async def disconnect(self) -> None:
        self._closing = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self.on_status({"state": "disconnected", "provider": "alpaca", "feed": self.feed})

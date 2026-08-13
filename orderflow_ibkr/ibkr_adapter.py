from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Literal

from ib_async import IB, Stock

from .engine import QuoteEvent, TradeEvent

Mode = Literal["auto", "focus", "radar"]


def _value(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _to_ns(dt: datetime | None) -> int:
    if dt is None:
        return time.time_ns()
    try:
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return time.time_ns()


@dataclass(slots=True)
class Plan:
    requested_mode: str
    active_mode: str
    symbols: list[str]
    market_data_lines: int
    tbt_capacity: int
    quality: str
    trade_source: str
    quote_source: str


class IBKRAdapter:
    """Read-only IBKR market-data adapter with two explicit quality modes.

    FOCUS:
      - up to five symbols by product design
      - true AllLast tick-by-tick executions
      - reqMktData BBO/quote context

    RADAR:
      - larger universe up to available market-data lines
      - reqMktData snapshots + generic ticks
      - reconstructed flow is explicitly marked as proxy
    """

    GENERIC_TICKS = "233,293,294,295,375"

    def __init__(
        self,
        *,
        symbols: list[str],
        mode: Mode = "auto",
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 4712,
        market_data_lines: int = 100,
        focus_product_cap: int = 5,
        on_quote: Callable[[str, QuoteEvent], None] | None = None,
        on_trade: Callable[[str, TradeEvent], None] | None = None,
        on_status: Callable[[dict], None] | None = None,
    ):
        cleaned = []
        for s in symbols:
            u = s.strip().upper()
            if u and u not in cleaned:
                cleaned.append(u)
        if not cleaned:
            raise ValueError("At least one symbol is required")
        self.symbols = cleaned
        self.mode = mode
        self.host = host
        self.port = port
        self.client_id = client_id
        self.market_data_lines = max(1, int(market_data_lines))
        self.focus_product_cap = max(1, int(focus_product_cap))
        self.on_quote = on_quote or (lambda *_: None)
        self.on_trade = on_trade or (lambda *_: None)
        self.on_status = on_status or (lambda *_: None)

        self.ib = IB()
        self.plan = self._make_plan()
        self.contracts: dict[str, object] = {}
        self.tickers: dict[str, object] = {}
        self._ticker_to_symbol: dict[int, str] = {}
        self._connected = False

    def _make_plan(self) -> Plan:
        tbt_capacity = max(1, int(self.market_data_lines * 0.05))
        focus_cap = min(self.focus_product_cap, tbt_capacity)
        active = self.mode
        if self.mode == "auto":
            active = "focus" if len(self.symbols) <= focus_cap else "radar"
        if active == "focus" and len(self.symbols) > focus_cap:
            raise ValueError(
                f"FOCUS supports {focus_cap} symbols with {self.market_data_lines} market-data lines; "
                f"received {len(self.symbols)}. Use --mode radar or --mode auto."
            )
        if active == "radar" and len(self.symbols) > self.market_data_lines:
            raise ValueError(
                f"RADAR requested {len(self.symbols)} symbols but market_data_lines={self.market_data_lines}. "
                "TWS watchlists and other API subscriptions share this pool, so leave headroom."
            )
        if active == "focus":
            quality = "TBT_TRADES_MKTDATA_QUOTES"
            trade_source = "TBT_ALL_LAST"
            quote_source = "REQ_MKT_DATA"
        else:
            quality = "MKTDATA_SNAPSHOT_PROXY"
            trade_source = "REQ_MKT_DATA_VOLUME_DELTA"
            quote_source = "REQ_MKT_DATA"
        return Plan(
            requested_mode=self.mode,
            active_mode=active,
            symbols=self.symbols,
            market_data_lines=self.market_data_lines,
            tbt_capacity=tbt_capacity,
            quality=quality,
            trade_source=trade_source,
            quote_source=quote_source,
        )

    async def connect(self) -> Plan:
        self.on_status({"state": "connecting", "host": self.host, "port": self.port})
        await self.ib.connectAsync(
            self.host,
            self.port,
            clientId=self.client_id,
            timeout=8,
            readonly=True,
        )
        contracts = [Stock(s, "SMART", "USD") for s in self.symbols]
        qualified = await self.ib.qualifyContractsAsync(*contracts)
        failed = [s for s, c in zip(self.symbols, qualified) if c is None]
        if failed:
            self.ib.disconnect()
            raise RuntimeError(f"Unable to qualify contracts: {', '.join(failed)}")

        self.ib.pendingTickersEvent += self._on_pending_tickers
        for symbol, contract in zip(self.symbols, qualified):
            self.contracts[symbol] = contract
            ticker = self.ib.reqMktData(
                contract,
                genericTickList=self.GENERIC_TICKS,
                snapshot=False,
                regulatorySnapshot=False,
            )
            self.tickers[symbol] = ticker
            self._ticker_to_symbol[id(ticker)] = symbol
            if self.plan.active_mode == "focus":
                # Same ib_async Ticker object is reused for this contract; AllLast
                # events arrive in ticker.tickByTicks while reqMktData maintains BBO.
                self.ib.reqTickByTickData(contract, "AllLast", 0, False)
            await asyncio.sleep(0.03)

        self._connected = True
        self.on_status(
            {
                "state": "connected",
                "mode": self.plan.active_mode,
                "quality": self.plan.quality,
                "symbols": self.symbols,
                "tbt_capacity": self.plan.tbt_capacity,
            }
        )
        return self.plan

    def _on_pending_tickers(self, tickers) -> None:
        for ticker in tickers:
            symbol = self._ticker_to_symbol.get(id(ticker))
            if not symbol:
                contract = getattr(ticker, "contract", None)
                symbol = getattr(contract, "symbol", "").upper() if contract else ""
            if not symbol:
                continue

            # reqMktData ticks and generic ticks are present only in ticker.ticks
            # for the current network packet. Avoid writing an unchanged quote for
            # every TBT trade packet.
            if getattr(ticker, "ticks", None):
                quote = QuoteEvent(
                    ts_ns=_to_ns(getattr(ticker, "time", None)),
                    bid=_value(getattr(ticker, "bid", None)),
                    ask=_value(getattr(ticker, "ask", None)),
                    bid_size=_value(getattr(ticker, "bidSize", None)),
                    ask_size=_value(getattr(ticker, "askSize", None)),
                    last=_value(getattr(ticker, "last", None)),
                    last_size=_value(getattr(ticker, "lastSize", None)),
                    volume=_value(getattr(ticker, "volume", None)),
                    vwap=_value(getattr(ticker, "vwap", None)),
                    trade_count=_value(getattr(ticker, "tradeCount", None)),
                    trade_rate=_value(getattr(ticker, "tradeRate", None)),
                    volume_rate=_value(getattr(ticker, "volumeRate", None)),
                    source="REQ_MKT_DATA",
                    quality=self.plan.quality,
                )
                self.on_quote(symbol, quote)

            if self.plan.active_mode == "focus":
                for tick in getattr(ticker, "tickByTicks", []) or []:
                    # AllLast has price/size; BidAsk does not. This product mode
                    # subscribes only to AllLast so 5 symbols fit the default 5 TBT slots.
                    if not hasattr(tick, "price") or not hasattr(tick, "size"):
                        continue
                    conditions = getattr(tick, "specialConditions", "") or ""
                    event = TradeEvent(
                        ts_ns=_to_ns(getattr(tick, "time", None)),
                        price=float(tick.price),
                        size=float(tick.size),
                        exchange=getattr(tick, "exchange", None) or None,
                        conditions=[x for x in str(conditions).split() if x] or None,
                        source="TBT_ALL_LAST",
                        quality=self.plan.quality,
                    )
                    self.on_trade(symbol, event)

    async def disconnect(self) -> None:
        if not self._connected:
            return
        try:
            self.ib.pendingTickersEvent -= self._on_pending_tickers
        except Exception:
            pass
        for symbol, contract in list(self.contracts.items()):
            try:
                self.ib.cancelMktData(contract)
            except Exception:
                pass
            if self.plan.active_mode == "focus":
                try:
                    self.ib.cancelTickByTickData(contract, "AllLast")
                except Exception:
                    pass
        self.ib.disconnect()
        self._connected = False
        self.on_status({"state": "disconnected"})

from __future__ import annotations

import json
import math
import statistics
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from typing import Any, Deque


def _finite(value: Any) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


@dataclass(slots=True)
class QuoteEvent:
    ts_ns: int
    bid: float | None
    ask: float | None
    bid_size: float | None
    ask_size: float | None
    last: float | None = None
    last_size: float | None = None
    volume: float | None = None
    vwap: float | None = None
    trade_count: float | None = None
    trade_rate: float | None = None
    volume_rate: float | None = None
    source: str = "REQ_MKT_DATA"
    quality: str = "MKTDATA_SNAPSHOT_PROXY"


@dataclass(slots=True)
class TradeEvent:
    ts_ns: int
    price: float
    size: float
    exchange: str | None = None
    conditions: list[str] | None = None
    aggressor: str = "UNKNOWN"
    aggressor_confidence: float = 0.0
    source: str = "TBT_ALL_LAST"
    quality: str = "TBT_TRADES_MKTDATA_QUOTES"


class SymbolFlowState:
    def __init__(self, symbol: str, *, history_sec: int = 1800):
        self.symbol = symbol
        self.history_ns = history_sec * 1_000_000_000
        self.quotes: Deque[QuoteEvent] = deque()
        self.trades: Deque[TradeEvent] = deque()
        self.cvd = 0.0
        self.last_trade_price: float | None = None
        self.last_quote: QuoteEvent | None = None
        self.last_cum_volume: float | None = None
        self.activity_history: Deque[tuple[int, float]] = deque()
        self.volume_rate_history: Deque[tuple[int, float]] = deque()

    def _trim(self, now_ns: int) -> None:
        cutoff = now_ns - self.history_ns
        while self.quotes and self.quotes[0].ts_ns < cutoff:
            self.quotes.popleft()
        while self.trades and self.trades[0].ts_ns < cutoff:
            self.trades.popleft()
        while self.activity_history and self.activity_history[0][0] < cutoff:
            self.activity_history.popleft()
        while self.volume_rate_history and self.volume_rate_history[0][0] < cutoff:
            self.volume_rate_history.popleft()

    def add_quote(self, quote: QuoteEvent) -> TradeEvent | None:
        """Add quote/snapshot and optionally reconstruct one proxy trade.

        In RADAR mode this is intentionally a *proxy*. A cumulative-volume
        increase can represent multiple prints between IBKR snapshots, so it
        must never be presented as true Time & Sales.
        """
        proxy_trade: TradeEvent | None = None
        quote.volume = _finite(quote.volume)
        if (
            quote.quality == "MKTDATA_SNAPSHOT_PROXY"
            and quote.volume is not None
            and self.last_cum_volume is not None
            and quote.volume > self.last_cum_volume
            and quote.last is not None
        ):
            size = quote.volume - self.last_cum_volume
            side, conf = self.classify_aggressor(float(quote.last), quote)
            proxy_trade = TradeEvent(
                ts_ns=quote.ts_ns,
                price=float(quote.last),
                size=float(size),
                aggressor=side,
                aggressor_confidence=min(conf, 0.55),
                source="REQ_MKT_DATA_VOLUME_DELTA",
                quality="MKTDATA_SNAPSHOT_PROXY",
            )
            self.add_trade(proxy_trade)
        if quote.volume is not None:
            self.last_cum_volume = quote.volume
        if quote.trade_rate is not None and math.isfinite(float(quote.trade_rate)):
            self.activity_history.append((quote.ts_ns, float(quote.trade_rate)))
        if quote.volume_rate is not None and math.isfinite(float(quote.volume_rate)):
            self.volume_rate_history.append((quote.ts_ns, float(quote.volume_rate)))
        self.quotes.append(quote)
        self.last_quote = quote
        self._trim(quote.ts_ns)
        return proxy_trade

    def classify_aggressor(self, price: float, quote: QuoteEvent | None = None) -> tuple[str, float]:
        q = quote or self.last_quote
        if q:
            bid = _finite(q.bid)
            ask = _finite(q.ask)
            if bid is not None and ask is not None and ask >= bid:
                eps = max(1e-9, (ask - bid) * 0.05)
                if price >= ask - eps:
                    return "BUY", 0.95
                if price <= bid + eps:
                    return "SELL", 0.95
                mid = (bid + ask) / 2
                if price > mid:
                    return "BUY", 0.70
                if price < mid:
                    return "SELL", 0.70
        if self.last_trade_price is not None:
            if price > self.last_trade_price:
                return "BUY", 0.45
            if price < self.last_trade_price:
                return "SELL", 0.45
        return "UNKNOWN", 0.0

    def add_trade(self, trade: TradeEvent) -> None:
        if trade.aggressor == "UNKNOWN":
            trade.aggressor, trade.aggressor_confidence = self.classify_aggressor(trade.price)
        if trade.aggressor == "BUY":
            self.cvd += trade.size
        elif trade.aggressor == "SELL":
            self.cvd -= trade.size
        self.last_trade_price = trade.price
        self.trades.append(trade)
        self._trim(trade.ts_ns)

    def _window_trades(self, now_ns: int, sec: int) -> list[TradeEvent]:
        cutoff = now_ns - sec * 1_000_000_000
        return [t for t in self.trades if t.ts_ns >= cutoff]

    @staticmethod
    def _rate(events: list[TradeEvent], side: str, sec: float) -> float:
        return sum(t.size for t in events if t.aggressor == side) / max(sec, 1e-6)

    def _activity_score(self, now_ns: int, fallback_tps: float) -> float:
        current = None
        if self.activity_history:
            current = self.activity_history[-1][1]
            samples = [x[1] for x in self.activity_history if x[1] >= 0]
        else:
            samples = []
        if current is None:
            current = fallback_tps
        if len(samples) >= 10:
            med = statistics.median(samples)
            mad = statistics.median(abs(x - med) for x in samples) or max(abs(med) * 0.1, 1e-6)
            z = (current - med) / (1.4826 * mad)
            return _clamp(50 + z * 12.5)
        return _clamp(35 + math.log1p(max(current, 0)) * 18)

    def metrics(self, *, now_ns: int | None = None, window_sec: int = 15, quality: str) -> dict[str, Any]:
        now_ns = now_ns or time.time_ns()
        ts = self._window_trades(now_ns, window_sec)
        prior_cut = now_ns - window_sec * 1_000_000_000
        recent5_cut = now_ns - 5 * 1_000_000_000
        prior15_cut = now_ns - 20 * 1_000_000_000
        recent5 = [t for t in self.trades if t.ts_ns >= recent5_cut]
        previous15 = [t for t in self.trades if prior15_cut <= t.ts_ns < recent5_cut]

        buy = sum(t.size for t in ts if t.aggressor == "BUY")
        sell = sum(t.size for t in ts if t.aggressor == "SELL")
        total = buy + sell
        delta = buy - sell
        tps = len(ts) / max(window_sec, 1)
        vps = total / max(window_sec, 1)

        quote = self.last_quote
        bid = _finite(quote.bid) if quote else None
        ask = _finite(quote.ask) if quote else None
        bs = _finite(quote.bid_size) if quote else None
        a_s = _finite(quote.ask_size) if quote else None
        last = self.last_trade_price or (_finite(quote.last) if quote else None)

        spread_bps = None
        if bid is not None and ask is not None and bid > 0 and ask >= bid:
            mid = (bid + ask) / 2
            spread_bps = (ask - bid) / mid * 10_000 if mid else None

        quote_imbalance = None
        if bs is not None and a_s is not None and bs + a_s > 0:
            quote_imbalance = (bs - a_s) / (bs + a_s)

        start_price = ts[0].price if ts else last
        move_bps = 0.0
        if start_price and last and start_price > 0:
            move_bps = (last - start_price) / start_price * 10_000

        buy_share = buy / total if total else 0.0
        sell_share = sell / total if total else 0.0
        bid_absorption = 0.0
        offer_absorption = 0.0
        if total > 0:
            # High sell pressure + little/no downside progress => bid absorption.
            bid_absorption = _clamp(100 * sell_share * math.exp(-max(0.0, -move_bps) / 4.0))
            offer_absorption = _clamp(100 * buy_share * math.exp(-max(0.0, move_bps) / 4.0))

        recent_sell_rate = self._rate(recent5, "SELL", 5)
        recent_buy_rate = self._rate(recent5, "BUY", 5)
        prior_sell_rate = self._rate(previous15, "SELL", 15)
        prior_buy_rate = self._rate(previous15, "BUY", 15)
        seller_exhaustion = 0.0
        buyer_exhaustion = 0.0
        if prior_sell_rate > 0:
            seller_exhaustion = _clamp(100 * max(0.0, 1 - recent_sell_rate / prior_sell_rate))
        if prior_buy_rate > 0:
            buyer_exhaustion = _clamp(100 * max(0.0, 1 - recent_buy_rate / prior_buy_rate))

        buy_impact = max(0.0, move_bps) / max(buy / 100_000, 1.0) if buy else 0.0
        sell_impact = max(0.0, -move_bps) / max(sell / 100_000, 1.0) if sell else 0.0

        large_score = 0.0
        recent_sizes = [t.size for t in self.trades if t.ts_ns >= now_ns - 300 * 1_000_000_000]
        if ts and len(recent_sizes) >= 5:
            med = statistics.median(recent_sizes)
            mad = statistics.median(abs(x - med) for x in recent_sizes) or max(med * 0.1, 1.0)
            latest = ts[-1]
            z = (latest.size - med) / (1.4826 * mad)
            sign = 1 if latest.aggressor == "BUY" else (-1 if latest.aggressor == "SELL" else 0)
            large_score = max(-100.0, min(100.0, sign * max(0.0, z) * 20.0))

        activity = self._activity_score(now_ns, tps)
        known = [t.aggressor_confidence for t in ts if t.aggressor != "UNKNOWN"]
        side_conf = statistics.mean(known) if known else 0.0
        source_multiplier = 1.0 if quality.startswith("TBT_") else 0.62
        confidence = _clamp(100 * source_multiplier * (0.45 + 0.55 * side_conf))

        flow_score = _clamp(50 + (buy_share - sell_share) * 50) if total else 50.0
        absorption_score = max(bid_absorption, offer_absorption)
        overall = _clamp(0.35 * activity + 0.25 * abs(flow_score - 50) * 2 + 0.40 * absorption_score)

        return {
            "ts_ns": now_ns,
            "window_sec": window_sec,
            "last_price": last,
            "spread_bps": spread_bps,
            "buy_volume": buy,
            "sell_volume": sell,
            "delta": delta,
            "cvd": self.cvd,
            "trades_per_sec": tps,
            "volume_per_sec": vps,
            "quote_imbalance": quote_imbalance,
            "bid_absorption": bid_absorption,
            "offer_absorption": offer_absorption,
            "seller_exhaustion": seller_exhaustion,
            "buyer_exhaustion": buyer_exhaustion,
            "buy_price_impact_bps": buy_impact,
            "sell_price_impact_bps": sell_impact,
            "large_trade_score": large_score,
            "activity_score": activity,
            "flow_score": flow_score,
            "absorption_score": absorption_score,
            "score": overall,
            "confidence": confidence,
            "quality": quality,
            "move_bps": move_bps,
        }

    def footprint(self, *, now_ns: int | None = None, sec: int = 300, tick_size: float = 0.01) -> list[dict[str, Any]]:
        now_ns = now_ns or time.time_ns()
        cutoff = now_ns - sec * 1_000_000_000
        buckets: dict[float, dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "unknown": 0.0, "trades": 0.0})
        for t in self.trades:
            if t.ts_ns < cutoff:
                continue
            p = round(t.price / tick_size) * tick_size
            b = buckets[p]
            b["trades"] += 1
            if t.aggressor == "BUY":
                b["buy"] += t.size
            elif t.aggressor == "SELL":
                b["sell"] += t.size
            else:
                b["unknown"] += t.size
        out = []
        for price in sorted(buckets, reverse=True):
            b = buckets[price]
            out.append(
                {
                    "price": price,
                    "buy": b["buy"],
                    "sell": b["sell"],
                    "unknown": b["unknown"],
                    "delta": b["buy"] - b["sell"],
                    "volume": b["buy"] + b["sell"] + b["unknown"],
                    "trades": int(b["trades"]),
                }
            )
        return out


class OrderFlowEngine:
    def __init__(self):
        self.states: dict[str, SymbolFlowState] = {}

    def state(self, symbol: str) -> SymbolFlowState:
        symbol = symbol.upper()
        if symbol not in self.states:
            self.states[symbol] = SymbolFlowState(symbol)
        return self.states[symbol]

    @staticmethod
    def signals_from_metrics(symbol: str, m: dict[str, Any]) -> list[dict[str, Any]]:
        signals: list[dict[str, Any]] = []
        candidates = [
            ("BID_ABSORPTION", "BULLISH", m["bid_absorption"], "Heavy sell flow with limited downside progress"),
            ("OFFER_ABSORPTION", "BEARISH", m["offer_absorption"], "Heavy buy flow with limited upside progress"),
            ("SELLER_EXHAUSTION", "BULLISH", m["seller_exhaustion"], "Sell flow rate has decayed versus the prior window"),
            ("BUYER_EXHAUSTION", "BEARISH", m["buyer_exhaustion"], "Buy flow rate has decayed versus the prior window"),
            ("ACTIVITY_BURST", "NEUTRAL", m["activity_score"], "Trade/activity rate is elevated versus local baseline"),
        ]
        for signal_type, direction, score, explanation in candidates:
            if score >= 70:
                signals.append(
                    {
                        "symbol": symbol,
                        "ts_ns": m["ts_ns"],
                        "signal_type": signal_type,
                        "direction": direction,
                        "score": score,
                        "price": m["last_price"],
                        "quality": m["quality"],
                        "explanation": explanation,
                        "evidence_json": json.dumps(
                            {
                                "delta": m["delta"],
                                "cvd": m["cvd"],
                                "move_bps": m["move_bps"],
                                "buy_volume": m["buy_volume"],
                                "sell_volume": m["sell_volume"],
                                "confidence": m["confidence"],
                            },
                            separators=(",", ":"),
                        ),
                    }
                )
        return signals

from __future__ import annotations

from pathlib import Path

from .alpaca_adapter import AlpacaAdapter, AlpacaFeed
from .runtime import OrderFlowRuntime


class AlpacaOrderFlowRuntime(OrderFlowRuntime):
    """LIVE runtime backed by Alpaca instead of TWS/IB Gateway.

    OrderFlowRuntime owns the common engine, persistence and websocket event
    contract. This subclass only swaps the transport adapter, so IBKR LIVE,
    Alpaca LIVE and SQLite REPLAY all converge on the same QuoteEvent /
    TradeEvent processing path.
    """

    def __init__(
        self,
        *,
        symbols: list[str],
        db_path: str | Path,
        feed: AlpacaFeed = "iex",
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        # Initialize the common runtime. The temporary IBKR adapter is never
        # connected; it is immediately replaced by AlpacaAdapter below.
        super().__init__(
            symbols=symbols,
            mode="focus",
            db_path=db_path,
            ib_host="stream.data.alpaca.markets",
            ib_port=443,
            client_id=0,
            # The common runtime constructs a temporary IBKR plan before the
            # Alpaca adapter replaces it. Give that plan the normal 100-line
            # budget so Alpaca can start with the five-symbol replay universe.
            market_data_lines=100,
        )
        self.provider = "alpaca"
        self.alpaca_feed = feed
        self.adapter = AlpacaAdapter(
            symbols=self.symbols,
            feed=feed,
            api_key=api_key,
            api_secret=api_secret,
            on_quote=self._on_quote,
            on_trade=self._on_trade,
            on_status=self._on_status,
        )

    async def start(self) -> None:
        await super().start()
        self.status.update({"provider": "alpaca", "feed": self.alpaca_feed})
        self._publish({"type": "status", "data": self.status})

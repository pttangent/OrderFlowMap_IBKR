from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import aiohttp

from .alpaca_history import DownloadStats, _bounds, _iso_z, record_day
from .replay_runtime import ReplayRuntime

# Alpaca Basic currently allows 30 live equity WebSocket symbols and 200
# historical requests/minute. Historical REST does not publish a 30-symbol
# ceiling, but keeping the interactive replay picker inside the free real-time
# envelope gives the workstation one simple, conservative product limit.
REPLAY_SYMBOL_CAP = 30
FREE_HISTORICAL_RPM = 200
REPLAY_DOWNLOAD_RPM = 180
LATEST_DATA_DELAY_MIN = 16

ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass(slots=True)
class PreparedReplay:
    runtime: ReplayRuntime
    stats: DownloadStats
    trading_day: date
    db_path: Path


def clean_replay_symbols(values: list[str]) -> list[str]:
    out: list[str] = []
    for raw in values:
        for token in str(raw).replace(",", " ").split():
            symbol = token.strip().upper()
            if symbol and symbol not in out:
                out.append(symbol)
    if not out:
        raise ValueError("At least one replay symbol is required")
    if len(out) > REPLAY_SYMBOL_CAP:
        raise ValueError(
            f"Replay supports at most {REPLAY_SYMBOL_CAP} symbols in the free-tier UI; "
            f"received {len(out)}."
        )
    return out


def _initial_candidate(now_utc: datetime | None = None) -> date:
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    eastern = ZoneInfo("America/New_York")
    now_et = now_utc.astimezone(eastern)
    # The free SIP historical feed excludes the latest 15 minutes. A session is
    # usable as a complete replay only once regular close is safely older than it.
    if now_et.time() >= dtime(16, LATEST_DATA_DELAY_MIN):
        return now_et.date()
    return now_et.date() - timedelta(days=1)


def _credentials(api_key: str | None, api_secret: str | None) -> tuple[str, str]:
    key = api_key or os.getenv("APCA_API_KEY_ID")
    secret = api_secret or os.getenv("APCA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError(
            "Alpaca credentials are required for replay download. Set APCA_API_KEY_ID / "
            "APCA_API_SECRET_KEY or enter them in the local Replay dialog."
        )
    return key, secret


async def latest_completed_trading_day(
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    now_utc: datetime | None = None,
    max_lookback_days: int = 10,
) -> date:
    """Find the latest fully downloadable US session using free SIP data.

    We deliberately probe SPY through the same historical market-data API used
    for replay rather than relying on a separate trading-account calendar
    entitlement. Weekends/holidays naturally return no prints and are skipped.
    """

    key, secret = _credentials(api_key, api_secret)
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    candidate = _initial_candidate(now_utc)
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)

    async with aiohttp.ClientSession(timeout=timeout) as client:
        for _ in range(max_lookback_days):
            if candidate.weekday() < 5:
                start_dt, end_dt = _bounds(candidate, "regular")
                params = {
                    "start": _iso_z(start_dt),
                    "end": _iso_z(end_dt),
                    "feed": "sip",
                    "limit": 1,
                    "sort": "asc",
                }
                backoff = 0.5
                for attempt in range(5):
                    async with client.get(
                        "https://data.alpaca.markets/v2/stocks/SPY/trades",
                        params=params,
                        headers=headers,
                    ) as response:
                        body = await response.text()
                        if response.status == 429 and attempt < 4:
                            retry_after = response.headers.get("Retry-After")
                            await asyncio.sleep(float(retry_after) if retry_after else backoff)
                            backoff = min(backoff * 2, 8.0)
                            continue
                        if response.status in (401, 403):
                            raise RuntimeError(
                                f"Alpaca historical authorization failed ({response.status}). "
                                "Check the API key/secret and SIP historical access."
                            )
                        if response.status >= 400:
                            raise RuntimeError(
                                f"Alpaca trading-day probe failed HTTP {response.status}: {body[:500]}"
                            )
                        try:
                            payload = await response.json()
                        except Exception as exc:
                            raise RuntimeError("Invalid Alpaca trading-day probe response") from exc
                        if payload.get("trades"):
                            return candidate
                        break
            candidate -= timedelta(days=1)

    raise RuntimeError(
        f"Could not locate a completed US trading session in the last {max_lookback_days} days"
    )


async def build_previous_session_replay(
    *,
    symbols: list[str],
    db_path: str | Path,
    api_key: str | None = None,
    api_secret: str | None = None,
    speed: float = 1.0,
    on_progress: ProgressCallback | None = None,
) -> PreparedReplay:
    symbols = clean_replay_symbols(symbols)
    key, secret = _credentials(api_key, api_secret)
    notify = on_progress or (lambda _payload: None)

    notify({"stage": "resolving_day", "symbols": symbols})
    trading_day = await latest_completed_trading_day(api_key=key, api_secret=secret)
    notify(
        {
            "stage": "downloading",
            "symbols": symbols,
            "trading_day": trading_day.isoformat(),
            "feed": "sip",
            "requests_per_minute": REPLAY_DOWNLOAD_RPM,
        }
    )

    path = Path(db_path)
    stats = await record_day(
        symbols=symbols,
        day=trading_day,
        db_path=path,
        feed="sip",
        session="regular",
        api_key=key,
        api_secret=secret,
        requests_per_minute=REPLAY_DOWNLOAD_RPM,
    )

    notify(
        {
            "stage": "indexing",
            "symbols": symbols,
            "trading_day": trading_day.isoformat(),
            "quotes": stats.quotes,
            "trades": stats.trades,
            "requests": stats.requests,
        }
    )
    runtime = ReplayRuntime(
        symbols=symbols,
        db_path=path,
        source_session=stats.session_id,
        duration_sec=24 * 60 * 60,
        speed=speed,
    )
    await runtime.start()
    return PreparedReplay(runtime=runtime, stats=stats, trading_day=trading_day, db_path=path)

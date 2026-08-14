from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import aiohttp

from .alpaca_history import DownloadStats, _bounds, _iso_z, record_day
from .replay_runtime import ReplayRuntime

# Replay is intentionally kept small even though Historical REST itself does
# not publish a five-symbol ceiling. Five symbols keeps full-day tick replay
# practical and aligns with the workstation's deep-focus use case.
REPLAY_SYMBOL_CAP = 5
FREE_HISTORICAL_RPM = 200
REPLAY_DOWNLOAD_RPM = 180
LATEST_DATA_DELAY_MIN = 16
CACHE_META_PREFIX = "replay_symbol_complete:"

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
            f"Replay supports at most {REPLAY_SYMBOL_CAP} symbols; received {len(out)}."
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


def _cache_session_id(trading_day: date) -> str:
    return f"alpaca-sip-{trading_day.isoformat()}-regular-cache"


def _cache_marker_key(session_id: str, symbol: str) -> str:
    return f"{CACHE_META_PREFIX}{session_id}:{symbol.upper()}"


def _open_existing(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _cached_symbol_counts(
    path: Path,
    *,
    session_id: str,
    symbol: str,
) -> tuple[int, int] | None:
    conn = _open_existing(path)
    if conn is None:
        return None
    try:
        marker = conn.execute(
            "SELECT value FROM schema_meta WHERE key=?",
            (_cache_marker_key(session_id, symbol),),
        ).fetchone()
        if marker is None:
            return None
        try:
            meta = json.loads(str(marker["value"]))
        except Exception:
            return None
        if not bool(meta.get("complete")):
            return None
        q = int(
            conn.execute(
                "SELECT COUNT(*) FROM raw_quotes WHERE session_id=? AND symbol=?",
                (session_id, symbol),
            ).fetchone()[0]
        )
        t = int(
            conn.execute(
                "SELECT COUNT(*) FROM raw_trades WHERE session_id=? AND symbol=?",
                (session_id, symbol),
            ).fetchone()[0]
        )
        if q <= 0 or t <= 0:
            return None
        return q, t
    finally:
        conn.close()


def _ensure_cache_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    trading_day: date,
    symbol: str,
) -> None:
    start_dt, end_dt = _bounds(trading_day, "regular")
    start_ns = int(start_dt.timestamp() * 1_000_000_000)
    end_ns = int(end_dt.timestamp() * 1_000_000_000)
    row = conn.execute(
        "SELECT symbols_json FROM sessions WHERE session_id=?",
        (session_id,),
    ).fetchone()
    existing: list[str] = []
    if row is not None:
        try:
            existing = [str(x).upper() for x in json.loads(str(row[0]))]
        except Exception:
            existing = []
    merged = list(dict.fromkeys([*existing, symbol.upper()]))
    notes = json.dumps(
        {
            "provider": "alpaca",
            "historical": True,
            "feed": "sip",
            "session": "regular",
            "trading_day": trading_day.isoformat(),
            "replay_cache": True,
            "download_strategy": "sequential_per_symbol",
            "quote_size_unit": "shares",
        },
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO sessions(
          session_id,started_ns,ended_ns,mode,symbols_json,ib_host,ib_port,
          client_id,market_data_lines,notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(session_id) DO UPDATE SET
          started_ns=excluded.started_ns,
          ended_ns=excluded.ended_ns,
          mode=excluded.mode,
          symbols_json=excluded.symbols_json,
          ib_host=excluded.ib_host,
          ib_port=excluded.ib_port,
          client_id=excluded.client_id,
          market_data_lines=excluded.market_data_lines,
          notes=excluded.notes
        """,
        (
            session_id,
            start_ns,
            end_ns,
            "ALPACA_HISTORICAL_SIP_CACHE",
            json.dumps(merged),
            "data.alpaca.markets",
            443,
            0,
            len(merged),
            notes,
        ),
    )


def _merge_temp_symbol_into_cache(
    path: Path,
    *,
    temp_session_id: str,
    cache_session_id: str,
    trading_day: date,
    symbol: str,
) -> tuple[int, int]:
    symbol = symbol.upper()
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        with conn:
            _ensure_cache_session(
                conn,
                session_id=cache_session_id,
                trading_day=trading_day,
                symbol=symbol,
            )
            conn.execute(
                "DELETE FROM raw_quotes WHERE session_id=? AND symbol=?",
                (cache_session_id, symbol),
            )
            conn.execute(
                "DELETE FROM raw_trades WHERE session_id=? AND symbol=?",
                (cache_session_id, symbol),
            )
            conn.execute(
                "DELETE FROM subscriptions WHERE session_id=? AND symbol=?",
                (cache_session_id, symbol),
            )
            conn.execute(
                """
                INSERT INTO raw_quotes(
                  session_id,symbol,ts_ns,bid,ask,bid_size,ask_size,last,last_size,
                  volume,vwap,trade_count,trade_rate,volume_rate,source,quality
                )
                SELECT ?,symbol,ts_ns,bid,ask,bid_size,ask_size,last,last_size,
                       volume,vwap,trade_count,trade_rate,volume_rate,source,quality
                FROM raw_quotes
                WHERE session_id=? AND symbol=?
                ORDER BY id
                """,
                (cache_session_id, temp_session_id, symbol),
            )
            conn.execute(
                """
                INSERT INTO raw_trades(
                  session_id,symbol,ts_ns,price,size,exchange,conditions_json,
                  aggressor,aggressor_confidence,source,quality
                )
                SELECT ?,symbol,ts_ns,price,size,exchange,conditions_json,
                       aggressor,aggressor_confidence,source,quality
                FROM raw_trades
                WHERE session_id=? AND symbol=?
                ORDER BY id
                """,
                (cache_session_id, temp_session_id, symbol),
            )
            q = int(
                conn.execute(
                    "SELECT COUNT(*) FROM raw_quotes WHERE session_id=? AND symbol=?",
                    (cache_session_id, symbol),
                ).fetchone()[0]
            )
            t = int(
                conn.execute(
                    "SELECT COUNT(*) FROM raw_trades WHERE session_id=? AND symbol=?",
                    (cache_session_id, symbol),
                ).fetchone()[0]
            )
            if q <= 0 or t <= 0:
                raise RuntimeError(
                    f"Cannot cache incomplete Alpaca replay data for {symbol}: quotes={q}, trades={t}"
                )
            start_dt, end_dt = _bounds(trading_day, "regular")
            conn.execute(
                """
                INSERT OR REPLACE INTO subscriptions(
                  session_id,symbol,mode,trade_source,quote_source,quality,started_ns,ended_ns
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    cache_session_id,
                    symbol,
                    "alpaca-historical-sip-cache",
                    "ALPACA_HIST_SIP_TRADE",
                    "ALPACA_HIST_SIP_QUOTE",
                    "ALPACA_SIP_TRADES_QUOTES",
                    int(start_dt.timestamp() * 1_000_000_000),
                    int(end_dt.timestamp() * 1_000_000_000),
                ),
            )
            marker = json.dumps(
                {
                    "complete": True,
                    "trading_day": trading_day.isoformat(),
                    "feed": "sip",
                    "session": "regular",
                    "symbol": symbol,
                    "quotes": q,
                    "trades": t,
                },
                separators=(",", ":"),
            )
            conn.execute(
                "INSERT OR REPLACE INTO schema_meta(key,value) VALUES(?,?)",
                (_cache_marker_key(cache_session_id, symbol), marker),
            )

            # The temporary recording is only a download staging area. Once the
            # full symbol is atomically copied and marked complete, remove it so
            # we do not keep a second copy of the same tick tape.
            conn.execute("DELETE FROM raw_quotes WHERE session_id=?", (temp_session_id,))
            conn.execute("DELETE FROM raw_trades WHERE session_id=?", (temp_session_id,))
            conn.execute("DELETE FROM subscriptions WHERE session_id=?", (temp_session_id,))
            conn.execute("DELETE FROM sessions WHERE session_id=?", (temp_session_id,))
            return q, t
    finally:
        conn.close()


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
    path = Path(db_path)
    cache_session = _cache_session_id(trading_day)
    total_requests = 0
    total_retries = 0
    cached_symbols: list[str] = []
    downloaded_symbols: list[str] = []

    # Intentionally sequential. A symbol is either already fully cached or is
    # downloaded completely (quotes then trades), atomically promoted into the
    # canonical day cache, and only then do we move to the next symbol.
    for index, symbol in enumerate(symbols, 1):
        cached = _cached_symbol_counts(
            path,
            session_id=cache_session,
            symbol=symbol,
        )
        if cached is not None:
            cached_symbols.append(symbol)
            notify(
                {
                    "stage": "downloading",
                    "symbols": [symbol],
                    "all_symbols": symbols,
                    "current_symbol": symbol,
                    "symbol_index": index,
                    "symbol_total": len(symbols),
                    "trading_day": trading_day.isoformat(),
                    "feed": "sip",
                    "cache_hit": True,
                    "cached_symbols": list(cached_symbols),
                    "downloaded_symbols": list(downloaded_symbols),
                }
            )
            continue

        notify(
            {
                "stage": "downloading",
                "symbols": [symbol],
                "all_symbols": symbols,
                "current_symbol": symbol,
                "symbol_index": index,
                "symbol_total": len(symbols),
                "trading_day": trading_day.isoformat(),
                "feed": "sip",
                "requests_per_minute": REPLAY_DOWNLOAD_RPM,
                "cache_hit": False,
                "cached_symbols": list(cached_symbols),
                "downloaded_symbols": list(downloaded_symbols),
            }
        )
        single = await record_day(
            symbols=[symbol],
            day=trading_day,
            db_path=path,
            feed="sip",
            session="regular",
            api_key=key,
            api_secret=secret,
            requests_per_minute=REPLAY_DOWNLOAD_RPM,
        )
        total_requests += single.requests
        total_retries += single.retries
        _merge_temp_symbol_into_cache(
            path,
            temp_session_id=single.session_id,
            cache_session_id=cache_session,
            trading_day=trading_day,
            symbol=symbol,
        )
        downloaded_symbols.append(symbol)

        # record_day has its own request pacer. Keep a small inter-symbol gap so
        # restarting that pacer cannot create a boundary burst near 200 RPM.
        if index < len(symbols):
            await asyncio.sleep(60.0 / REPLAY_DOWNLOAD_RPM)

    quote_total = 0
    trade_total = 0
    for symbol in symbols:
        counts = _cached_symbol_counts(
            path,
            session_id=cache_session,
            symbol=symbol,
        )
        if counts is None:
            raise RuntimeError(f"Replay cache verification failed for {symbol}")
        q, t = counts
        quote_total += q
        trade_total += t

    start_dt, end_dt = _bounds(trading_day, "regular")
    stats = DownloadStats(
        session_id=cache_session,
        symbols=symbols,
        feed="sip",
        start=_iso_z(start_dt),
        end=_iso_z(end_dt),
        quotes=quote_total,
        trades=trade_total,
        requests=total_requests,
        retries=total_retries,
    )
    notify(
        {
            "stage": "indexing",
            "symbols": symbols,
            "trading_day": trading_day.isoformat(),
            "quotes": stats.quotes,
            "trades": stats.trades,
            "requests": stats.requests,
            "cached_symbols": cached_symbols,
            "downloaded_symbols": downloaded_symbols,
            "cache_hit": not downloaded_symbols,
            "download_strategy": "sequential_per_symbol",
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

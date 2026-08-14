from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Literal
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import aiohttp

from .alpaca_adapter import _rfc3339_to_ns
from .storage import DDL

HistoricalFeed = Literal["sip", "iex"]
SessionKind = Literal["regular", "extended"]


@dataclass(slots=True)
class DownloadStats:
    session_id: str
    symbols: list[str]
    feed: str
    start: str
    end: str
    quotes: int = 0
    trades: int = 0
    requests: int = 0
    retries: int = 0


class RequestPacer:
    def __init__(self, requests_per_minute: int) -> None:
        rpm = max(1, int(requests_per_minute))
        self.interval = 60.0 / rpm
        self._last = 0.0

    async def wait(self) -> None:
        now = time.monotonic()
        delay = self.interval - (now - self._last)
        if delay > 0:
            await asyncio.sleep(delay)
        self._last = time.monotonic()


def _quality(feed: str) -> str:
    return "ALPACA_SIP_TRADES_QUOTES" if feed == "sip" else "ALPACA_IEX_TRADES_QUOTES"


def _bounds(day: date, session: SessionKind) -> tuple[datetime, datetime]:
    eastern = ZoneInfo("America/New_York")
    if session == "regular":
        start_local = datetime.combine(day, dtime(9, 30), tzinfo=eastern)
        end_local = datetime.combine(day, dtime(16, 0), tzinfo=eastern)
    else:
        start_local = datetime.combine(day, dtime(4, 0), tzinfo=eastern)
        end_local = datetime.combine(day, dtime(20, 0), tzinfo=eastern)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_symbols(symbols: list[str]) -> list[str]:
    out: list[str] = []
    for symbol in symbols:
        value = symbol.strip().upper()
        if value and value not in out:
            out.append(value)
    if not out:
        raise ValueError("At least one symbol is required")
    return out


async def _pages(
    client: aiohttp.ClientSession,
    *,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    pacer: RequestPacer,
    stats: DownloadStats,
    max_retries: int = 8,
) -> AsyncIterator[dict[str, Any]]:
    page_token: str | None = None
    backoff = 1.0
    while True:
        query = dict(params)
        if page_token:
            query["page_token"] = page_token

        attempt = 0
        while True:
            await pacer.wait()
            stats.requests += 1
            try:
                async with client.get(url, params=query, headers=headers) as response:
                    if response.status == 429:
                        attempt += 1
                        stats.retries += 1
                        if attempt > max_retries:
                            body = await response.text()
                            raise RuntimeError(f"Alpaca rate limit persisted: {body[:500]}")
                        retry_after = response.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after else backoff
                        await asyncio.sleep(max(0.25, delay))
                        backoff = min(backoff * 2.0, 30.0)
                        continue
                    body = await response.text()
                    if response.status >= 400:
                        raise RuntimeError(
                            f"Alpaca historical HTTP {response.status}: {body[:1000]} "
                            f"URL={url}?{urlencode(query)}"
                        )
                    payload = json.loads(body)
                    if not isinstance(payload, dict):
                        raise RuntimeError("Unexpected Alpaca historical payload")
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                attempt += 1
                stats.retries += 1
                if attempt > max_retries:
                    raise RuntimeError(f"Alpaca request failed after retries: {exc}") from exc
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

        yield payload
        page_token = payload.get("next_page_token")
        if not page_token:
            return
        backoff = 1.0


def _open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(DDL)
    return conn


def _begin_recording(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    symbols: list[str],
    feed: str,
    start_ns: int,
    end_ns: int,
    start_text: str,
    end_text: str,
) -> None:
    quality = _quality(feed)
    notes = json.dumps(
        {
            "provider": "alpaca",
            "historical": True,
            "feed": feed,
            "start": start_text,
            "end": end_text,
            "replay_base": True,
        },
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO sessions(
          session_id,started_ns,ended_ns,mode,symbols_json,ib_host,ib_port,
          client_id,market_data_lines,notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            session_id,
            start_ns,
            end_ns,
            f"ALPACA_HISTORICAL_{feed.upper()}",
            json.dumps(symbols),
            "data.alpaca.markets",
            443,
            0,
            len(symbols),
            notes,
        ),
    )
    for symbol in symbols:
        conn.execute(
            """
            INSERT OR REPLACE INTO subscriptions(
              session_id,symbol,mode,trade_source,quote_source,quality,started_ns,ended_ns
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                session_id,
                symbol,
                f"alpaca-historical-{feed}",
                f"ALPACA_HIST_{feed.upper()}_TRADE",
                f"ALPACA_HIST_{feed.upper()}_QUOTE",
                quality,
                start_ns,
                end_ns,
            ),
        )
    conn.commit()


def _insert_quotes(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    symbol: str,
    feed: str,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    quality = _quality(feed)
    payload = [
        (
            session_id,
            symbol,
            _rfc3339_to_ns(row["t"]),
            row.get("bp"),
            row.get("ap"),
            row.get("bs"),
            row.get("as"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            f"ALPACA_HIST_{feed.upper()}_QUOTE",
            quality,
        )
        for row in rows
    ]
    conn.executemany(
        """
        INSERT INTO raw_quotes(
          session_id,symbol,ts_ns,bid,ask,bid_size,ask_size,last,last_size,
          volume,vwap,trade_count,trade_rate,volume_rate,source,quality
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        payload,
    )
    conn.commit()
    return len(payload)


def _insert_trades(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    symbol: str,
    feed: str,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    quality = _quality(feed)
    payload = [
        (
            session_id,
            symbol,
            _rfc3339_to_ns(row["t"]),
            float(row["p"]),
            float(row["s"]),
            row.get("x"),
            json.dumps(row.get("c") or [], separators=(",", ":")),
            "UNKNOWN",
            0.0,
            f"ALPACA_HIST_{feed.upper()}_TRADE",
            quality,
        )
        for row in rows
    ]
    conn.executemany(
        """
        INSERT INTO raw_trades(
          session_id,symbol,ts_ns,price,size,exchange,conditions_json,
          aggressor,aggressor_confidence,source,quality
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        payload,
    )
    conn.commit()
    return len(payload)


async def record_day(
    *,
    symbols: list[str],
    day: date,
    db_path: str | Path,
    feed: HistoricalFeed = "sip",
    session: SessionKind = "regular",
    api_key: str | None = None,
    api_secret: str | None = None,
    requests_per_minute: int = 180,
    session_id: str | None = None,
) -> DownloadStats:
    symbols = _clean_symbols(symbols)
    key = api_key or os.getenv("APCA_API_KEY_ID")
    secret = api_secret or os.getenv("APCA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError(
            "Alpaca credentials are required. Set APCA_API_KEY_ID and "
            "APCA_API_SECRET_KEY or pass explicit credentials."
        )
    if feed not in {"sip", "iex"}:
        raise ValueError(f"Unsupported historical stock feed: {feed}")

    start_dt, end_dt = _bounds(day, session)
    start_text, end_text = _iso_z(start_dt), _iso_z(end_dt)
    start_ns = int(start_dt.timestamp() * 1_000_000_000)
    end_ns = int(end_dt.timestamp() * 1_000_000_000)
    session_id = session_id or f"alpaca-{feed}-{day.isoformat()}-{uuid.uuid4().hex[:8]}"
    stats = DownloadStats(
        session_id=session_id,
        symbols=symbols,
        feed=feed,
        start=start_text,
        end=end_text,
    )

    db = Path(db_path)
    conn = _open_db(db)
    _begin_recording(
        conn,
        session_id=session_id,
        symbols=symbols,
        feed=feed,
        start_ns=start_ns,
        end_ns=end_ns,
        start_text=start_text,
        end_text=end_text,
    )

    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    params = {
        "start": start_text,
        "end": end_text,
        "feed": feed,
        "limit": 10000,
        "sort": "asc",
    }
    pacer = RequestPacer(requests_per_minute)
    timeout = aiohttp.ClientTimeout(total=90, connect=15, sock_read=75)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            for symbol in symbols:
                quote_url = f"https://data.alpaca.markets/v2/stocks/{symbol}/quotes"
                async for page in _pages(
                    client,
                    url=quote_url,
                    params=params,
                    headers=headers,
                    pacer=pacer,
                    stats=stats,
                ):
                    stats.quotes += _insert_quotes(
                        conn,
                        session_id=session_id,
                        symbol=symbol,
                        feed=feed,
                        rows=page.get("quotes") or [],
                    )

                trade_url = f"https://data.alpaca.markets/v2/stocks/{symbol}/trades"
                async for page in _pages(
                    client,
                    url=trade_url,
                    params=params,
                    headers=headers,
                    pacer=pacer,
                    stats=stats,
                ):
                    stats.trades += _insert_trades(
                        conn,
                        session_id=session_id,
                        symbol=symbol,
                        feed=feed,
                        rows=page.get("trades") or [],
                    )
    except Exception:
        conn.execute(
            "UPDATE sessions SET notes = COALESCE(notes,'') || ? WHERE session_id=?",
            ("\nINCOMPLETE_DOWNLOAD", session_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()

    if stats.quotes == 0 or stats.trades == 0:
        raise RuntimeError(
            f"Recording is incomplete for replay: quotes={stats.quotes}, trades={stats.trades}. "
            "Check the trading date, symbol, feed entitlement and requested session."
        )
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Alpaca historical trades + quotes into ReplayRuntime-compatible SQLite"
    )
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--date", required=True, help="US market date YYYY-MM-DD")
    parser.add_argument("--db", default="data/alpaca_replay.sqlite")
    parser.add_argument("--feed", choices=["sip", "iex"], default="sip")
    parser.add_argument(
        "--session",
        choices=["regular", "extended"],
        default="regular",
        help="regular=09:30-16:00 ET, extended=04:00-20:00 ET",
    )
    parser.add_argument(
        "--requests-per-minute",
        type=int,
        default=180,
        help="Client-side pacing; raise this if your Alpaca plan permits a higher rate.",
    )
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--alpaca-key", default=None)
    parser.add_argument("--alpaca-secret", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    stats = asyncio.run(
        record_day(
            symbols=args.symbols,
            day=date.fromisoformat(args.date),
            db_path=args.db,
            feed=args.feed,
            session=args.session,
            api_key=args.alpaca_key,
            api_secret=args.alpaca_secret,
            requests_per_minute=args.requests_per_minute,
            session_id=args.session_id,
        )
    )
    print(
        json.dumps(
            {
                "session_id": stats.session_id,
                "symbols": stats.symbols,
                "feed": stats.feed,
                "start": stats.start,
                "end": stats.end,
                "quotes": stats.quotes,
                "trades": stats.trades,
                "requests": stats.requests,
                "retries": stats.retries,
                "replay_db": str(Path(args.db).resolve()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

"""
Market Price MCP Connector (SRC-PRICE: Yahoo Finance + Stooq)
=============================================================

A Model Context Protocol server that gives Claude market price / volume / yield
data. SRC-PRICE is your highest-coverage free source (6 frameworks): SURGE,
Struct, ThirdWave, IPO Alpha, Income, P2P.

Tools:
  * price_history -> OHLCV history (raw close + adjusted close + volume) plus a
                     summary (latest, 52w high/low, all-time-high in range, % off
                     ATH, avg volume). Serves SURGE S4 (price/volume), Income &
                     P2P (prices), ThirdWave/Struct (price for the S8 multiple).
  * quote         -> current snapshot (last price, day change, volume, 52w range)
  * dividends     -> dividend history + trailing-12-mo total + indicated yield
                     (Income yield / yield-trap signal)
  * valuation     -> BEST-EFFORT multiples (market cap, P/E, div yield). See note.

THE TWO THINGS THAT SHAPE THIS BUILD (per the blueprint + the live API reality):
  1. Yahoo has NO official API. Its v8 CHART endpoint still works without auth
     and is reliable; its v7/v10 QUOTE/QUOTESUMMARY endpoints now need a crumb+
     cookie and break often (especially from a cloud host). So price/volume/
     dividends use the chart endpoint with a Stooq CSV fallback; `valuation` is
     best-effort and degrades gracefully.
  2. Valuation MULTIPLES are most reliably computed, not fetched: the blueprint
     sources ThirdWave's S8 as "EDGAR + price". The durable recipe is
        P/E        = price (here) / EPS (EDGAR xbrl_concept EarningsPerShareDiluted)
        market cap = price (here) x shares (EDGAR dei EntityCommonStockSharesOutstanding)
     and FLOAT for SURGE S4 also comes from EDGAR, not price data.

  PIT note: SRC-PRICE is PIT_PARTIAL — free sources revise splits/dividends
  retroactively, so adjusted close can change. Both raw `close` and `adj_close`
  are returned; for backtests, archive the value at score date (Income/ThirdWave
  both flag this) since a later-adjusted series is mild hindsight.

Data access: FREE, no API key (LIC_TOS_CHECK — be a polite client; this server
rate-limits to ~2 req/s and sends a browser User-Agent).

Transports (same pattern as your other connectors):
  * stdio            (default)  -> local testing in Claude Desktop
  * streamable-http  (MCP_TRANSPORT=http) -> hosted URL for a claude.ai project (/mcp)
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import time
from datetime import date, datetime, timezone, timedelta
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

MAX_RPS = float(os.environ.get("PRICE_MAX_RPS", "2"))   # Yahoo tolerates ~2/s
USER_AGENT = os.environ.get(
    "PRICE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
)
YCHART = "https://query1.finance.yahoo.com/v8/finance/chart"
YSUM = "https://query1.finance.yahoo.com/v10/finance/quoteSummary"
YCRUMB = "https://query1.finance.yahoo.com/v1/test/getcrumb"
STOOQ = "https://stooq.com/q/d/l/"

# Host-header fix (same as your other hosted connectors).
_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
mcp = FastMCP("market-price", transport_security=_security)


# ----------------------------------------------------------------------------
# Rate limiter + helpers
# ----------------------------------------------------------------------------

class _RateLimiter:
    def __init__(self, rps: float) -> None:
        self._min_interval = 1.0 / rps
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            delta = time.monotonic() - self._last
            if delta < self._min_interval:
                await asyncio.sleep(self._min_interval - delta)
            self._last = time.monotonic()


_limiter = _RateLimiter(MAX_RPS)


def _num(v: Any) -> float | None:
    try:
        f = float(v)
        return f
    except (TypeError, ValueError):
        return None


def _raw(v: Any) -> Any:
    """Yahoo wraps numbers as {'raw':123,'fmt':'123'}; unwrap to the raw value."""
    if isinstance(v, dict):
        return v.get("raw")
    return v


def _to_stooq_symbol(ticker: str) -> str:
    t = ticker.strip().lower()
    if "." in t or t.startswith("^"):
        return t
    return f"{t}.us"            # US equities/ETFs on Stooq use the .us suffix


# ----------------------------------------------------------------------------
# Data sources
# ----------------------------------------------------------------------------

async def _yahoo_chart(symbol: str, rng: str, interval: str,
                       events: str | None = None) -> dict:
    await _limiter.wait()
    params = {"range": rng, "interval": interval}
    if events:
        params["events"] = events
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT},
                                 follow_redirects=True) as c:
        r = await c.get(f"{YCHART}/{symbol.upper()}", params=params)
        r.raise_for_status()
        data = r.json()
    result = (((data or {}).get("chart") or {}).get("result") or [None])[0]
    if not result:
        raise ValueError("Empty chart result")
    meta = result.get("meta", {})
    ts = result.get("timestamp", []) or []
    q = ((result.get("indicators", {}).get("quote") or [{}])[0]) or {}
    adj = ((result.get("indicators", {}).get("adjclose") or [{}])[0] or {}).get("adjclose")
    obs = []
    for i, t in enumerate(ts):
        c_ = (q.get("close") or [None] * len(ts))[i]
        if c_ is None:
            continue
        obs.append({
            "date": datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat(),
            "open": _num((q.get("open") or [None])[i]),
            "high": _num((q.get("high") or [None])[i]),
            "low": _num((q.get("low") or [None])[i]),
            "close": _num(c_),
            "adj_close": _num(adj[i]) if adj and i < len(adj) else None,
            "volume": _num((q.get("volume") or [None])[i]),
        })
    return {"source": "yahoo", "meta": meta, "observations": obs,
            "events": result.get("events", {})}


async def _stooq_daily(symbol: str) -> dict:
    await _limiter.wait()
    s = _to_stooq_symbol(symbol)
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT},
                                 follow_redirects=True) as c:
        r = await c.get(STOOQ, params={"s": s, "i": "d"})
        r.raise_for_status()
        text = r.text
    if not text or "<" in text[:1] or text.strip().lower().startswith("no data"):
        raise ValueError("Stooq returned no data")
    obs = []
    for row in csv.DictReader(io.StringIO(text)):
        if not row.get("Close"):
            continue
        obs.append({
            "date": row.get("Date"),
            "open": _num(row.get("Open")), "high": _num(row.get("High")),
            "low": _num(row.get("Low")), "close": _num(row.get("Close")),
            "adj_close": None, "volume": _num(row.get("Volume")),
        })
    return {"source": "stooq", "meta": {}, "observations": obs, "events": {}}


def _summarize(obs: list[dict]) -> dict:
    closes = [o["close"] for o in obs if o["close"] is not None]
    highs = [o["high"] for o in obs if o["high"] is not None] or closes
    lows = [o["low"] for o in obs if o["low"] is not None] or closes
    vols = [o["volume"] for o in obs if o["volume"] is not None]
    if not closes:
        return {}
    ath = max(highs) if highs else max(closes)
    latest = closes[-1]
    last30 = vols[-30:] if vols else []
    return {
        "latest_close": latest,
        "latest_adj_close": next((o["adj_close"] for o in reversed(obs)
                                  if o["adj_close"] is not None), None),
        "period_high": max(highs) if highs else None,
        "period_low": min(lows) if lows else None,
        "all_time_high_in_range": ath,
        "pct_off_ath": round((latest / ath - 1) * 100, 2) if ath else None,
        "latest_volume": vols[-1] if vols else None,
        "avg_volume_30d": round(sum(last30) / len(last30)) if last30 else None,
        "observation_count": len(obs),
    }


# ----------------------------------------------------------------------------
# Tools
# ----------------------------------------------------------------------------

@mcp.tool()
async def price_history(ticker: str, range: str = "1y", interval: str = "1d") -> dict:
    """OHLCV price history with raw close, adjusted close, and volume.

    Tries Yahoo's chart endpoint first, falls back to Stooq if Yahoo is blocked.

    Args:
        ticker: Symbol, e.g. "AAPL", "BRK-B", "^GSPC", "BTC-USD".
        range: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max (default 1y).
        interval: 1d,1wk,1mo (default 1d).
    Returns:
        Source, currency/exchange, a summary (latest, 52w/period high-low,
        all-time-high in range, % off ATH, avg volume), and recent observations.
    """
    try:
        data = await _yahoo_chart(ticker, range, interval, events="div,split")
    except Exception:
        try:
            data = await _stooq_daily(ticker)
        except Exception as e:
            return {"ticker": ticker.upper(), "found": False,
                    "note": f"Neither Yahoo nor Stooq returned data ({e})."}
    obs = data["observations"]
    if not obs:
        return {"ticker": ticker.upper(), "found": False,
                "note": "No observations returned for that symbol/range."}
    summary = _summarize(obs)            # computed over the FULL series
    meta = data.get("meta", {})
    trimmed = obs[-250:]                 # keep the payload reasonable
    return {
        "ticker": ticker.upper(),
        "found": True,
        "source": data["source"],
        "currency": meta.get("currency"),
        "exchange": meta.get("exchangeName"),
        "range": range, "interval": interval,
        "summary": summary,
        "observations_returned": len(trimmed),
        "observations_truncated": len(obs) > len(trimmed),
        "observations": trimmed,
        "pit_note": "Adjusted close can change as splits/dividends are applied "
                    "retroactively (PIT_PARTIAL). Archive at score date for backtests.",
    }


@mcp.tool()
async def quote(ticker: str) -> dict:
    """Current snapshot: last price, day change, volume, and 52-week range.

    Args:
        ticker: Symbol, e.g. "AAPL".
    Returns:
        Latest price and key levels (from Yahoo chart meta; Stooq fallback).
    """
    try:
        data = await _yahoo_chart(ticker, "5d", "1d")
        m = data.get("meta", {})
        obs = data["observations"]
        last = _num(m.get("regularMarketPrice")) or (obs[-1]["close"] if obs else None)
        prev = _num(m.get("chartPreviousClose")) or _num(m.get("previousClose"))
        chg = (last - prev) if (last is not None and prev is not None) else None
        return {
            "ticker": ticker.upper(), "found": last is not None, "source": "yahoo",
            "price": last,
            "previous_close": prev,
            "change": round(chg, 4) if chg is not None else None,
            "change_pct": round(chg / prev * 100, 2) if (chg is not None and prev) else None,
            "day_high": _num(m.get("regularMarketDayHigh")),
            "day_low": _num(m.get("regularMarketDayLow")),
            "volume": _num(m.get("regularMarketVolume")),
            "fifty_two_week_high": _num(m.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low": _num(m.get("fiftyTwoWeekLow")),
            "currency": m.get("currency"), "exchange": m.get("exchangeName"),
        }
    except Exception:
        try:
            data = await _stooq_daily(ticker)
            obs = data["observations"]
            last, prev = obs[-1]["close"], (obs[-2]["close"] if len(obs) > 1 else None)
            chg = (last - prev) if (last is not None and prev is not None) else None
            return {"ticker": ticker.upper(), "found": True, "source": "stooq",
                    "price": last, "previous_close": prev,
                    "change": round(chg, 4) if chg is not None else None,
                    "volume": obs[-1]["volume"]}
        except Exception as e:
            return {"ticker": ticker.upper(), "found": False,
                    "note": f"No quote available ({e})."}


@mcp.tool()
async def dividends(ticker: str, range: str = "2y") -> dict:
    """Dividend history plus trailing-12-month total and indicated yield.

    Serves Income's yield and yield-trap signal (a yield spike driven by a
    falling price, not a raised dividend, is the trap signature).

    Args:
        ticker: Symbol, e.g. "JEPI".
        range: Lookback for dividend events (default 2y).
    Returns:
        Dividend payments, TTM total, latest price, and indicated yield %.
    """
    try:
        data = await _yahoo_chart(ticker, range, "1d", events="div")
    except Exception as e:
        return {"ticker": ticker.upper(), "found": False,
                "note": f"Yahoo dividend data unavailable ({e}). Stooq has no "
                        "dividend feed; cross-check via your EDGAR connector (8-K)."}
    divs = ((data.get("events") or {}).get("dividends") or {})
    payments = []
    for d in divs.values():
        amt = _num(d.get("amount"))
        ts = d.get("date")
        if amt is None or ts is None:
            continue
        payments.append({"date": datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(),
                         "amount": amt})
    payments.sort(key=lambda x: x["date"])
    cutoff = (date.today() - timedelta(days=365)).isoformat()
    ttm = round(sum(p["amount"] for p in payments if p["date"] >= cutoff), 4)
    summary = _summarize(data["observations"])
    price = summary.get("latest_close")
    return {
        "ticker": ticker.upper(), "found": True,
        "payment_count": len(payments),
        "payments": payments[-12:],
        "ttm_dividend": ttm,
        "latest_price": price,
        "indicated_yield_pct": round(ttm / price * 100, 2) if (price and ttm) else None,
        "yield_trap_note": "If yield rose because price fell (not because the "
                           "dividend was raised), treat as a yield-trap flag.",
    }


@mcp.tool()
async def valuation(ticker: str) -> dict:
    """BEST-EFFORT valuation multiples (market cap, P/E, dividend yield).

    Yahoo's fundamentals endpoint requires a crumb/cookie and is frequently
    blocked from cloud hosts. This attempts it; if it fails, it still returns the
    live price and the durable recipe for computing multiples from your EDGAR
    connector (the blueprint's "EDGAR + price" path).

    Args:
        ticker: Symbol, e.g. "MSFT".
    Returns:
        Multiples if reachable; otherwise price + the EDGAR-based recipe.
    """
    sym = ticker.upper()
    recipe = {
        "pe_ratio": "price / EPS  (EDGAR xbrl_concept EarningsPerShareDiluted)",
        "market_cap": "price * shares  (EDGAR dei:EntityCommonStockSharesOutstanding)",
        "note": "This is the blueprint's reliable 'EDGAR + price' path for the "
                "ThirdWave S8 multiple; float for SURGE S4 also comes from EDGAR.",
    }
    try:
        async with httpx.AsyncClient(timeout=30.0,
                                     headers={"User-Agent": USER_AGENT},
                                     follow_redirects=True) as c:
            for seed in ("https://fc.yahoo.com", "https://finance.yahoo.com"):
                try:
                    await c.get(seed)
                    break
                except Exception:
                    continue
            await _limiter.wait()
            crumb = (await c.get(YCRUMB)).text.strip()
            if not crumb or "<" in crumb:
                raise ValueError("could not obtain crumb")
            await _limiter.wait()
            mods = "price,summaryDetail,defaultKeyStatistics,financialData"
            r = await c.get(f"{YSUM}/{sym}", params={"modules": mods, "crumb": crumb})
            r.raise_for_status()
            res = (((r.json() or {}).get("quoteSummary") or {}).get("result") or [None])[0]
            if not res:
                raise ValueError("empty quoteSummary")
            sd = res.get("summaryDetail", {})
            ks = res.get("defaultKeyStatistics", {})
            pr = res.get("price", {})
            return {
                "ticker": sym, "found": True, "best_effort": True, "source": "yahoo",
                "market_cap": _raw(pr.get("marketCap")),
                "trailing_pe": _raw(sd.get("trailingPE")),
                "forward_pe": _raw(sd.get("forwardPE")),
                "price_to_book": _raw(ks.get("priceToBook")),
                "dividend_yield_pct": (round(_raw(sd.get("dividendYield")) * 100, 2)
                                       if _raw(sd.get("dividendYield")) is not None else None),
                "shares_outstanding": _raw(ks.get("sharesOutstanding")),
                "float_shares": _raw(ks.get("floatShares")),
            }
    except Exception as e:
        live = await quote(sym)
        return {
            "ticker": sym, "best_effort": False,
            "yahoo_multiples_available": False,
            "reason": f"Yahoo fundamentals blocked/unavailable ({type(e).__name__}).",
            "price": live.get("price"),
            "compute_instead": recipe,
        }


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    print(f"[market-price] build-1 | dns_rebinding_protection="
          f"{_security.enable_dns_rebinding_protection} | "
          f"sources=yahoo-chart+stooq | "
          f"transport={os.environ.get('MCP_TRANSPORT', 'stdio')}",
          file=sys.stderr, flush=True)
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()
    if transport in ("http", "streamable-http", "streamable_http"):
        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")

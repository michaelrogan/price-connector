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
  * options_chain -> SURGE-ready options read: S3 gamma bin (call vol/OI), R5
                     put/call short-proxy + ATM IV, gamma ramp above spot, and
                     a naive dealer-GEX estimate (UW-2). CBOE delayed quotes
                     primary (no auth, all expiries, greeks included); Yahoo
                     options fallback (crumb-gated, BS gamma from IV).

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
YOPTS = "https://query1.finance.yahoo.com/v7/finance/options"
CBOE_OPTS = "https://cdn.cboe.com/api/global/delayed_quotes/options"
STOOQ = "https://stooq.com/q/d/l/"
RISK_FREE = float(os.environ.get("PRICE_RISK_FREE", "0.04"))  # for BS gamma fallback

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
# Options data sources (CBOE primary — one unauthenticated call, includes
# greeks; Yahoo fallback — needs the crumb dance and one call per expiry)
# ----------------------------------------------------------------------------

import math
import re

_OCC = re.compile(r"^([A-Z.]{1,6})(\d{6})([CP])(\d{8})$")


def _parse_occ(sym: str) -> dict | None:
    """'GME261218C00025000' -> {expiry, type, strike}."""
    m = _OCC.match(sym.replace(" ", ""))
    if not m:
        return None
    _, ymd, cp, strike8 = m.groups()
    try:
        expiry = datetime.strptime(ymd, "%y%m%d").date().isoformat()
    except ValueError:
        return None
    return {"expiry": expiry, "type": "call" if cp == "C" else "put",
            "strike": int(strike8) / 1000.0}


def _bs_gamma(spot: float, strike: float, iv: float, t_years: float,
              r: float = RISK_FREE) -> float | None:
    """Black-Scholes gamma (same for calls and puts). Used only when the
    source doesn't supply gamma (Yahoo path)."""
    if not all([spot and spot > 0, strike and strike > 0,
                iv and iv > 0, t_years and t_years > 0]):
        return None
    try:
        d1 = (math.log(spot / strike) + (r + iv * iv / 2.0) * t_years) / (iv * math.sqrt(t_years))
        npdf = math.exp(-d1 * d1 / 2.0) / math.sqrt(2.0 * math.pi)
        return npdf / (spot * iv * math.sqrt(t_years))
    except (ValueError, ZeroDivisionError, OverflowError):
        return None


async def _cboe_options(symbol: str) -> dict:
    """All listed contracts in one call. Returns {spot, contracts:[...]} with
    each contract: {expiry, type, strike, volume, oi, iv, gamma}."""
    await _limiter.wait()
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT},
                                 follow_redirects=True) as c:
        r = await c.get(f"{CBOE_OPTS}/{symbol.upper()}.json")
        r.raise_for_status()
        data = r.json()
    d = (data or {}).get("data") or {}
    spot = _num(d.get("close")) or _num(d.get("current_price"))
    contracts = []
    for o in d.get("options") or []:
        meta = _parse_occ(str(o.get("option", "")))
        if not meta:
            continue
        iv = _num(o.get("iv"))
        if iv is not None and iv > 5:      # some feeds report 45.2 not 0.452
            iv = iv / 100.0
        contracts.append({**meta,
                          "volume": _num(o.get("volume")) or 0.0,
                          "oi": _num(o.get("open_interest")) or 0.0,
                          "iv": iv,
                          "gamma": _num(o.get("gamma"))})
    if not contracts:
        raise ValueError("CBOE returned no parseable contracts")
    return {"source": "cboe", "spot": spot, "contracts": contracts}


async def _yahoo_options(symbol: str, max_expiries: int = 3) -> dict:
    """Crumb-authenticated Yahoo path; one request per expiry, capped."""
    sym = symbol.upper()
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT},
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

        async def _one(date_epoch: int | None) -> dict:
            await _limiter.wait()
            params: dict[str, Any] = {"crumb": crumb}
            if date_epoch:
                params["date"] = date_epoch
            r = await c.get(f"{YOPTS}/{sym}", params=params)
            r.raise_for_status()
            res = (((r.json() or {}).get("optionChain") or {}).get("result") or [None])[0]
            if not res:
                raise ValueError("empty optionChain")
            return res

        first = await _one(None)
        spot = _num(_raw((first.get("quote") or {}).get("regularMarketPrice")))
        expiries = (first.get("expirationDates") or [])[:max_expiries]
        blocks = [first]
        for ep in expiries[1:]:
            try:
                blocks.append(await _one(ep))
            except Exception:
                break
        contracts = []
        for b in blocks:
            for grp in b.get("options") or []:
                ep = grp.get("expirationDate")
                if not ep:
                    continue
                expiry = datetime.fromtimestamp(ep, tz=timezone.utc).date().isoformat()
                for side, typ in (("calls", "call"), ("puts", "put")):
                    for o in grp.get(side) or []:
                        contracts.append({
                            "expiry": expiry, "type": typ,
                            "strike": _num(_raw(o.get("strike"))),
                            "volume": _num(_raw(o.get("volume"))) or 0.0,
                            "oi": _num(_raw(o.get("openInterest"))) or 0.0,
                            "iv": _num(_raw(o.get("impliedVolatility"))),
                            "gamma": None,
                        })
    if not contracts:
        raise ValueError("Yahoo returned no contracts")
    return {"source": "yahoo", "spot": spot, "contracts": contracts}


def _derive_options_read(spot: float | None, contracts: list[dict],
                         window_days: int) -> dict:
    """The SURGE-ready derived fields: S3 gamma bin, R5 put/call proxy,
    gamma ramp, and a naive dealer-GEX estimate."""
    today = date.today()
    horizon = today + timedelta(days=window_days)
    window = [k for k in contracts
              if k.get("strike") and k["expiry"] >= today.isoformat()
              and k["expiry"] <= horizon.isoformat()]
    if not window:
        window = [k for k in contracts if k["expiry"] >= today.isoformat()]
    calls = [k for k in window if k["type"] == "call"]
    puts = [k for k in window if k["type"] == "put"]

    cv = sum(k["volume"] for k in calls)
    coi = sum(k["oi"] for k in calls)
    pv = sum(k["volume"] for k in puts)
    poi = sum(k["oi"] for k in puts)

    ratio = round(cv / coi, 3) if coi else None
    if ratio is None:
        s3_pts, s3_rule = None, "call OI is zero/unreported — cannot bin"
    elif ratio > 2:
        s3_pts, s3_rule = 19, "> 2x"
    elif ratio >= 1.5:
        s3_pts, s3_rule = 13, "1.5-2x"
    elif ratio >= 1.0:
        s3_pts, s3_rule = 7, "1-1.5x"
    else:
        s3_pts, s3_rule = 0, "< 1x"

    pc_vol = round(pv / cv, 3) if cv else None
    pc_oi = round(poi / coi, 3) if coi else None
    if pc_vol is None:
        r5_read = "no call volume — P/C undefined"
    elif pc_vol > 1.2:
        r5_read = ("P/C > 1.2 — IF IV is flat/declining vs your archived "
                   "snapshot, treat as institutional shorting (+1 S1 bin)")
    elif pc_vol < 0.6:
        r5_read = "P/C < 0.6 — bullish positioning / shorts may be covering"
    else:
        r5_read = "P/C 0.6-1.2 — neutral, standard S1 scoring"

    atm_ivs = []
    if spot:
        for k in window:
            if k.get("iv") and abs(k["strike"] - spot) / spot <= 0.05:
                atm_ivs.append(k["iv"])
    atm_iv = round(sum(atm_ivs) / len(atm_ivs) * 100, 1) if atm_ivs else None

    ramp_pct, top_strikes = None, []
    if spot and coi:
        above = [k for k in calls if spot < k["strike"] <= spot * 1.3]
        ramp_pct = round(sum(k["oi"] for k in above) / coi * 100, 1)
        by_strike: dict[float, float] = {}
        for k in calls:
            by_strike[k["strike"]] = by_strike.get(k["strike"], 0.0) + k["oi"]
        top_strikes = [{"strike": s, "call_oi": int(o)} for s, o in
                       sorted(by_strike.items(), key=lambda x: -x[1])[:3]]

    gex, gex_n = 0.0, 0
    if spot:
        for k in window:
            g = k.get("gamma")
            if g is None and k.get("iv"):
                t = (date.fromisoformat(k["expiry"]) - today).days / 365.0
                g = _bs_gamma(spot, k["strike"], k["iv"], max(t, 1 / 365.0))
            if g is None or not k["oi"]:
                continue
            sign = 1.0 if k["type"] == "call" else -1.0
            gex += sign * g * k["oi"] * 100.0 * spot * spot * 0.01
            gex_n += 1
    gex_out = None
    if gex_n:
        gex_out = {
            "gex_usd_per_1pct_move": round(gex),
            "sign": "positive" if gex > 0 else "negative",
            "contracts_used": gex_n,
            "caveat": ("Naive dealer model (dealers long calls / short puts). In "
                       "squeeze names where RETAIL is the call buyer, dealers are "
                       "short calls and the sign interpretation INVERTS. Use the "
                       "day-over-day FLIP (UW-2), not the level, and archive "
                       "snapshots to see it."),
        }

    return {
        "window_days": window_days,
        "contracts_in_window": len(window),
        "expiries_in_window": sorted({k["expiry"] for k in window}),
        "s3_gamma": {"call_volume": int(cv), "call_open_interest": int(coi),
                     "call_vol_oi_ratio": ratio, "s3_points": s3_pts,
                     "bin": s3_rule, "max_points_note":
                     "S3 gamma sub-component only (max 19); ETF mechanical "
                     "+8 is scored separately from holdings data"},
        "short_proxy_r5": {"put_call_volume_ratio": pc_vol,
                           "put_call_oi_ratio": pc_oi,
                           "atm_iv_pct": atm_iv, "read": r5_read,
                           "iv_trend_note": "IV TREND needs two snapshots — "
                           "archive atm_iv_pct daily; R5 fires on P/C > 1.2 "
                           "with flat/declining IV."},
        "gamma_ramp": {"pct_call_oi_0_to_30pct_above_spot": ramp_pct,
                       "top_call_oi_strikes": top_strikes},
        "dealer_gex_estimate": gex_out,
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


@mcp.tool()
async def options_chain(ticker: str, window_days: int = 45,
                        include_strikes: bool = False) -> dict:
    """SURGE-ready options read: S3 gamma bin, R5 put/call short-proxy,
    gamma ramp above spot, and a naive dealer-GEX estimate (UW-2 input).

    Sources: CBOE delayed quotes PRIMARY (one unauthenticated call, all
    expiries, dealer greeks included), Yahoo options FALLBACK (crumb-gated,
    nearest 3 expiries, gamma computed via Black-Scholes from IV).

    Args:
        ticker: Symbol, e.g. "GME".
        window_days: Expiry horizon for the derived metrics (default 45 —
            squeeze mechanics live in near-dated contracts).
        include_strikes: If True, also return near-the-money per-strike rows
            (spot +/- 30%), capped at 60 rows. Default False — payloads are big.
    Returns:
        Derived SURGE fields plus a PIT note. Velocity-style reads (IV trend,
        GEX flip) need day-over-day snapshots — archive this output daily for
        any ticker under active monitoring.
    """
    sym = ticker.upper()
    data = None
    errors = []
    for fetch in (_cboe_options, _yahoo_options):
        try:
            data = await fetch(sym)
            break
        except Exception as e:
            errors.append(f"{fetch.__name__}: {type(e).__name__}")
    if not data:
        return {"ticker": sym, "found": False,
                "note": f"No options data from CBOE or Yahoo ({'; '.join(errors)}). "
                        "Ticker may have no listed options — note that no listed "
                        "options also means S3 gamma scores 0 by definition."}

    spot = data.get("spot")
    if spot is None:
        try:
            q = await quote(sym)
            spot = q.get("price")
        except Exception:
            pass

    derived = _derive_options_read(spot, data["contracts"], window_days)
    out = {
        "ticker": sym, "found": True, "source": data["source"],
        "spot": spot,
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **derived,
        "pit_note": "PIT_NONE — this is a latest-value snapshot. IV trend (R5) "
                    "and the GEX flip (UW-2) are day-over-day signals: archive "
                    "daily for tickers under active monitoring.",
    }
    if include_strikes and spot:
        rows = [k for k in data["contracts"]
                if k.get("strike") and abs(k["strike"] - spot) / spot <= 0.30
                and k["expiry"] in derived["expiries_in_window"]]
        rows.sort(key=lambda k: (k["expiry"], k["strike"], k["type"]))
        out["strikes"] = [{**{kk: vv for kk, vv in k.items() if kk != "gamma"},
                           "iv_pct": round(k["iv"] * 100, 1) if k.get("iv") else None}
                          for k in rows[:60]]
        for row in out["strikes"]:
            row.pop("iv", None)
    return out


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    print(f"[market-price] build-2-options | dns_rebinding_protection="
          f"{_security.enable_dns_rebinding_protection} | "
          f"sources=yahoo-chart+stooq+cboe-options | "
          f"transport={os.environ.get('MCP_TRANSPORT', 'stdio')}",
          file=sys.stderr, flush=True)
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()
    if transport in ("http", "streamable-http", "streamable_http"):
        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")

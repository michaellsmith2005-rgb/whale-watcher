"""
Polymarket Top-50 Consensus Trade Pipeline
============================================

Identifies the "consensus trade" among the top 50 traders on the Polymarket
leaderboard by:
  1. Pulling the top 50 traders from the official Leaderboard endpoint.
  2. Fetching each trader's current open positions + portfolio value.
  3. Aggregating by market/outcome to compute:
       - Pure Count Consensus   (most traders in the same outcome)
       - Capital-Weighted Consensus (most $ exposure in the same outcome)
       - High-Conviction Divergence (>=5 traders, each >13% of own portfolio)
  4. Writing a markdown summary + structured JSON report.

Data sources (official, public, no auth required):
  - GET https://data-api.polymarket.com/v1/leaderboard   (rankings)
  - GET https://data-api.polymarket.com/positions         (per-wallet positions)
  - GET https://data-api.polymarket.com/value              (per-wallet total portfolio value)

API rate limits (per Polymarket docs, Data API):
  - /positions: 150 req / 10s server-side cap
  - This script self-throttles client-side to a configurable
    MAX_REQUESTS_PER_SECOND (default 12), comfortably under that cap and
    polite to shared infrastructure.

Usage:
    python polymarket_consensus.py
    python polymarket_consensus.py --top-n 50 --time-period MONTH --min-position-usd 1.0

Outputs:
    consensus_report.json
    consensus_report.md
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import pandas as pd

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

DATA_API_BASE = "https://data-api.polymarket.com"
LEADERBOARD_URL = f"{DATA_API_BASE}/v1/leaderboard"
POSITIONS_URL = f"{DATA_API_BASE}/positions"
VALUE_URL = f"{DATA_API_BASE}/value"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

DEFAULT_TOP_N = 50                  # leaderboard /v1/leaderboard caps 'limit' at 50
DEFAULT_TIME_PERIOD = "MONTH"       # DAY | WEEK | MONTH | ALL
DEFAULT_ORDER_BY = "PNL"            # PNL | VOL
MAX_REQUESTS_PER_SECOND = 12        # client-side throttle; server cap is 150/10s on /positions
REQUEST_TIMEOUT = 20.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.5            # seconds, exponential backoff
HIGH_CONVICTION_MIN_TRADERS = 5
HIGH_CONVICTION_MIN_PORTFOLIO_PCT = 13.0   # (legacy) old absolute-% conviction threshold
# Elite skill-weighted conviction signal:
CONVICTION_MIN_TRADERS = 3                 # >= this many proven wallets convicted on a market
CONVICTION_FLOOR_PCT = 2.0                 # a bet must be >= 2% of the trader's bankroll (cost basis)
CONVICTION_OUTSIZED_MULT = 3.0             # AND >= 3x that trader's own median bet size
MIN_POSITION_USD_DEFAULT = 1.0      # ignore dust positions
ACTIVE_ONLY_DEFAULT = True          # show only games still live/tradeable on the board
ECONOMICALLY_RESOLVED_HI = 0.99     # a token at >= this has effectively already won;
ECONOMICALLY_RESOLVED_LO = 0.01     # at <= this it has already lost. Either way the
                                    # position is a dead ticket, not a live bet — counting
                                    # it as "consensus" produced fake 20-trader signals on
                                    # decided games (e.g. Mexico Yes @ 1.00).
MAX_POSITIONS_PER_WALLET = 10000    # hard cap on pagination; some wallets make /positions
                                    # return full pages indefinitely (offset is ignored),
                                    # which would otherwise loop forever.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("polymarket_consensus")


# --------------------------------------------------------------------------
# Data classes
# --------------------------------------------------------------------------

@dataclass
class Trader:
    rank: Optional[str]
    proxy_wallet: str
    username: str
    pnl: float
    vol: float
    portfolio_value: Optional[float] = None   # filled in from /value
    fetch_error: Optional[str] = None          # records why positions are missing, if so


@dataclass
class Position:
    trader_wallet: str
    trader_username: str
    condition_id: str
    market_slug: str
    market_title: str
    event_slug: str
    outcome: str
    outcome_index: Optional[int]
    size: float
    avg_price: float
    cur_price: float
    current_value_usd: float
    cash_pnl: float
    end_date: Optional[str] = None         # market end/resolution date (ISO), if known
    redeemable: bool = False               # True once the market has resolved (off the board)
    portfolio_pct: Optional[float] = None  # current_value_usd / trader portfolio_value * 100
    cost_basis_pct: Optional[float] = None  # (avg_price*size) / portfolio_value * 100 — capital
                                            # DELIBERATELY deployed, not inflated by paper gains


# --------------------------------------------------------------------------
# Async rate-limited HTTP client
# --------------------------------------------------------------------------

class RateLimiter:
    """Simple token-bucket-ish limiter: at most `rate` permits granted per second."""

    def __init__(self, rate_per_second: int):
        self._interval = 1.0 / rate_per_second
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._last + self._interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


async def fetch_json_with_retry(
    client: httpx.AsyncClient,
    limiter: RateLimiter,
    url: str,
    params: dict[str, Any],
    context: str,
) -> Optional[Any]:
    """GET with client-side rate limiting, retries on 429/5xx, exponential backoff."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        await limiter.acquire()
        try:
            resp = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", RETRY_BACKOFF_BASE * attempt))
                logger.warning(f"[{context}] 429 rate-limited, backing off {retry_after:.1f}s (attempt {attempt})")
                await asyncio.sleep(retry_after)
                continue
            if resp.status_code >= 500:
                logger.warning(f"[{context}] {resp.status_code} server error, retrying (attempt {attempt})")
                await asyncio.sleep(RETRY_BACKOFF_BASE * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_exc = exc
            logger.warning(f"[{context}] network error: {exc!r} (attempt {attempt})")
            await asyncio.sleep(RETRY_BACKOFF_BASE * attempt)
        except httpx.HTTPStatusError as exc:
            # 4xx other than 429: not retryable (e.g. malformed wallet, hidden profile -> often 400/404)
            logger.error(f"[{context}] HTTP {exc.response.status_code}: {exc}")
            return None
        except Exception as exc:  # noqa: BLE001 - we want to capture and continue the pipeline
            last_exc = exc
            logger.error(f"[{context}] unexpected error: {exc!r}")
            return None

    logger.error(f"[{context}] giving up after {MAX_RETRIES} attempts: {last_exc!r}")
    return None


# --------------------------------------------------------------------------
# Step 1: Leaderboard
# --------------------------------------------------------------------------

async def fetch_leaderboard(
    client: httpx.AsyncClient,
    limiter: RateLimiter,
    top_n: int,
    time_period: str,
    order_by: str,
) -> list[Trader]:
    """
    Fetch top traders from the official leaderboard endpoint.
    NOTE: the API hard-caps `limit` at 50 per request, so top_n > 50 requires
    pagination via `offset`. We paginate defensively even though top_n=50 is
    the common case.
    """
    traders: list[Trader] = []
    offset = 0
    page_size = min(50, top_n)

    while len(traders) < top_n:
        remaining = top_n - len(traders)
        limit = min(page_size, remaining, 50)
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
            "offset": offset,
        }
        data = await fetch_json_with_retry(client, limiter, LEADERBOARD_URL, params, "leaderboard")
        if not data:
            logger.error("Leaderboard fetch returned no data — aborting pagination.")
            break

        for row in data:
            wallet = row.get("proxyWallet")
            if not wallet:
                continue
            traders.append(
                Trader(
                    rank=row.get("rank"),
                    proxy_wallet=wallet,
                    username=row.get("userName") or wallet[:10],
                    pnl=float(row.get("pnl") or 0.0),
                    vol=float(row.get("vol") or 0.0),
                )
            )

        if len(data) < limit:
            break  # no more pages
        offset += limit

    logger.info(f"Fetched {len(traders)} traders from leaderboard (timePeriod={time_period}, orderBy={order_by}).")
    return traders[:top_n]


# --------------------------------------------------------------------------
# Step 2: Portfolio value + positions per trader
# --------------------------------------------------------------------------

async def fetch_portfolio_value(
    client: httpx.AsyncClient, limiter: RateLimiter, wallet: str
) -> Optional[float]:
    data = await fetch_json_with_retry(
        client, limiter, VALUE_URL, {"user": wallet}, f"value:{wallet[:8]}"
    )
    if not data:
        return None
    # Response shape: list with one object containing a 'value' field, or a single object.
    try:
        if isinstance(data, list) and data:
            return float(data[0].get("value", 0.0))
        if isinstance(data, dict):
            return float(data.get("value", 0.0))
    except (TypeError, ValueError):
        return None
    return None


async def fetch_positions(
    client: httpx.AsyncClient, limiter: RateLimiter, wallet: str
) -> Optional[list[dict]]:
    all_positions: list[dict] = []
    offset = 0
    limit = 500  # API max page size
    while True:
        params = {
            "user": wallet,
            "limit": limit,
            "offset": offset,
            "sortBy": "CURRENT",
            "sortDirection": "DESC",
        }
        page = await fetch_json_with_retry(
            client, limiter, POSITIONS_URL, params, f"positions:{wallet[:8]}"
        )
        if page is None:
            # Distinguish "no data returned due to error" from "empty portfolio".
            return None if offset == 0 else all_positions
        if not isinstance(page, list):
            return all_positions
        all_positions.extend(page)
        if len(page) < limit:
            break
        offset += limit
        # Safety valve: a few wallets cause /positions to return full pages
        # forever (the API stops honoring `offset`), which would loop until OOM.
        # No real trader holds anywhere near this many positions, so stop here.
        if offset >= MAX_POSITIONS_PER_WALLET:
            logger.warning(
                f"[positions:{wallet[:8]}] hit {MAX_POSITIONS_PER_WALLET}-position cap; "
                f"stopping pagination (likely an API quirk for this wallet)."
            )
            break
    return all_positions


async def enrich_trader(
    client: httpx.AsyncClient,
    limiter: RateLimiter,
    trader: Trader,
    min_position_usd: float,
    active_only: bool,
    today_iso: str,
) -> list[Position]:
    """Fetch value + positions for one trader; return normalized Position rows."""
    value_task = fetch_portfolio_value(client, limiter, trader.proxy_wallet)
    positions_task = fetch_positions(client, limiter, trader.proxy_wallet)
    portfolio_value, raw_positions = await asyncio.gather(value_task, positions_task)

    trader.portfolio_value = portfolio_value

    if raw_positions is None:
        trader.fetch_error = "positions_fetch_failed_or_hidden_profile"
        logger.warning(f"Could not fetch positions for {trader.username} ({trader.proxy_wallet[:10]}...)")
        return []

    rows: list[Position] = []
    for p in raw_positions:
        try:
            current_value = float(p.get("currentValue") or 0.0)
            if current_value < min_position_usd:
                continue
            redeemable = bool(p.get("redeemable"))
            end_date = p.get("endDate")
            # Skip games already off the board (resolved) unless explicitly asked to keep them.
            if active_only and not is_active_market(redeemable, end_date, today_iso):
                continue
            size = float(p.get("size") or 0.0)
            avg_price = float(p.get("avgPrice") or 0.0)
            cur_price = float(p.get("curPrice") or 0.0)
            # A token already at ~1.00 (won) or ~0.00 (lost) is a dead ticket, even if
            # the market hasn't been formally marked redeemable yet. Not a live bet.
            if active_only and (
                cur_price >= ECONOMICALLY_RESOLVED_HI or cur_price <= ECONOMICALLY_RESOLVED_LO
            ):
                continue
            cash_pnl = float(p.get("cashPnl") or 0.0)
            pct = None
            cost_pct = None
            if portfolio_value and portfolio_value > 0:
                pct = round((current_value / portfolio_value) * 100, 2)
                # Cost basis = what they paid to open (avg entry × size) — measures
                # deliberate conviction, immune to mark-to-market drift from winners.
                cost_pct = round((avg_price * size / portfolio_value) * 100, 2)

            rows.append(
                Position(
                    trader_wallet=trader.proxy_wallet,
                    trader_username=trader.username,
                    condition_id=p.get("conditionId", ""),
                    market_slug=p.get("slug", ""),
                    market_title=p.get("title", "Unknown Market"),
                    event_slug=p.get("eventSlug", ""),
                    outcome=p.get("outcome", "Unknown"),
                    outcome_index=p.get("outcomeIndex"),
                    size=size,
                    avg_price=avg_price,
                    cur_price=cur_price,
                    current_value_usd=current_value,
                    cash_pnl=cash_pnl,
                    end_date=str(end_date) if end_date else None,
                    redeemable=redeemable,
                    portfolio_pct=pct,
                    cost_basis_pct=cost_pct,
                )
            )
        except (TypeError, ValueError) as exc:
            logger.warning(f"Skipping malformed position for {trader.username}: {exc!r} | raw={p}")
            continue

    return rows


async def gather_all_positions(
    traders: list[Trader], min_position_usd: float, active_only: bool, today_iso: str
) -> list[Position]:
    limiter = RateLimiter(MAX_REQUESTS_PER_SECOND)
    headers = {"User-Agent": "polymarket-consensus-pipeline/1.0"}

    async with httpx.AsyncClient(headers=headers) as client:
        tasks = [
            enrich_trader(client, limiter, t, min_position_usd, active_only, today_iso)
            for t in traders
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    all_positions: list[Position] = []
    for rows in results:
        all_positions.extend(rows)

    n_failed = sum(1 for t in traders if t.fetch_error)
    logger.info(
        f"Position fetch complete: {len(all_positions)} positions across "
        f"{len(traders) - n_failed}/{len(traders)} traders "
        f"({n_failed} profiles failed/hidden)."
    )
    return all_positions


# --------------------------------------------------------------------------
# Step 3: Consensus & aggregation logic
# --------------------------------------------------------------------------

def build_dataframe(positions: list[Position]) -> pd.DataFrame:
    if not positions:
        return pd.DataFrame(
            columns=[
                "trader_wallet", "trader_username", "condition_id", "market_slug",
                "market_title", "event_slug", "outcome", "outcome_index", "size",
                "avg_price", "cur_price", "current_value_usd", "cash_pnl",
                "end_date", "redeemable", "portfolio_pct", "cost_basis_pct",
            ]
        )
    return pd.DataFrame([asdict(p) for p in positions])


def is_active_market(redeemable: Any, end_date: Any, today_iso: str) -> bool:
    """
    A market is still "on the board" (live and accessible) when it has not yet
    resolved. The clearest signal Polymarket gives is `redeemable`: it flips to
    True once a market settles and the position can be cashed out. We also drop
    anything whose end date is already in the past, to catch the brief window
    between a game ending and the position being marked redeemable.
    """
    if redeemable:
        return False
    if end_date:
        # endDate is ISO (e.g. "2026-06-28" or a full timestamp); date prefix sorts correctly.
        if str(end_date)[:10] < today_iso:
            return False
    return True


def market_key(row) -> str:
    """A market+outcome is uniquely identified by conditionId + outcome string."""
    return f"{row['condition_id']}::{row['outcome']}"


def add_value_gap(grouped: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach the price/value gap to each consensus market.

    `cur_price`   — the current market-implied probability (≈ uniform across the
                    wallets holding that outcome).
    `avg_entry`   — the crowd's size-weighted average entry price (their cost basis).
    `entry_gap_cents` — (avg_entry - cur_price) * 100. Positive means the outcome
                    now trades BELOW where the smart money got in (you could enter
                    cheaper than they did); negative means it has run past their
                    entry and they're already in profit (you'd be paying up / late).

    This is the earnings-critical readout: consensus tells you WHERE the money is;
    the gap tells you whether there's any left. It is informational, not a buy
    signal — a lower price can also mean the market simply disagrees more.
    """
    if grouped.empty or df.empty:
        return grouped
    tmp = df.copy()
    tmp["_entry_notional"] = tmp["avg_price"] * tmp["size"]
    stats = (
        tmp.groupby(["condition_id", "outcome"])
        .agg(_en=("_entry_notional", "sum"), _sz=("size", "sum"), cur_price=("cur_price", "mean"))
        .reset_index()
    )
    stats["avg_entry"] = (stats["_en"] / stats["_sz"].where(stats["_sz"] != 0)).round(4)
    stats["cur_price"] = stats["cur_price"].round(4)
    stats["entry_gap_cents"] = ((stats["avg_entry"] - stats["cur_price"]) * 100).round(1)
    stats = stats.drop(columns=["_en", "_sz"])
    return grouped.merge(stats, on=["condition_id", "outcome"], how="left")


async def fetch_market_quality(
    client: httpx.AsyncClient, limiter: RateLimiter, condition_ids: list[str]
) -> dict[str, dict]:
    """
    Pull live liquidity / volume / spread for the given markets from Gamma.
    Powers the dashboard's trust meter and slippage estimate: a price is only as
    reliable as the money standing behind it, and a big bet into a thin book moves
    the price against you.
    """
    ids = sorted({c for c in condition_ids if c})
    out: dict[str, dict] = {}
    for i in range(0, len(ids), 20):
        chunk = ids[i:i + 20]
        params = [("condition_ids", c) for c in chunk] + [("limit", str(len(chunk)))]
        data = await fetch_json_with_retry(client, limiter, GAMMA_MARKETS_URL, params, "gamma:quality")
        if not isinstance(data, list):
            continue
        for m in data:
            cid = m.get("conditionId")
            if not cid:
                continue
            try:
                bid = float(m.get("bestBid")) if m.get("bestBid") is not None else None
                ask = float(m.get("bestAsk")) if m.get("bestAsk") is not None else None
            except (TypeError, ValueError):
                bid = ask = None
            out[cid] = {
                "liquidity_usd": _safe_float(m.get("liquidityNum")),
                "volume_24h": _safe_float(m.get("volume24hr")),
                "spread": _safe_float(m.get("spread")),
                "best_bid": bid,
                "best_ask": ask,
            }
    return out


def _safe_float(v):
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def add_market_quality(grouped: pd.DataFrame, quality: dict[str, dict]) -> pd.DataFrame:
    """
    Attach liquidity/volume/spread + best bid/ask to a consensus table.

    Gamma reports bestBid/bestAsk for the YES token. For a "No" consensus row the
    tradeable price is the complement (buying No ≡ selling Yes), so we flip:
    no_ask = 1 - yes_bid, no_bid = 1 - yes_ask. Without this, a "No" row would be
    priced off the wrong side of the book and produce nonsense edges.
    """
    if grouped.empty:
        return grouped
    g = grouped.copy()
    for col in ("liquidity_usd", "volume_24h", "spread"):
        g[col] = g["condition_id"].map(lambda c: (quality.get(c) or {}).get(col))

    def _side(row, want):
        q = quality.get(row["condition_id"]) or {}
        yb, ya = q.get("best_bid"), q.get("best_ask")
        is_no = str(row["outcome"]).upper() == "NO"
        if want == "ask":
            return (round(1 - yb, 4) if yb is not None else None) if is_no else ya
        return (round(1 - ya, 4) if ya is not None else None) if is_no else yb

    g["best_ask"] = g.apply(lambda r: _side(r, "ask"), axis=1)
    g["best_bid"] = g.apply(lambda r: _side(r, "bid"), axis=1)
    return g


def add_conflict_flags(grouped: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect self-contradicting consensus. The forward replay showed the same market
    appearing on BOTH sides at once (e.g. 9 top wallets on 'Canada Yes' AND 8 on
    'Canada No'). When the cohort disagrees with itself, "consensus" is just a
    description of an active market, not a signal.

    Adds per consensus row:
      opp_traders — distinct top wallets holding any OTHER outcome of this market
      net_traders — trader_count minus opp_traders (the cohort's actual net lean)
      conflicted  — True when the opposing side is at least half the backing side
    Recorded at fire-time so the backtester can grade clean vs conflicted cohorts.
    """
    if grouped.empty or df.empty:
        return grouped
    g = grouped.copy()
    per_outcome = (
        df.groupby(["condition_id", "outcome"])["trader_wallet"].nunique().rename("n").reset_index()
    )
    per_market = per_outcome.groupby("condition_id")["n"].sum().to_dict()
    own = {(r.condition_id, r.outcome): r.n for r in per_outcome.itertuples()}

    def _opp(row):
        total = per_market.get(row["condition_id"], 0)
        mine = own.get((row["condition_id"], row["outcome"]), 0)
        return int(total - mine)

    g["opp_traders"] = g.apply(_opp, axis=1)
    base = g["trader_count"] if "trader_count" in g.columns else g.get("conviction_traders", 0)
    g["net_traders"] = (base - g["opp_traders"]).astype(int)
    g["conflicted"] = g["opp_traders"] >= (base / 2).clip(lower=1)
    return g


def compute_pure_count_consensus(df: pd.DataFrame, top_k: int = 5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(["condition_id", "market_title", "event_slug", "outcome"])
        .agg(
            trader_count=("trader_wallet", "nunique"),
            total_usd=("current_value_usd", "sum"),
            traders=("trader_username", lambda s: sorted(set(s))),
        )
        .reset_index()
        .sort_values(["trader_count", "total_usd"], ascending=[False, False])
    )
    return add_value_gap(grouped.head(top_k), df)


def compute_capital_weighted_consensus(df: pd.DataFrame, top_k: int = 5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(["condition_id", "market_title", "event_slug", "outcome"])
        .agg(
            total_usd=("current_value_usd", "sum"),
            trader_count=("trader_wallet", "nunique"),
            traders=("trader_username", lambda s: sorted(set(s))),
        )
        .reset_index()
        .sort_values(["total_usd", "trader_count"], ascending=[False, False])
    )
    return add_value_gap(grouped.head(top_k), df)


def compute_trader_skill_weights(traders: list[Trader]) -> dict[str, float]:
    """
    A per-trader skill weight in [0.15, 1.0] from PnL-EFFICIENCY (profit per dollar
    of volume), Bayesian-shrunk toward the cohort's volume-weighted mean. Rewards
    edge over churn, and shrinkage stops a low-volume trader's lucky ratio from
    dominating. (Until the backtester has real per-trader calibration, this is the
    best skill proxy from data we already have.)
    """
    data = [(t.proxy_wallet, t.pnl, t.vol) for t in traders if t.vol and t.vol > 0]
    if not data:
        return {}
    total_pnl = sum(p for _, p, _ in data)
    total_vol = sum(v for _, _, v in data)
    mean_eff = (total_pnl / total_vol) if total_vol > 0 else 0.0
    vols = sorted(v for _, _, v in data)
    k = vols[len(vols) // 2] or 1.0          # median volume = shrinkage strength
    raw = {w: max((p + k * mean_eff) / (v + k), 0.0) for w, p, v in data}
    hi = max(raw.values()) or 1.0
    # Normalize to [0.15, 1.0] so every qualified wallet still counts a little.
    return {w: round(0.15 + 0.85 * (val / hi), 4) for w, val in raw.items()}


def compute_conviction_consensus(
    df: pd.DataFrame,
    skill_weights: dict[str, float],
    min_traders: int = CONVICTION_MIN_TRADERS,
    floor_pct: float = CONVICTION_FLOOR_PCT,
    outsized_mult: float = CONVICTION_OUTSIZED_MULT,
) -> pd.DataFrame:
    """
    Elite conviction signal. A position counts as conviction for a trader when it is
    BOTH outsized for them (>= outsized_mult x their own median bet, by COST BASIS —
    so paper gains don't inflate it) AND materially large (>= floor_pct of bankroll).
    Each qualifying bet is weighted by the trader's skill, and a market needs
    >= min_traders such wallets. The score is the skill-weighted sum of bankroll
    deployed, normalized to 0-100.
    """
    if df.empty:
        return pd.DataFrame()
    d = df[df["cost_basis_pct"].notna() & (df["cost_basis_pct"] > 0)].copy()
    if d.empty:
        return pd.DataFrame()

    d["t_median"] = d.groupby("trader_wallet")["cost_basis_pct"].transform("median")
    qual = d[(d["cost_basis_pct"] >= outsized_mult * d["t_median"])
             & (d["cost_basis_pct"] >= floor_pct)].copy()
    if qual.empty:
        return pd.DataFrame()

    qual["skill"] = qual["trader_wallet"].map(lambda w: skill_weights.get(w, 0.15))
    qual["weighted_conv"] = qual["skill"] * qual["cost_basis_pct"]

    grouped = (
        qual.groupby(["condition_id", "market_title", "event_slug", "outcome"])
        .agg(
            conviction_traders=("trader_wallet", "nunique"),
            score_raw=("weighted_conv", "sum"),
            avg_cost_pct=("cost_basis_pct", "mean"),
            avg_skill=("skill", "mean"),
            total_usd=("current_value_usd", "sum"),
            traders=("trader_username", lambda s: sorted(set(s))),
        )
        .reset_index()
    )
    grouped = grouped[grouped["conviction_traders"] >= min_traders]
    if grouped.empty:
        return pd.DataFrame()
    hi = grouped["score_raw"].max() or 1.0
    grouped["conviction_score"] = (grouped["score_raw"] / hi * 100).round(1)
    grouped["avg_cost_pct"] = grouped["avg_cost_pct"].round(2)
    grouped["avg_skill"] = grouped["avg_skill"].round(3)
    grouped = grouped.sort_values(["conviction_score", "conviction_traders"], ascending=False)
    # Attach cur_price / avg_entry / entry_gap so snapshots record a price for this
    # signal at fire-time. Without it, the forward backtest can never grade the
    # conviction signal — history would accumulate for the weaker signals only.
    return add_value_gap(grouped, df)


# --------------------------------------------------------------------------
# Step 4: Report generation
# --------------------------------------------------------------------------

def df_to_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records"))


def build_json_report(
    traders: list[Trader],
    positions_df: pd.DataFrame,
    pure_count: pd.DataFrame,
    capital_weighted: pd.DataFrame,
    high_conviction: pd.DataFrame,
    run_metadata: dict,
) -> dict:
    n_failed = sum(1 for t in traders if t.fetch_error)

    top5_combined = []
    for _, row in pure_count.head(5).iterrows():
        top5_combined.append(
            {
                "market_title": row["market_title"],
                "condition_id": row["condition_id"],
                "event_slug": row["event_slug"],
                "favored_outcome": row["outcome"],
                "num_top50_traders": int(row["trader_count"]),
                "total_combined_usd": round(float(row["total_usd"]), 2),
                "backers": row["traders"],
            }
        )

    return {
        "run_metadata": run_metadata,
        "summary": {
            "traders_requested": len(traders),
            "traders_with_data": len(traders) - n_failed,
            "traders_failed_or_hidden": n_failed,
            "total_open_positions_analyzed": int(len(positions_df)),
            "unique_markets_represented": int(positions_df["condition_id"].nunique()) if not positions_df.empty else 0,
        },
        "top_5_consensus_markets": top5_combined,
        "pure_count_consensus": df_to_records(pure_count),
        "capital_weighted_consensus": df_to_records(capital_weighted),
        "high_conviction_divergence": df_to_records(high_conviction),
        "failed_or_hidden_profiles": [
            {"username": t.username, "wallet": t.proxy_wallet, "reason": t.fetch_error}
            for t in traders
            if t.fetch_error
        ],
    }


def build_markdown_report(report: dict) -> str:
    md = []
    meta = report["run_metadata"]
    summ = report["summary"]

    md.append(f"# Polymarket Top-{meta['top_n']} Consensus Trade Report")
    md.append("")
    md.append(f"*Generated: {meta['generated_at_utc']} UTC*  ")
    md.append(f"*Leaderboard window: {meta['time_period']} | Ranked by: {meta['order_by']}*")
    md.append("")
    md.append("## Run Summary")
    md.append("")
    md.append(f"- Traders requested: **{summ['traders_requested']}**")
    md.append(f"- Traders with usable data: **{summ['traders_with_data']}**")
    md.append(f"- Failed / hidden profiles: **{summ['traders_failed_or_hidden']}**")
    md.append(f"- Total open positions analyzed: **{summ['total_open_positions_analyzed']}**")
    md.append(f"- Unique markets represented: **{summ['unique_markets_represented']}**")
    md.append("")

    md.append("## Top 5 Consensus Markets (by trader count)")
    md.append("")
    if not report["top_5_consensus_markets"]:
        md.append("_No consensus markets found (insufficient data)._")
    else:
        for i, m in enumerate(report["top_5_consensus_markets"], 1):
            md.append(f"### {i}. {m['market_title']}")
            md.append(f"- **Favored outcome:** {m['favored_outcome']}")
            md.append(f"- **Top-50 traders backing it:** {m['num_top50_traders']}")
            md.append(f"- **Total combined USD exposure:** ${m['total_combined_usd']:,.2f}")
            backers_preview = ", ".join(m["backers"][:10])
            more = f" (+{len(m['backers']) - 10} more)" if len(m["backers"]) > 10 else ""
            md.append(f"- **Backers:** {backers_preview}{more}")
            md.append("")

    md.append("## Capital-Weighted Consensus (by total USD exposure)")
    md.append("")
    cw = report["capital_weighted_consensus"][:5]
    if not cw:
        md.append("_No data._")
    else:
        md.append("| Market | Outcome | Traders | Total USD |")
        md.append("|---|---|---|---|")
        for row in cw:
            md.append(
                f"| {row['market_title']} | {row['outcome']} | {row['trader_count']} | "
                f"${row['total_usd']:,.2f} |"
            )
    md.append("")

    md.append(f"## Skill-Weighted Conviction (>= {CONVICTION_MIN_TRADERS} proven wallets, each outsized for them)")
    md.append("")
    hc = report["high_conviction_divergence"]
    if not hc:
        md.append("_No markets met the conviction threshold._")
    else:
        md.append("| Market | Outcome | Conviction Score | Wallets | Avg % of Bankroll | Avg Skill |")
        md.append("|---|---|---|---|---|---|")
        for row in hc:
            md.append(
                f"| {row['market_title']} | {row['outcome']} | {row['conviction_score']:.0f} | "
                f"{row['conviction_traders']} | {row['avg_cost_pct']:.1f}% | {row['avg_skill']:.2f} |"
            )
    md.append("")

    if report["failed_or_hidden_profiles"]:
        md.append("## Notes")
        md.append("")
        md.append(f"- {len(report['failed_or_hidden_profiles'])} trader profile(s) could not be fetched (hidden/private positions or API error) and were excluded from aggregation.")
        md.append("")

    return "\n".join(md)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

async def run_pipeline(
    top_n: int,
    time_period: str,
    order_by: str,
    min_position_usd: float,
    active_only: bool = ACTIVE_ONLY_DEFAULT,
) -> dict:
    limiter = RateLimiter(MAX_REQUESTS_PER_SECOND)
    headers = {"User-Agent": "polymarket-consensus-pipeline/1.0"}

    async with httpx.AsyncClient(headers=headers) as client:
        traders = await fetch_leaderboard(client, limiter, top_n, time_period, order_by)

    if not traders:
        raise RuntimeError("No traders fetched from leaderboard — cannot proceed.")

    today_iso = datetime.now(timezone.utc).date().isoformat()
    all_positions = await gather_all_positions(traders, min_position_usd, active_only, today_iso)
    positions_df = build_dataframe(all_positions)

    pure_count = compute_pure_count_consensus(positions_df, top_k=10)
    capital_weighted = compute_capital_weighted_consensus(positions_df, top_k=10)
    skill_weights = compute_trader_skill_weights(traders)
    high_conviction = compute_conviction_consensus(positions_df, skill_weights)

    # Flag self-contradicting consensus (same market backed on both sides by the
    # cohort) — recorded at fire-time so the replay can grade clean vs conflicted.
    pure_count = add_conflict_flags(pure_count, positions_df)
    capital_weighted = add_conflict_flags(capital_weighted, positions_df)
    high_conviction = add_conflict_flags(high_conviction, positions_df)

    # Enrich the consensus markets with live liquidity/volume/spread from Gamma,
    # so the dashboard can show how much to trust each price and estimate slippage.
    cids = []
    for d in (pure_count, capital_weighted, high_conviction):
        if not d.empty:
            cids.extend(d["condition_id"].tolist())
    if cids:
        async with httpx.AsyncClient(headers=headers) as client:
            quality = await fetch_market_quality(client, limiter, cids)
        pure_count = add_market_quality(pure_count, quality)
        capital_weighted = add_market_quality(capital_weighted, quality)
        high_conviction = add_market_quality(high_conviction, quality)

    # Optional: sportsbook (Vegas) comparison, only if an Odds API key is set.
    # Wrapped so a missing key or odds-API hiccup can never break the pipeline.
    try:
        import vegas
        if vegas.has_key():
            records = df_to_records(pure_count) + df_to_records(capital_weighted)
            vprob = await vegas.vegas_probabilities(records)
            if vprob:
                # vprob is the team-wins probability per market. Align it to each
                # consensus row's actual outcome: a "No" row compares against 1-p.
                def _row_vegas(row):
                    p = vprob.get(row["condition_id"])
                    if p is None:
                        return None
                    return round(1 - p, 4) if str(row["outcome"]).upper() == "NO" else round(p, 4)

                for d in (pure_count, capital_weighted):
                    if not d.empty:
                        d["vegas_prob"] = d.apply(_row_vegas, axis=1)
                logger.info(f"Attached Vegas odds to {len(vprob)} market(s).")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Vegas comparison skipped: {exc!r}")

    run_metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "top_n": top_n,
        "time_period": time_period,
        "order_by": order_by,
        "min_position_usd_filter": min_position_usd,
        "active_only": active_only,
    }

    report = build_json_report(
        traders, positions_df, pure_count, capital_weighted, high_conviction, run_metadata
    )
    return report


def main():
    parser = argparse.ArgumentParser(description="Polymarket Top-50 Consensus Trade Pipeline")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="Number of top traders (max 50 per leaderboard page)")
    parser.add_argument("--time-period", choices=["DAY", "WEEK", "MONTH", "ALL"], default=DEFAULT_TIME_PERIOD)
    parser.add_argument("--order-by", choices=["PNL", "VOL"], default=DEFAULT_ORDER_BY)
    parser.add_argument("--min-position-usd", type=float, default=MIN_POSITION_USD_DEFAULT)
    parser.add_argument(
        "--active-only", action=argparse.BooleanOptionalAction, default=ACTIVE_ONLY_DEFAULT,
        help="Only include games still live on the board (drop resolved/redeemable markets). "
             "Use --no-active-only to include resolved positions too.",
    )
    parser.add_argument("--out-json", default="consensus_report.json")
    parser.add_argument("--out-md", default="consensus_report.md")
    parser.add_argument(
        "--snapshot", action="store_true",
        help="Also archive a timestamped copy into snapshots/ (for the backtester). "
             "Lets a scheduled job build history without the dashboard server running.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress the markdown dump (for cron logs)")
    args = parser.parse_args()

    try:
        report = asyncio.run(
            run_pipeline(args.top_n, args.time_period, args.order_by, args.min_position_usd, args.active_only)
        )
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc!r}")
        sys.exit(1)

    with open(args.out_json, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"Wrote JSON report -> {args.out_json}")

    md = build_markdown_report(report)
    with open(args.out_md, "w") as f:
        f.write(md)
    logger.info(f"Wrote Markdown report -> {args.out_md}")

    if args.snapshot:
        from pathlib import Path
        import shutil
        snap_dir = Path(args.out_json).resolve().parent / "snapshots"
        snap_dir.mkdir(exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dest = snap_dir / f"consensus_{stamp}.json"
        shutil.copy2(args.out_json, dest)
        logger.info(f"Archived snapshot -> {dest}")

    if not args.quiet:
        print("\n" + md)


if __name__ == "__main__":
    main()

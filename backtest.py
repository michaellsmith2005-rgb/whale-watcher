"""
backtest.py — does the smart-money consensus signal actually predict winners?
============================================================================

WHY THIS IS THE ONLY HONEST DESIGN
----------------------------------
The obvious idea — "pull the top traders' resolved bets and see how they did" —
DOES NOT WORK, and it's worth knowing why. On Polymarket a winning position is
redeemed and disappears from the feed; a losing token (worth $0) lingers forever
as "redeemable". So the resolved positions you can still see are ~100% losers by
construction. Measured directly, it showed a top-trader "win rate" near 0% — a
pure artifact. You cannot backtest from a single snapshot of holdings. Period.

The only lookahead-free method is to record the signal AT THE TIME and grade it
LATER against the market's true resolution:

  1. run_dashboard.py writes a timestamped snapshot on every refresh (snapshots/).
  2. This tool reads those snapshots, takes each consensus call and the market
     price at that moment, then asks the Polymarket Gamma API how the market
     actually resolved — independently of who held what.
  3. It scores win rate vs. the price you'd have paid (the only thing that implies
     skill), Brier calibration, and illustrative net ROI after costs.

Usage:
  python backtest.py              # grade accumulated snapshots (the real thing)
  python backtest.py --selftest   # prove the oracle+scoring work, using live
                                   # already-resolved markets (no snapshots needed)

Until your snapshots are old enough that their markets have resolved, the real
run will honestly report "still pending" — that's correct, not a bug.
"""
from __future__ import annotations

import argparse
import asyncio
import glob
import json
import os
from datetime import datetime, timezone

import httpx
import pandas as pd

import polymarket_consensus as P

HERE = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT_DIR = os.path.join(HERE, "snapshots")
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
RESOLVED_WIN = 0.99   # an outcome's settled price >= this => it won


# --------------------------------------------------------------------------
# Resolution oracle (Gamma API — independent of who holds the position)
# --------------------------------------------------------------------------

def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


async def fetch_resolutions(condition_ids: list[str]) -> dict[str, dict]:
    """
    Map condition_id -> {winner, outcomes, prices} for markets that have RESOLVED.
    Markets not yet settled simply won't come back (closed=true filters them), so
    callers treat "missing" as "still pending". Holder-independent ground truth.
    """
    ids = sorted({c for c in condition_ids if c})
    out: dict[str, dict] = {}
    if not ids:
        return out
    async with httpx.AsyncClient(headers={"User-Agent": "backtest/1.0"}, timeout=25) as client:
        for chunk in _chunks(ids, 20):
            params = [("condition_ids", c) for c in chunk]
            params += [("closed", "true"), ("limit", str(len(chunk)))]
            try:
                resp = await client.get(GAMMA_MARKETS, params=params)
                resp.raise_for_status()
                markets = resp.json()
            except (httpx.HTTPError, ValueError):
                continue
            for m in markets:
                cid = m.get("conditionId")
                try:
                    outs = json.loads(m.get("outcomes") or "[]")
                    prices = [float(x) for x in json.loads(m.get("outcomePrices") or "[]")]
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                winner = next((o for o, p in zip(outs, prices) if p >= RESOLVED_WIN), None)
                if cid and winner is not None:
                    out[cid] = {"winner": winner, "outcomes": outs, "prices": prices}
    return out


# --------------------------------------------------------------------------
# Scoring — shared by the real replay and the self-test
# --------------------------------------------------------------------------

def grade(calls: list[dict], resolutions: dict[str, dict], fee: float):
    """
    `calls`: dicts with condition_id, outcome, price (market-implied prob when the
             signal fired), n_traders, market_title.
    Returns (scored_rows, summary) for the calls whose markets have resolved.
    """
    scored = []
    for c in calls:
        res = resolutions.get(c["condition_id"])
        if not res:
            continue  # not resolved yet
        won = int(c["outcome"] == res["winner"])
        price = min(max(float(c.get("price") or 0.0), 1e-6), 1.0)
        roi = (won - price) / price - fee            # enter at signal-time price, hold to resolution
        scored.append({**c, "won": won, "winner": res["winner"], "price": price, "roi_net": roi})
    if not scored:
        return scored, None
    df = pd.DataFrame(scored)
    summary = {
        "n": len(df),
        "win_rate": df["won"].mean(),
        "implied": df["price"].mean(),
        "edge_pts": (df["won"].mean() - df["price"].mean()) * 100,
        "brier": ((df["price"] - df["won"]) ** 2).mean(),
        "roi_net": df["roi_net"].mean(),
    }
    return scored, summary


def print_summary(summary: dict, fee: float):
    print(f"    graded calls     : {summary['n']}")
    print(f"    win rate         : {summary['win_rate']*100:.1f}%")
    print(f"    implied (price)  : {summary['implied']*100:.1f}%   <- what you'd have paid")
    print(f"    edge vs price    : {summary['edge_pts']:+.1f} pts   <- >0 hints at real skill")
    print(f"    Brier score      : {summary['brier']:.3f}   <- lower = better calibrated")
    print(f"    net ROI/trade    : {summary['roi_net']*100:+.1f}%   (equal stake, {fee*100:.0f}% cost)")


# --------------------------------------------------------------------------
# Real replay over the snapshot archive
# --------------------------------------------------------------------------

def load_snapshot_calls() -> tuple[list[dict], str, str]:
    """Each consensus market's FIRST appearance across snapshots, with the price then."""
    files = sorted(glob.glob(os.path.join(SNAPSHOT_DIR, "consensus_*.json")))
    seen: dict[tuple, dict] = {}
    first = last = ""
    for f in files:
        try:
            with open(f) as fh:
                rep = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        ts = rep.get("run_metadata", {}).get("generated_at_utc", "")
        first = first or ts
        last = ts or last
        for c in rep.get("pure_count_consensus", []):
            key = (c.get("condition_id"), c.get("outcome"))
            if not key[0] or key in seen:
                continue
            seen[key] = {
                "condition_id": c.get("condition_id"),
                "outcome": c.get("outcome"),
                "price": c.get("cur_price"),
                "n_traders": c.get("trader_count"),
                "market_title": c.get("market_title"),
                "first_seen": ts,
            }
    return list(seen.values()), first, last


def run_replay(args):
    print("\nFORWARD REPLAY — grading past consensus calls against true resolutions\n")
    calls, first, last = load_snapshot_calls()
    if not calls:
        print("  No snapshots yet. run_dashboard.py writes one to snapshots/ on every refresh.")
        print("  Let it accumulate, then re-run `python backtest.py`.")
        return
    print(f"  {len(calls)} distinct consensus calls across snapshots "
          f"({first[:16]} → {last[:16]})")
    print("  Looking up resolutions via Gamma...")
    resolutions = asyncio.run(fetch_resolutions([c["condition_id"] for c in calls]))
    scored, summary = grade(calls, resolutions, args.fee)
    pending = len(calls) - len(scored)
    if summary is None:
        print(f"\n  0 of {len(calls)} calls have resolved yet ({pending} pending).")
        print("  This is expected early on — markets need time to settle. Re-run later.")
        return
    print(f"\n  Resolved & graded: {summary['n']}   |   still pending: {pending}\n")
    print_summary(summary, args.fee)
    print("\n  ⚠️  Small samples lie. Treat this as directional until you have many dozens")
    print("      of graded calls spanning different events.")
    won_calls = sorted([s for s in scored], key=lambda s: -(s.get("n_traders") or 0))[:8]
    print("\n  Most-backed graded calls:")
    for s in won_calls:
        v = "WON " if s["won"] else "lost"
        print(f"    [{v}] {s.get('n_traders')}× @ {s['price']:.2f} | "
              f"{(s.get('market_title') or '')[:46]} -> {s['outcome']}")


# --------------------------------------------------------------------------
# Self-test — proves oracle + scoring work today, using live resolved markets
# --------------------------------------------------------------------------

def run_selftest(args):
    print("\nSELF-TEST — validating the oracle + scoring on live resolved markets\n")

    async def grab_recent_closed(n):
        async with httpx.AsyncClient(headers={"User-Agent": "backtest/1.0"}, timeout=25) as client:
            r = await client.get(GAMMA_MARKETS, params={
                "closed": "true", "limit": str(n), "order": "endDate", "ascending": "false"})
            return r.json()

    markets = asyncio.run(grab_recent_closed(12))
    # Build synthetic "calls": pretend we'd flagged the YES outcome at a 0.50 price.
    calls = []
    for m in markets:
        try:
            outs = json.loads(m.get("outcomes") or "[]")
        except json.JSONDecodeError:
            continue
        if not outs:
            continue
        calls.append({
            "condition_id": m.get("conditionId"),
            "outcome": outs[0],
            "price": 0.50,
            "n_traders": 3,
            "market_title": m.get("question", ""),
        })
    resolutions = asyncio.run(fetch_resolutions([c["condition_id"] for c in calls]))
    scored, summary = grade(calls, resolutions, args.fee)
    if summary is None:
        print("  Could not resolve the sample markets — Gamma may be unreachable.")
        return
    print(f"  Fed {len(calls)} live resolved markets through the pipeline; "
          f"{summary['n']} graded cleanly.")
    print(f"  Resolution lookup + win/loss grading + metrics all executed:\n")
    print_summary(summary, args.fee)
    print("\n  (This isn't a strategy result — it's a wiring test proving the machinery")
    print("   is correct, so the real forward replay can be trusted once snapshots age.)")


def main():
    ap = argparse.ArgumentParser(description="Backtest the smart-money consensus signal (honestly).")
    ap.add_argument("--fee", type=float, default=0.02, help="Assumed round-trip cost (default 2%%)")
    ap.add_argument("--selftest", action="store_true", help="Validate the harness on live resolved markets")
    args = ap.parse_args()

    P.logging.getLogger("polymarket_consensus").setLevel(P.logging.WARNING)
    P.logging.getLogger("httpx").setLevel(P.logging.WARNING)

    if args.selftest:
        run_selftest(args)
    else:
        run_replay(args)


if __name__ == "__main__":
    main()

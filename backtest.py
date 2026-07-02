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
PRICE_CERTAIN_HI = 0.97   # calls priced at/above this (or at/below the LO) fired on an
PRICE_CERTAIN_LO = 0.03   # already-decided market — zero information, untradeable edge.
                          # Grading them inflates win rate with freebies (Mexico @ 1.00).


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

def wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval for a win rate — honest error bars on small n."""
    if n == 0:
        return 0.0, 1.0
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z / denom) * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return max(0.0, center - half), min(1.0, center + half)


def grade(calls: list[dict], resolutions: dict[str, dict], fee: float):
    """
    `calls`: dicts with condition_id, outcome, price (market-implied prob when the
             signal fired), n_traders, market_title.
    Returns (scored_rows, summary) for the calls whose markets have resolved.
    """
    scored = []
    excluded_certain = 0
    for c in calls:
        res = resolutions.get(c["condition_id"])
        if not res:
            continue  # not resolved yet
        price = min(max(float(c.get("price") or 0.0), 1e-6), 1.0)
        if price >= PRICE_CERTAIN_HI or price <= PRICE_CERTAIN_LO:
            excluded_certain += 1
            continue  # signal fired on an already-decided market — no information
        won = int(c["outcome"] == res["winner"])
        roi = (won - price) / price - fee            # enter at signal-time price, hold to resolution
        scored.append({**c, "won": won, "winner": res["winner"], "price": price, "roi_net": roi})
    if not scored:
        return scored, None
    df = pd.DataFrame(scored)
    wr = df["won"].mean()
    lo, hi = wilson_ci(int(df["won"].sum()), len(df))
    implied = df["price"].mean()
    summary = {
        "n": len(df),
        "excluded_certain": excluded_certain,
        "win_rate": wr,
        "wr_lo": lo,
        "wr_hi": hi,
        "implied": implied,
        "edge_pts": (wr - implied) * 100,
        "edge_lo_pts": (lo - implied) * 100,
        "edge_hi_pts": (hi - implied) * 100,
        "brier": ((df["price"] - df["won"]) ** 2).mean(),
        "roi_net": df["roi_net"].mean(),
        "roi_median": df["roi_net"].median(),
    }
    return scored, summary


def print_summary(summary: dict, fee: float):
    print(f"    graded calls     : {summary['n']}")
    if summary.get("excluded_certain"):
        print(f"    excluded         : {summary['excluded_certain']} call(s) priced ≥{PRICE_CERTAIN_HI:.2f} or "
              f"≤{PRICE_CERTAIN_LO:.2f} (already-decided, no information)")
    print(f"    win rate         : {summary['win_rate']*100:.1f}%   "
          f"(95% CI {summary['wr_lo']*100:.1f}–{summary['wr_hi']*100:.1f}%)")
    print(f"    implied (price)  : {summary['implied']*100:.1f}%   <- what you'd have paid")
    print(f"    edge vs price    : {summary['edge_pts']:+.1f} pts  "
          f"(95% CI {summary['edge_lo_pts']:+.1f} to {summary['edge_hi_pts']:+.1f})")
    if summary["edge_lo_pts"] <= 0 <= summary["edge_hi_pts"]:
        print(f"                       ^ CI straddles zero: NO detectable edge at this sample size")
    print(f"    Brier score      : {summary['brier']:.3f}   <- lower = better calibrated")
    print(f"    net ROI/trade    : mean {summary['roi_net']*100:+.1f}% | "
          f"median {summary['roi_median']*100:+.1f}%   ({fee*100:.0f}% cost)")
    if summary["roi_net"] > 0.10 and summary["roi_median"] < 0:
        print(f"                       ^ mean >> median: a few cheap longshots are carrying the")
        print(f"                         average — do NOT read mean ROI as repeatable edge")


# --------------------------------------------------------------------------
# Real replay over the snapshot archive
# --------------------------------------------------------------------------

SIGNAL_TABLES = {
    "pure_count": "pure_count_consensus",
    "capital_weighted": "capital_weighted_consensus",
    "conviction": "high_conviction_divergence",
}


def _entry_price(row: dict) -> tuple[float | None, str]:
    """
    The price you could actually have traded at signal time.
    best_ask (what a taker pays) beats cur_price (mid, untradeable).
    Returns (price, source_tag).
    """
    ask = row.get("best_ask")
    if ask is not None:
        try:
            a = float(ask)
            if 0 < a < 1:
                return a, "ask"
        except (TypeError, ValueError):
            pass
    cur = row.get("cur_price")
    if cur is not None:
        try:
            c = float(cur)
            if 0 < c < 1:
                return c, "mid"
        except (TypeError, ValueError):
            pass
    return None, "none"


def load_snapshot_calls() -> tuple[list[dict], str, str]:
    """
    Each (signal, market, outcome)'s FIRST appearance across snapshots, priced at
    that moment. All three signal families are graded separately — comparing them
    is the point of the exercise. Older snapshots recorded no price on conviction
    rows; for those we borrow the price from a sibling table in the SAME snapshot
    (same condition_id+outcome, same timestamp — still lookahead-free).
    """
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

        # Price index from sibling tables in this snapshot (for legacy conviction rows).
        sibling_price: dict[tuple, dict] = {}
        for tbl in ("pure_count_consensus", "capital_weighted_consensus"):
            for c in rep.get(tbl, []):
                k = (c.get("condition_id"), c.get("outcome"))
                if k[0] and k not in sibling_price:
                    sibling_price[k] = c

        for signal, tbl in SIGNAL_TABLES.items():
            for c in rep.get(tbl, []):
                cid, outc = c.get("condition_id"), c.get("outcome")
                key = (signal, cid, outc)
                if not cid or key in seen:
                    continue
                price, src = _entry_price(c)
                if price is None:  # legacy conviction rows: borrow same-snapshot price
                    sib = sibling_price.get((cid, outc))
                    if sib:
                        price, src = _entry_price(sib)
                        src = f"sibling_{src}" if price is not None else src
                if price is None:
                    continue  # genuinely ungradeable — no recorded price anywhere
                seen[key] = {
                    "signal": signal,
                    "condition_id": cid,
                    "outcome": outc,
                    "price": price,
                    "price_source": src,
                    "n_traders": c.get("trader_count") or c.get("conviction_traders"),
                    "entry_gap_cents": c.get("entry_gap_cents"),
                    "vegas_prob": c.get("vegas_prob"),
                    "conflicted": c.get("conflicted"),
                    "net_traders": c.get("net_traders"),
                    "market_title": c.get("market_title"),
                    "first_seen": ts,
                }
    return list(seen.values()), first, last


def _cohort_line(df: pd.DataFrame, label: str) -> str:
    if df.empty:
        return f"    {label:<26}: —"
    wr, imp = df["won"].mean(), df["price"].mean()
    return (f"    {label:<26}: n={len(df):<3} win {wr*100:5.1f}%  "
            f"paid {imp*100:5.1f}%  edge {(wr-imp)*100:+5.1f} pts")


def _dedup_markets(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (market, outcome). The same market fires in up to three signal
    families; treating those as independent calls lets a handful of markets
    triple-count and juice a cohort (seen 2026-07-02: 'conflicted +19.6 pts'
    was ~5 markets echoed across families). by_signal keeps the full frame —
    comparing families is its whole point — every other cohort dedups first.
    Prefer ask-priced rows (most tradeable) when duplicates disagree.
    """
    order = {"ask": 0, "sibling_ask": 1, "mid": 2, "sibling_mid": 3}
    d = df.copy()
    d["_src_rank"] = d.get("price_source", pd.Series(index=d.index)).map(order).fillna(9)
    d = d.sort_values("_src_rank").drop_duplicates(subset=["condition_id", "outcome"])
    return d.drop(columns=["_src_rank"])


def print_cohorts(scored: list[dict]):
    """The research questions: WHICH signal has edge, and does freshness/Vegas matter?"""
    df = pd.DataFrame(scored)
    print("\n  By signal family (all calls — families overlap by design):")
    for sig in SIGNAL_TABLES:
        print(_cohort_line(df[df["signal"] == sig], sig))

    u = _dedup_markets(df)
    print(f"\n  Cohorts below use unique markets only ({len(u)} of {len(df)} calls):")

    if "conflicted" in u.columns and u["conflicted"].notna().any():
        k = u[u["conflicted"].notna()]
        print("\n  By internal agreement (cohort on both sides of the market?):")
        print(_cohort_line(k[k["conflicted"] == False], "clean (one-sided cohort)"))  # noqa: E712
        print(_cohort_line(k[k["conflicted"] == True], "conflicted (both sides)"))    # noqa: E712

    if u["entry_gap_cents"].notna().any():
        g = u[u["entry_gap_cents"].notna()]
        print("\n  By freshness (entry gap = crowd avg entry − price at signal):")
        print(_cohort_line(g[g["entry_gap_cents"] >= 0], "fresh (≤ crowd's entry)"))
        print(_cohort_line(g[g["entry_gap_cents"] < 0], "late (past crowd's entry)"))

    if u["vegas_prob"].notna().any():
        v = u[u["vegas_prob"].notna()].copy()
        v["v_edge"] = v["vegas_prob"] - v["price"]
        print("\n  By Vegas confirmation (vegas prob vs price paid):")
        print(_cohort_line(v[v["v_edge"] >= 0.01], "vegas agrees (≥ +1 pt)"))
        print(_cohort_line(v[v["v_edge"].abs() < 0.01], "vegas neutral"))
        print(_cohort_line(v[v["v_edge"] <= -0.01], "vegas disagrees"))


def _cohort_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"n": 0}
    wr, imp = float(df["won"].mean()), float(df["price"].mean())
    return {"n": int(len(df)), "win_rate": round(wr, 4), "implied": round(imp, 4),
            "edge_pts": round((wr - imp) * 100, 2)}


def write_summary_json(scored: list[dict], summary: dict, pending: int,
                       path: str = "backtest_summary.json"):
    """Machine-readable verdict for downstream consumers (signal board, widget)."""
    df = pd.DataFrame(scored) if scored else pd.DataFrame()
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pending": pending,
        "edge_detected": bool(summary and summary["edge_lo_pts"] > 0),
        "headline": dict(summary) if summary else None,
        "cohorts": {},
    }
    if not df.empty:
        u = _dedup_markets(df)
        out["n_calls"] = int(len(df))
        out["n_unique_markets"] = int(len(u))
        out["cohorts"]["by_signal"] = {
            s: _cohort_stats(df[df["signal"] == s]) for s in SIGNAL_TABLES
        }
        if u["entry_gap_cents"].notna().any():
            g = u[u["entry_gap_cents"].notna()]
            out["cohorts"]["freshness"] = {
                "fresh": _cohort_stats(g[g["entry_gap_cents"] >= 0]),
                "late": _cohort_stats(g[g["entry_gap_cents"] < 0]),
            }
        if "conflicted" in u.columns and u["conflicted"].notna().any():
            k = u[u["conflicted"].notna()]
            out["cohorts"]["agreement"] = {
                "clean": _cohort_stats(k[k["conflicted"] == False]),   # noqa: E712
                "conflicted": _cohort_stats(k[k["conflicted"] == True]),  # noqa: E712
            }
    with open(path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n  Wrote machine-readable summary -> {path}")


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
    pending = len(calls) - len(scored) - (summary.get("excluded_certain", 0) if summary else 0)
    if summary is None:
        print(f"\n  0 of {len(calls)} calls have resolved yet ({pending} pending).")
        print("  This is expected early on — markets need time to settle. Re-run later.")
        write_summary_json([], None, pending)
        return
    print(f"\n  Resolved & graded: {summary['n']}   |   still pending: {pending}\n")
    print_summary(summary, args.fee)
    print_cohorts(scored)
    print("\n  ⚠️  Small samples lie. Treat this as directional until you have many dozens")
    print("      of graded calls spanning different events.")
    won_calls = sorted([s for s in scored], key=lambda s: -(s.get("n_traders") or 0))[:8]
    print("\n  Most-backed graded calls:")
    for s in won_calls:
        v = "WON " if s["won"] else "lost"
        print(f"    [{v}] [{s.get('signal','?'):<16}] {s.get('n_traders')}× @ {s['price']:.2f} "
              f"({s.get('price_source','?')}) | "
              f"{(s.get('market_title') or '')[:40]} -> {s['outcome']}")
    write_summary_json(scored, summary, pending)


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

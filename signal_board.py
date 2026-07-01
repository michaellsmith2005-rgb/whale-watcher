"""
signal_board.py — rank today's consensus setups into signal_board.json
=======================================================================

Reads the latest consensus_report.json (the pipeline's output) plus
backtest_summary.json (the verified track record) and produces a ranked
board for the widget/dashboard.

HONESTY CONTRACT — read before trusting the ranking:
  - The composite score below is a HEURISTIC, not a validated model. Its
    weights encode hypotheses (net lean matters, fresh beats late, clean
    beats conflicted, liquidity matters) that the forward test has NOT yet
    confirmed. The board carries `edge_detected` from the backtest; until
    that is true, treat the ranking as "what the signal sees", not
    "what to bet".
  - Rows are EXCLUDED (not just downranked) when they are untradeable or
    information-free: near-certain prices, conflicted cohorts, thin books.

Usage:
    python signal_board.py                # reads ./consensus_report.json
    python signal_board.py --report path --out signal_board.json
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

PRICE_CERTAIN_HI = 0.97
PRICE_CERTAIN_LO = 0.03
MIN_LIQUIDITY_USD = 10_000      # below this, the price is an opinion, not a market
MAX_SPREAD = 0.05               # a 5¢+ spread eats any plausible edge


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_rows(report: dict) -> list[dict]:
    """Merge the three signal tables into one row per (market, outcome)."""
    merged: dict[tuple, dict] = {}
    for tbl, tag in (("pure_count_consensus", "pure_count"),
                     ("capital_weighted_consensus", "capital_weighted"),
                     ("high_conviction_divergence", "conviction")):
        for r in report.get(tbl, []) or []:
            key = (r.get("condition_id"), r.get("outcome"))
            if not key[0]:
                continue
            row = merged.setdefault(key, {"signals": []})
            row["signals"].append(tag)
            for fld in ("market_title", "outcome", "condition_id", "event_slug",
                        "cur_price", "best_ask", "entry_gap_cents", "vegas_prob",
                        "liquidity_usd", "spread", "trader_count", "net_traders",
                        "opp_traders", "conflicted", "conviction_score", "avg_skill"):
                if row.get(fld) is None and r.get(fld) is not None:
                    row[fld] = r.get(fld)
    return list(merged.values())


def score_row(r: dict) -> tuple[float | None, list[str]]:
    """
    Returns (score, exclusion_reasons). score is None when excluded.
    Heuristic composite in [0, 100] — see HONESTY CONTRACT above.
    """
    reasons = []
    price = _f(r.get("best_ask")) or _f(r.get("cur_price"))
    if price is None:
        reasons.append("no price")
    elif price >= PRICE_CERTAIN_HI or price <= PRICE_CERTAIN_LO:
        reasons.append("already decided (near-certain price)")
    liq = _f(r.get("liquidity_usd"))
    if liq is not None and liq < MIN_LIQUIDITY_USD:
        reasons.append(f"thin book (<${MIN_LIQUIDITY_USD:,})")
    spread = _f(r.get("spread"))
    if spread is not None and spread > MAX_SPREAD:
        reasons.append("wide spread")
    if r.get("conflicted") is True:
        reasons.append("cohort on both sides")
    if reasons:
        return None, reasons

    # --- components, each 0..1 ---
    net = r.get("net_traders")
    base = r.get("trader_count") or 0
    lean = min((net if net is not None else base) / 10.0, 1.0)  # 10+ net wallets = max
    gap = _f(r.get("entry_gap_cents"))
    fresh = 0.5 if gap is None else (1.0 if gap >= 0 else max(0.0, 1.0 + gap / 10.0))
    n_sig = len(r.get("signals", [])) / 3.0                     # agreement across families
    skill = _f(r.get("avg_skill")) or 0.3
    vp, cp = _f(r.get("vegas_prob")), _f(r.get("cur_price"))
    vegas = 0.5
    if vp is not None and cp is not None:
        vegas = 1.0 if vp - cp >= 0.01 else (0.0 if cp - vp >= 0.01 else 0.5)

    score = 100 * (0.35 * lean + 0.25 * fresh + 0.20 * n_sig + 0.10 * skill + 0.10 * vegas)
    return round(score, 1), []


def build_board(report_path: str, backtest_path: str) -> dict:
    with open(report_path) as f:
        report = json.load(f)
    track = None
    if os.path.exists(backtest_path):
        with open(backtest_path) as f:
            track = json.load(f)

    rows = load_rows(report)
    ranked, excluded = [], []
    for r in rows:
        score, reasons = score_row(r)
        entry = {
            "market": r.get("market_title"),
            "outcome": r.get("outcome"),
            "price": _f(r.get("best_ask")) or _f(r.get("cur_price")),
            "net_traders": r.get("net_traders", r.get("trader_count")),
            "signals": r.get("signals"),
            "fresh": (_f(r.get("entry_gap_cents")) or 0) >= 0,
            "liquidity_usd": _f(r.get("liquidity_usd")),
        }
        if score is None:
            excluded.append({**entry, "why_excluded": reasons})
        else:
            ranked.append({**entry, "score": score})
    ranked.sort(key=lambda x: -x["score"])

    edge_detected = bool(track and track.get("edge_detected"))
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "VALIDATED EDGE" if edge_detected else "EXPERIMENTAL — no verified edge yet",
        "edge_detected": edge_detected,
        "track_record": (track or {}).get("headline"),
        "disclaimer": ("Heuristic ranking of an unvalidated signal. The forward test "
                       "has not shown this signal beats market prices. Not financial advice."),
        "board": ranked[:10],
        "excluded": excluded,
        "source_report_generated_at": report.get("run_metadata", {}).get("generated_at_utc"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="consensus_report.json")
    ap.add_argument("--backtest", default="backtest_summary.json")
    ap.add_argument("--out", default="signal_board.json")
    args = ap.parse_args()
    board = build_board(args.report, args.backtest)
    with open(args.out, "w") as f:
        json.dump(board, f, indent=2)
    n, x = len(board["board"]), len(board["excluded"])
    print(f"[{board['status']}] wrote {args.out}: {n} ranked, {x} excluded")
    for i, b in enumerate(board["board"][:5], 1):
        print(f"  {i}. {b['score']:5.1f}  {(b['market'] or '')[:44]} -> {b['outcome']} "
              f"@ {b['price']}")


if __name__ == "__main__":
    main()

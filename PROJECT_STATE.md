# Project State — Polymarket Consensus Tracker

_Last updated: 2026-07-01_

This file is the quick re-orientation point. Read it first when picking the
project back up. Keep it short and current — update the "Open threads" section
whenever you stop mid-task.

---

## What this is

A local tool that finds the "consensus trade" among Polymarket's most
profitable wallets — markets where the top traders are crowding into the same
bet — and cross-checks those against Vegas sportsbook odds. Runs locally because
Polymarket's data API sends no CORS headers, so a browser can't read it directly.

**Not financial advice. This is a signal-analysis tool, not an execution system.**

## Architecture (source of truth: the code, not this file)

| File | Role |
|------|------|
| `polymarket_consensus.py` | Async pipeline: fetch top-50 wallets + positions, compute the 3 consensus metrics, write `consensus_report.{json,md}` |
| `vegas.py` | Cross-check Polymarket implied prob vs. sportsbook odds (The Odds API). Env-var `ODDS_API_KEY` first, then `odds_api_key.txt` |
| `backtest.py` | Forward-test harness. Grades past signals against true Gamma-API resolutions. `--selftest` proves the machinery |
| `run_dashboard.py` | Runs pipeline, serves localhost, opens dashboard, writes a timestamped snapshot each refresh |
| `dashboard.html` | Renders `consensus_report.json` |
| `snapshots/` | Accumulated timestamped signal history — the raw material `backtest.py` needs |

## The three consensus metrics (core model)

1. **Consensus price (P_c)** — current Polymarket implied probability
2. **Volatility (σ)** — price fluctuation across time horizons
3. **Leaderboard synergy (L_s)** — weighted count of top traders holding the position

## Key design honesty (don't undo these)

- **No instant backtest is possible.** Polymarket redeems winning positions out
  of the feed, so a single snapshot of current holdings is ~100% losers by
  construction. The only valid method is forward: record signal now, grade after
  resolution. `backtest.py` does exactly this.
- **Headline consensus trades are usually already late** — the crowd's entry
  price vs. current price ("value gap") tells you if edge is left. Consensus
  tells you *where* the money is, not that it's still a good entry.

## Current state (as of last update)

- Snapshots accumulated: ~17 JSON snapshots spanning 2026-06-28 → 2026-06-30
- Backtest status: real replay likely still reports "pending" until snapshot
  markets resolve. `--selftest` works now.
- Dashboard: functional (v3 iteration reviewed in chat).

## Open threads / next steps

- [ ] Run `backtest.py` against accumulated snapshots — expect mostly "pending"
      until World Cup / CPI / Fed markets resolve. Confirm `--selftest` passes.
- [ ] Decide sizing logic (modified Kelly) — NOT yet built. Keep it conservative;
      the value-gap warning means naive full-Kelly on late signals is a trap.
- [ ] Optional pipeline additions previously discussed: liquidity-adjusted
      pricing, temporal decay, manipulation/wash-trade filter, resolution-source
      credibility, cross-market correlation. None built yet — signal layer only.

## Setup notes for a fresh machine

```bash
pip install httpx pandas
export ODDS_API_KEY=your_key_here   # or put it in odds_api_key.txt (gitignored)
python run_dashboard.py
```

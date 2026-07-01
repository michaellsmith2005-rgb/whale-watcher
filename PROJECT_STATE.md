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

## 2026-07-01 audit changes (do not silently revert)

- **Pipeline**: conviction table now records `cur_price` / `avg_entry` /
  `entry_gap_cents` / `best_ask` at fire-time. Before this, the strongest signal
  produced no gradeable price and could never be forward-tested.
- **Backtest**: grades all three signal families separately (`pure_count`,
  `capital_weighted`, `conviction`); enters at `best_ask` when recorded (mid
  fallback, tagged `price_source`); legacy conviction rows borrow the price from
  a sibling table in the same snapshot (still lookahead-free). Adds cohort
  breakdowns: by signal, fresh-vs-late, Vegas-agrees-vs-disagrees.
- Loader validated offline against the real archive: 96 distinct calls
  (was ~46), incl. 8 gradeable conviction calls. Live API validation was NOT
  possible in the sandbox — run `python backtest.py --selftest` locally once.
- Misc: `RateLimiter` uses `time.monotonic()`; added `requirements.txt`.

## Open threads / next steps

- [ ] Run `python backtest.py --selftest` locally to confirm the Gamma oracle
      wiring (sandbox couldn't reach Polymarket).
- [ ] Run the real replay once markets resolve — the cohort table (signal
      family × freshness × Vegas agreement) is the actual research readout.
- [ ] Decide sizing logic (modified Kelly) — NOT yet built, and deliberately
      blocked until the replay shows which signal, if any, has positive edge.
      Sizing an unproven signal is the classic failure mode.
- [ ] Optional pipeline additions previously discussed: temporal decay,
      manipulation/wash-trade filter, resolution-source credibility,
      cross-market correlation. None built yet.

## Setup notes for a fresh machine

```bash
pip install httpx pandas
export ODDS_API_KEY=your_key_here   # or put it in odds_api_key.txt (gitignored)
python run_dashboard.py
```

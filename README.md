# Polymarket Consensus Dashboard

Find the "consensus trade" among the top traders on Polymarket — the markets where the
most profitable wallets are crowding into the same bet — and view it in a clean local dashboard.

## Why this runs locally (and not as a web page)

Polymarket's `data-api.polymarket.com` sends **no CORS headers**, so a browser refuses to let
any web page or embedded widget read it directly. There is no client-side workaround — it's a
browser security rule. A Python script has no such restriction, so the pipeline fetches the data,
writes it to disk, and a tiny localhost server hands it to the dashboard. Nothing leaves your machine.

## Files

- `polymarket_consensus.py` — the async pipeline (fetches leaderboard + positions, computes the three consensus metrics, writes `consensus_report.json` and `.md`)
- `dashboard.html` — the dashboard; loads `consensus_report.json` and renders it
- `run_dashboard.py` — one command: runs the pipeline, serves the folder, opens the dashboard, and exposes a refresh endpoint
- `Whale Watcher.command` — double-click in Finder to launch everything (installs deps if needed); no terminal required
- `backtest.py` — grades past consensus calls against true market resolutions (Gamma API). `--selftest` proves the machinery; the default replay needs accumulated `snapshots/` to produce real numbers. See note below.
- `run_snapshot.sh` + `AUTOMATION.md` — scheduled snapshotting that builds the history `backtest.py` needs

## Value-gap & backtesting

- **Value-gap** — each consensus market shows the crowd's average **entry price vs. current price** ("6¢ now · crowd in @ 7¢ · fresh/late"). Consensus tells you *where* the money is; the gap tells you whether any is left. Headline trades are usually already *late*.
- **Why there's no instant backtest** — Polymarket redeems winning positions out of the feed, so current holdings are ~100% losers; you can't measure edge from one snapshot. The only honest method is forward: snapshot the signal now, grade it after markets resolve. That's what `snapshots/` + `backtest.py` do. **Not financial advice.**

## Quick start

Double-click **`Whale Watcher.command`** in Finder — it installs dependencies if needed, fetches data, and opens the dashboard. (First time, macOS may ask you to right-click → **Open** once to trust it.)

Or from the terminal:

```bash
pip install httpx pandas
python run_dashboard.py
```

That fetches the top 50 (month, by PnL), writes the report, and opens
`http://localhost:8000/dashboard.html` in your browser.

## Using the dashboard

- **Refresh data** (top-right button) re-runs the pipeline in place and reloads — no need to restart anything.
- **Leaderboard window** (Day / Week / Month / All time) switches the time window and refreshes automatically.
- **Live games only**: by default the dashboard shows only markets still open on the board — games that have already resolved (and are just waiting to be redeemed) are filtered out. Pass `--no-active-only` to include resolved positions too.

> The Refresh button and window switcher need the `run_dashboard.py` server (the launcher). If you open `dashboard.html` as a plain file, it still renders the last report but those controls are inert.

### Options

```bash
python run_dashboard.py --top-n 50 --time-period ALL --order-by PNL --port 8000
python run_dashboard.py --no-fetch          # just open the dashboard on existing data
python run_dashboard.py --no-active-only    # include already-resolved markets
```

Time periods: `DAY` `WEEK` `MONTH` `ALL`. Order by: `PNL` or `VOL`.

## Running the pieces separately

```bash
# 1. fetch + compute only
python polymarket_consensus.py --top-n 50 --time-period MONTH

# 2. serve + view (any static server works)
python -m http.server 8000
# then open http://localhost:8000/dashboard.html
```

## The three metrics

1. **By trader count** — how many of the top N hold the same market outcome (one wallet, one vote).
2. **By capital** — total dollars the top N have on each outcome (a whale can outweigh a crowd).
3. **High conviction** — markets where ≥5 top traders each have >20% of their own portfolio in the same bet.

Comparing count vs. capital is where it gets interesting: a market high on one list but low on the
other tells you whether agreement is broad (many small bets) or concentrated (a few big ones).

## Notes

- Wallets with hidden/private portfolios are logged and excluded; the count appears in the dashboard footer.
- "Live games only" filtering uses each position's `redeemable` flag and market `endDate`: a market is kept only if it hasn't resolved and its end date hasn't passed.
- The pipeline self-throttles to stay well under Polymarket's rate limits (150 req/10s on `/positions`).
- Not financial advice. Past performance of these traders doesn't predict outcomes.

#!/bin/bash
# Fetch a fresh consensus report and archive a timestamped snapshot.
# Run by the LaunchAgent a few times a day so the backtester accumulates history
# without the dashboard server needing to be open. Safe to run by hand too.
cd "$(dirname "$0")" || exit 1
mkdir -p snapshots

PY=/Library/Frameworks/Python.framework/Versions/3.14/bin/python3
[ -x "$PY" ] || PY="$(command -v python3)"

echo "=== $(date -u '+%Y-%m-%dT%H:%M:%SZ') snapshot run ===" >> snapshots/cron.log
"$PY" polymarket_consensus.py \
  --time-period MONTH --snapshot --quiet \
  --out-json consensus_report.json --out-md consensus_report.md \
  >> snapshots/cron.log 2>&1
echo "exit $? at $(date -u '+%H:%M:%SZ')" >> snapshots/cron.log

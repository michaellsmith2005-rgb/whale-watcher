# Vegas / sportsbook odds (optional)

The Bet Analyzer can compare each market against sportsbook odds — it needs a free feed.

1. Get a free key at **https://the-odds-api.com** (~500 calls/month, ~2 minutes).
2. Create a file in this `app` folder called **`odds_api_key.txt`** and paste **just the
   key** into it (nothing else).
3. Verify:  `cd app && python3 vegas.py --selftest`  — you should see World-Cup odds.
4. Refresh the dashboard — the **Vegas line** box in the Bet Analyzer fills in for
   sports/World-Cup markets (politics/crypto markets have no sportsbook line).

Everything fails soft: no key = the analyzer just shows "no line", nothing breaks.

---

# Automated snapshots

A macOS LaunchAgent (`~/Library/LaunchAgents/com.whalewatcher.snapshot.plist`) runs
the pipeline **4× a day** (09:00, 13:00, 18:00, 22:00) and archives a timestamped
report into `snapshots/`. This is what feeds `backtest.py` over time.

## ⚠️ One-time setup: grant Full Disk Access (required)

macOS blocks unattended background jobs from reading `~/Downloads` until you allow it.
Until you do this, the scheduled runs fail silently (you'll see "Operation not
permitted" in `snapshots/launchd.err.log`). To enable automation:

1. Open **System Settings → Privacy & Security → Full Disk Access**.
2. Click **＋**. In the file dialog press **⌘⇧G** and paste:
   `/Library/Frameworks/Python.framework/Versions/3.14/bin/python3`
   then **Open**, and toggle it **on**.
3. Test it immediately (no need to wait for a scheduled time):
   ```
   launchctl kickstart -k gui/$(id -u)/com.whalewatcher.snapshot
   ls -t snapshots/consensus_*.json | head   # a new file should appear within ~30s
   ```

**Don't want to grant disk access?** You don't strictly need the scheduler — a
snapshot is also written **every time you refresh the dashboard**. Just opening
Whale Watcher and hitting Refresh a few times a day builds the same history.
You can also run it by hand anytime: `./run_snapshot.sh`.

## Managing the schedule
- Change times: edit the `StartCalendarInterval` blocks in the plist, then
  `launchctl unload …/com.whalewatcher.snapshot.plist && launchctl load …` it.
- Turn it off:   `launchctl unload ~/Library/LaunchAgents/com.whalewatcher.snapshot.plist`
- Remove it:     unload, then delete the plist.
- Logs:          `snapshots/cron.log`, `snapshots/launchd.err.log`

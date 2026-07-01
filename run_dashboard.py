#!/usr/bin/env python3
"""
run_dashboard.py — one command to fetch fresh data and open the dashboard.

What it does:
  1. Runs the consensus pipeline (writes consensus_report.json + .md)
  2. Starts a local HTTP server (so the dashboard's fetch() of the JSON works —
     opening dashboard.html directly via file:// is blocked by the browser)
  3. Opens the dashboard in your browser
  4. Serves a small POST /api/refresh endpoint so the dashboard's "Refresh"
     button can re-run the pipeline in place — no need to touch the terminal.

Usage:
    python run_dashboard.py
    python run_dashboard.py --top-n 50 --time-period MONTH --port 8000

Why a server at all? Browsers refuse fetch() against file:// URLs for security,
so the dashboard can't read consensus_report.json from a double-clicked file.
A tiny localhost server sidesteps that. The data itself is already on disk;
nothing leaves your machine.
"""
import argparse
import functools
import http.server
import json
import shutil
import socketserver
import subprocess
import sys
import threading
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Pipeline parameters chosen at launch; the in-dashboard Refresh button reuses
# these so a refresh reproduces the same view with fresh data.
CONFIG: dict = {}
# Serialize refreshes so two clicks can't run the pipeline concurrently.
_refresh_lock = threading.Lock()


VALID_PERIODS = ("DAY", "WEEK", "MONTH", "ALL")
SNAPSHOT_DIR = HERE / "snapshots"


def archive_snapshot():
    """
    Copy the freshly-written report into snapshots/ with a UTC timestamp.

    Every run thus leaves a point-in-time record. Over days/weeks this builds the
    history the backtester needs to ask "did a high-consensus signal at time T
    actually resolve in the money?" — which is the only honest way to know whether
    any of this predicts earnings. The live dashboard only ever sees the latest
    file, so this is purely additive.
    """
    report = HERE / "consensus_report.json"
    if not report.exists():
        return
    try:
        SNAPSHOT_DIR.mkdir(exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(report, SNAPSHOT_DIR / f"consensus_{stamp}.json")
    except OSError as exc:
        print(f"  (snapshot skipped: {exc})")


def run_pipeline(top_n, time_period, order_by, min_usd, active_only=True):
    cmd = [
        sys.executable, str(HERE / "polymarket_consensus.py"),
        "--top-n", str(top_n),
        "--time-period", time_period,
        "--order-by", order_by,
        "--min-position-usd", str(min_usd),
        "--active-only" if active_only else "--no-active-only",
        "--out-json", str(HERE / "consensus_report.json"),
        "--out-md", str(HERE / "consensus_report.md"),
    ]
    print(f"→ Running pipeline: top {top_n} traders, {time_period} window...\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("\n✗ Pipeline failed. Dashboard will show the last successful report if one exists.")
        return False
    archive_snapshot()
    print("\n✓ Pipeline complete — consensus_report.json written (snapshot archived).\n")
    return True


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Static file server + a POST /api/refresh hook that re-runs the pipeline."""

    def log_message(self, *args, **kwargs):  # quiet
        pass

    def end_headers(self):
        # Never let the browser serve a stale cached page — always fetch the
        # current dashboard.html / data so new features show up on reload.
        self.send_header("Cache-Control", "no-store, max-age=0")
        super().end_headers()

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path.rstrip("/") != "/api/refresh":
            self._send_json(404, {"ok": False, "error": "not found"})
            return

        # Optional JSON body lets the dashboard's window switcher change the
        # leaderboard period (DAY/WEEK/MONTH/ALL) without relaunching the server.
        requested_period = None
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                body = json.loads(self.rfile.read(length) or b"{}")
                tp = str(body.get("time_period", "")).upper()
                if tp in VALID_PERIODS:
                    requested_period = tp
        except (ValueError, json.JSONDecodeError):
            pass  # ignore a malformed body and just refresh the current window

        # If a refresh is already running, don't pile on — tell the client.
        if not _refresh_lock.acquire(blocking=False):
            self._send_json(409, {"ok": False, "error": "A refresh is already in progress."})
            return
        try:
            if requested_period:
                CONFIG["time_period"] = requested_period  # remember for future refreshes too
            ok = run_pipeline(
                CONFIG["top_n"], CONFIG["time_period"],
                CONFIG["order_by"], CONFIG["min_position_usd"],
                CONFIG["active_only"],
            )
        finally:
            _refresh_lock.release()

        if ok:
            self._send_json(200, {"ok": True, "message": "Report refreshed.", "time_period": CONFIG["time_period"]})
        else:
            self._send_json(500, {"ok": False, "error": "Pipeline failed — see server logs. Showing last report."})


def serve(port):
    """Start the server, auto-advancing to a free port if the chosen one is busy."""
    handler = functools.partial(DashboardHandler, directory=str(HERE))
    socketserver.TCPServer.allow_reuse_address = True
    last_err = None
    for p in range(port, port + 25):
        try:
            httpd = socketserver.ThreadingTCPServer(("", p), handler)
        except OSError as e:
            last_err = e
            continue
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        return httpd, p
    raise last_err  # 25 ports all busy — extremely unlikely


def main():
    ap = argparse.ArgumentParser(description="Run the Polymarket consensus pipeline and open the dashboard.")
    ap.add_argument("--top-n", type=int, default=50)
    ap.add_argument("--time-period", choices=["DAY", "WEEK", "MONTH", "ALL"], default="MONTH")
    ap.add_argument("--order-by", choices=["PNL", "VOL"], default="PNL")
    ap.add_argument("--min-position-usd", type=float, default=1.0)
    ap.add_argument("--active-only", action=argparse.BooleanOptionalAction, default=True,
                    help="Only show games still live on the board (default). Use --no-active-only to include resolved markets.")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--no-fetch", action="store_true", help="Skip the pipeline, just open the dashboard on existing JSON")
    args = ap.parse_args()

    # Remember the launch params so the dashboard's Refresh button reuses them.
    CONFIG.update(
        top_n=args.top_n,
        time_period=args.time_period,
        order_by=args.order_by,
        min_position_usd=args.min_position_usd,
        active_only=args.active_only,
    )

    have_data = (HERE / "consensus_report.json").exists()

    # First run with no data at all: we must fetch before there's anything to show.
    if not args.no_fetch and not have_data:
        print("→ First run — fetching data, this takes ~30 seconds...\n")
        run_pipeline(args.top_n, args.time_period, args.order_by, args.min_position_usd, args.active_only)

    httpd, port = serve(args.port)
    url = f"http://localhost:{port}/dashboard.html"
    print("\n" + "=" * 56)
    print(f"  ✓ Whale Watcher is open at:  {url}")
    print("  Your browser should open automatically.")
    print("  Leave this window open while you use it; close it to stop.")
    print("=" * 56 + "\n")
    try:
        webbrowser.open(url)
    except Exception:
        pass

    # We already have data, so the page shows instantly. Pull fresh numbers in the
    # background; the page auto-updates when they land (no clicking needed).
    if not args.no_fetch and have_data:
        def _bg_refresh():
            with _refresh_lock:
                print("→ Updating to the latest data in the background...")
                run_pipeline(args.top_n, args.time_period, args.order_by,
                             args.min_position_usd, args.active_only)
                print("✓ Latest data loaded — the page will update on its own.\n")
        threading.Thread(target=_bg_refresh, daemon=True).start()

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()

"""
vegas.py — compare Polymarket prices against sportsbook (Vegas) odds.

The single best edge-spotter: if the smart money is on "Argentina to win" at 20¢
but sportsbooks imply 28%, the market may be underpricing it. This module pulls
odds from The Odds API, de-vigs them into clean probabilities, and matches them to
the consensus markets.

SETUP (one time):
  1. Get a free key at https://the-odds-api.com  (~500 calls/month).
  2. Put it in a file next to this one called  odds_api_key.txt  (just the key),
     OR set the environment variable  ODDS_API_KEY.
  3. Verify:  python vegas.py --selftest

Coverage is sports only (soccer / World Cup, etc.) — there's no Vegas line for
politics or crypto markets, which simply won't get a comparison.

Everything here fails soft: any error returns no data, so the pipeline and
dashboard keep working with the Vegas slot just showing "no line".
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time

import httpx

ODDS_BASE = "https://api.the-odds-api.com/v4"
HERE = os.path.dirname(os.path.abspath(__file__))
KEY_FILE = os.path.join(HERE, "odds_api_key.txt")

# A few name differences between Polymarket and sportsbooks.
ALIASES = {
    "usa": "united states", "us": "united states", "uk": "england",
    "south korea": "korea republic", "ivory coast": "cote d'ivoire",
}


def load_key() -> str:
    k = os.environ.get("ODDS_API_KEY", "").strip()
    if k:
        return k
    try:
        with open(KEY_FILE) as f:
            return f.read().strip()
    except OSError:
        return ""


def has_key() -> bool:
    return bool(load_key())


def _norm(name: str) -> str:
    n = re.sub(r"[^a-z ]", "", (name or "").lower()).strip()
    return ALIASES.get(n, n)


def _devig(name_to_decimal: dict[str, float]) -> dict[str, float]:
    """Decimal odds -> implied probs, normalized to remove the bookmaker margin."""
    raw = {k: (1.0 / v) for k, v in name_to_decimal.items() if v and v > 1.0}
    s = sum(raw.values())
    return {k: v / s for k, v in raw.items()} if s > 0 else {}


async def _get(client: httpx.AsyncClient, path: str, params: dict):
    p = {**params, "apiKey": load_key()}
    r = await client.get(f"{ODDS_BASE}{path}", params=p, timeout=25)
    r.raise_for_status()
    return r.json()


async def _discover_keys(client) -> tuple[list[str], list[str], list[str]]:
    """Discover usable sport keys: (WC outright-winner, WC match, politics outright)."""
    sports = await _get(client, "/sports", {"all": "true"})
    keys = [s.get("key", "") for s in sports]
    # Keep it tight to limit API usage: only the soccer World Cup, not cricket /
    # qualifiers / club / women's variants.
    winners = [k for k in keys if k == "soccer_fifa_world_cup_winner"]
    matches = [k for k in keys if k == "soccer_fifa_world_cup"]
    politics = [s["key"] for s in sports
                if s.get("group", "").lower() == "politics" or "election" in s.get("key", "").lower()]
    return winners, matches, politics


def _avg_outcomes(events: list, market_key: str) -> dict:
    """Average decimal odds per outcome name across all books, per event."""
    per_event = []
    for ev in events:
        acc: dict[str, list[float]] = {}
        for bk in ev.get("bookmakers", []):
            for mk in bk.get("markets", []):
                if mk.get("key") != market_key:
                    continue
                for oc in mk.get("outcomes", []):
                    acc.setdefault(_norm(oc.get("name", "")), []).append(float(oc.get("price", 0)))
        avg = {k: (sum(v) / len(v)) for k, v in acc.items() if v}
        per_event.append({"event": ev, "avg": avg})
    return per_event


CACHE_FILE = os.path.join(HERE, "vegas_cache.json")
CACHE_TTL = 12 * 3600   # 12h — odds barely move day-to-day, and this keeps us well
                        # under The Odds API's free 500-calls/month even with 4
                        # automated snapshots a day.


def _load_cached_tables():
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        if time.time() - data.get("ts", 0) < CACHE_TTL:
            return data.get("tables")
    except (OSError, json.JSONDecodeError, ValueError):
        pass
    return None


def _save_tables(tables):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({"ts": time.time(), "tables": tables}, f)
    except OSError:
        pass


async def _fetch_tables(client) -> dict:
    """One round of API calls -> {outright, politics, matches}. Cached by caller."""
    winner_keys, match_keys, politics_keys = await _discover_keys(client)

    async def _odds(key, market):
        try:
            return await _get(client, f"/sports/{key}/odds",
                              {"regions": "us", "markets": market, "oddsFormat": "decimal"})
        except Exception:  # one dud key shouldn't sink the rest
            return []

    outright: dict[str, float] = {}   # team -> P(win tournament)
    for key in winner_keys:
        for blk in _avg_outcomes(await _odds(key, "outrights"), "outrights"):
            outright.update(_devig(blk["avg"]))
    politics: dict[str, float] = {}   # candidate -> P(win election)
    for key in politics_keys:
        for blk in _avg_outcomes(await _odds(key, "outrights"), "outrights"):
            politics.update(_devig(blk["avg"]))
    matches = []                      # per upcoming match: team -> P(win)
    for key in match_keys:
        for blk in _avg_outcomes(await _odds(key, "h2h"), "h2h"):
            matches.append({"teams": _devig(blk["avg"]),
                            "when": blk["event"].get("commence_time", "")[:10]})
    return {"outright": outright, "politics": politics, "matches": matches}


def _match_records(records: list[dict], tables: dict) -> dict[str, float]:
    """Local-only: map each consensus market to a Vegas/candidate prob. No API."""
    outright = tables.get("outright", {})
    politics = tables.get("politics", {})
    matches = tables.get("matches", [])

    def _lookup_name(table: dict, name: str):
        if name in table:
            return table[name]
        last = name.split()[-1] if name else ""
        return next((v for k, v in table.items() if last and k.split()[-1] == last), None)

    out: dict[str, float] = {}
    for rec in records:
        title = (rec.get("market_title", "") or "").lower()
        cid = rec.get("condition_id")
        if not cid:
            continue
        # Always store the TEAM/CANDIDATE-WINS prob; the pipeline flips to 1-p for "No".
        m = re.search(r"will (.+?) win the .*world cup", title)
        if m:
            p = _lookup_name(outright, _norm(m.group(1)))
            if p is not None:
                out[cid] = round(p, 4)
            continue
        m = re.search(r"will (.+?) win the .*(?:presidential election|election|presidency)", title)
        if m:
            p = _lookup_name(politics, _norm(m.group(1)))
            if p is not None:
                out[cid] = round(p, 4)
            continue
        m = re.search(r"will (.+?) win on (\d{4}-\d{2}-\d{2})", title)
        if m:
            team, date = _norm(m.group(1)), m.group(2)
            best = next((mt["teams"][team] for mt in matches
                         if team in mt["teams"] and (not mt["when"] or mt["when"] == date)), None)
            if best is not None:
                out[cid] = round(best, 4)
    return out


async def vegas_probabilities(records: list[dict]) -> dict[str, float]:
    """
    records: dicts with condition_id, market_title, outcome.
    Returns {condition_id: vegas_implied_prob} for markets we could match.
    Odds tables are cached for CACHE_TTL to stay under the free API quota.
    """
    if not has_key() or not records:
        return {}
    tables = _load_cached_tables()
    if tables is None:
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "whale-watcher/1.0"}) as client:
                tables = await _fetch_tables(client)
            _save_tables(tables)
        except Exception as exc:  # never break the pipeline over odds
            print(f"[vegas] skipped ({exc!r})", file=sys.stderr)
            return {}
    return _match_records(records, tables)


async def _selftest():
    if not has_key():
        print("No API key found. Put it in odds_api_key.txt or set ODDS_API_KEY.")
        return
    async with httpx.AsyncClient(headers={"User-Agent": "whale-watcher/1.0"}) as client:
        winners, matches, politics = await _discover_keys(client)
        print(f"Sport keys found:\n  WC winner={winners}\n  WC matches={matches}\n  politics={politics}")
        sample = [
            {"condition_id": "x1", "market_title": "Will Argentina win the 2026 FIFA World Cup?", "outcome": "Yes"},
            {"condition_id": "x2", "market_title": "Will Brazil win the 2026 FIFA World Cup?", "outcome": "Yes"},
            {"condition_id": "x3", "market_title": "Will JD Vance win the 2028 US Presidential Election?", "outcome": "Yes"},
            {"condition_id": "x4", "market_title": "Will Gavin Newsom win the 2028 US Presidential Election?", "outcome": "Yes"},
        ]
    probs = await vegas_probabilities(sample)
    if probs:
        print("Sample matched Vegas probabilities:")
        for cid, p in probs.items():
            print(f"   {cid}: {p*100:.1f}%")
        print("\n✓ Working — re-run the pipeline and the dashboard's Vegas slot will fill in.")
    else:
        print("Key works but no sample matched (tournament may be between rounds, or key names differ).")


def main():
    ap = argparse.ArgumentParser(description="Vegas/sportsbook odds comparison.")
    ap.add_argument("--selftest", action="store_true", help="Verify the API key and matching.")
    args = ap.parse_args()
    if args.selftest:
        asyncio.run(_selftest())
    else:
        print("Use --selftest to verify, or import vegas_probabilities() from the pipeline.")


if __name__ == "__main__":
    main()

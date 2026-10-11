"""
Stage 1 - Ingestion
-------------------
Pulls FC Barcelona match data from StatsBomb's free open dataset
and saves it as raw JSON (the "bronze" / raw layer of a data lake).

Rule of the raw layer: save data EXACTLY as the source gave it.
No cleaning here - that happens in later stages.

Run:  python src/ingest.py --season 2020/2021
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
TEAM = "Barcelona"
COMPETITION = "La Liga"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("ingest")


def fetch_json(url: str, retries: int = 3):
    """GET a URL and return parsed JSON, retrying on network errors."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as err:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, retries, url, err)
            if attempt == retries:
                raise
            time.sleep(2 * attempt)  # simple backoff: wait 2s, then 4s


def save_json(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def find_season(competitions, season_name: str):
    """Look up the competition_id and season_id for La Liga + the given season."""
    for c in competitions:
        if c["competition_name"] == COMPETITION and c["season_name"] == season_name:
            return c["competition_id"], c["season_id"]
    available = sorted(c["season_name"] for c in competitions if c["competition_name"] == COMPETITION)
    raise ValueError(f"Season {season_name} not found. Available: {available}")


def is_barca_match(match) -> bool:
    return TEAM in (match["home_team"]["home_team_name"], match["away_team"]["away_team_name"])


def run(season_name: str, with_events: bool):
    # Partition by season + ingestion date, like a real data lake:
    # data/raw/season=2020_2021/ingest_date=2026-10-11/...
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = RAW_DIR / f"season={season_name.replace('/', '_')}" / f"ingest_date={ingest_date}"

    log.info("Fetching competitions list")
    competitions = fetch_json(f"{BASE_URL}/competitions.json")
    comp_id, season_id = find_season(competitions, season_name)
    log.info("%s %s -> competition_id=%s, season_id=%s", COMPETITION, season_name, comp_id, season_id)

    matches = fetch_json(f"{BASE_URL}/matches/{comp_id}/{season_id}.json")
    barca_matches = [m for m in matches if is_barca_match(m)]
    log.info("Found %d matches, %d involve %s", len(matches), len(barca_matches), TEAM)
    save_json(barca_matches, out_dir / "matches.json")

    events_saved = 0
    if with_events:
        for m in barca_matches:
            events = fetch_json(f"{BASE_URL}/events/{m['match_id']}.json")
            save_json(events, out_dir / "events" / f"{m['match_id']}.json")
            events_saved += 1
        log.info("Saved event files for %d matches", events_saved)

    # A small manifest so later stages know what this run produced
    save_json(
        {
            "source": "statsbomb-open-data",
            "competition": COMPETITION,
            "season": season_name,
            "team": TEAM,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "match_count": len(barca_matches),
            "event_files": events_saved,
        },
        out_dir / "_manifest.json",
    )
    log.info("Done. Raw data written to %s", out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FC Barcelona match data (raw layer)")
    parser.add_argument("--season", default="2020/2021", help="e.g. 2020/2021")
    parser.add_argument("--no-events", action="store_true", help="skip per-match event files")
    args = parser.parse_args()
    run(args.season, with_events=not args.no_events)

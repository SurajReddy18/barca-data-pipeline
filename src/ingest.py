"""
Stage 1 - Ingestion
-------------------
Pulls La Liga match data for one team from StatsBomb's free open dataset
and saves it as raw JSON (the "bronze" / raw layer of a data lake).

Rule of the raw layer: save data EXACTLY as the source gave it.
No cleaning here - that happens in later stages.

Run:  python src/ingest.py --season 2020/2021 --team "Barcelona"
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
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


def is_team_match(match, team: str) -> bool:
    """True if the given team played in this match (home or away)."""
    return team in (match["home_team"]["home_team_name"], match["away_team"]["away_team_name"])


def run(season_name: str, team: str, with_events: bool):
    # Partition by season + team + ingestion date, like a real data lake:
    # data/raw/season=2020_2021/team=barcelona/ingest_date=2026-10-11/...
    # (team is in the path so a Real Madrid run never overwrites Barcelona's files)
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    team_slug = team.lower().replace(" ", "_")
    out_dir = (
        RAW_DIR
        / f"season={season_name.replace('/', '_')}"
        / f"team={team_slug}"
        / f"ingest_date={ingest_date}"
    )

    log.info("Fetching competitions list")
    competitions = fetch_json(f"{BASE_URL}/competitions.json")
    comp_id, season_id = find_season(competitions, season_name)
    log.info("%s %s -> competition_id=%s, season_id=%s", COMPETITION, season_name, comp_id, season_id)

    matches = fetch_json(f"{BASE_URL}/matches/{comp_id}/{season_id}.json")
    team_matches = [m for m in matches if is_team_match(m, team)]
    log.info("Found %d matches, %d involve %s", len(matches), len(team_matches), team)
    if not team_matches:
        log.warning("No matches for '%s'. Check the exact spelling used in the data.", team)
    save_json(team_matches, out_dir / "matches.json")

    events_saved = 0
    if with_events:
        for m in team_matches:
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
            "team": team,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "match_count": len(team_matches),
            "event_files": events_saved,
        },
        out_dir / "_manifest.json",
    )
    log.info("Done. Raw data written to %s", out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest La Liga match data for a team (raw layer)")
    parser.add_argument("--season", default="2020/2021", help="e.g. 2020/2021")
    parser.add_argument("--team", default="Barcelona", help="e.g. Real Madrid")
    parser.add_argument("--no-events", action="store_true", help="skip per-match event files")
    args = parser.parse_args()
    run(args.season, args.team, with_events=not args.no_events)

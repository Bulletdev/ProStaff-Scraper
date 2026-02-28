#!/usr/bin/env python3
"""
Historical Backfill Pipeline

Imports ALL games for every edition of a league (e.g. CBLOL since 2013)
from Leaguepedia. The pipeline is fully resumable: progress is persisted to
a JSON file so it can be interrupted and restarted without re-processing
already-completed tournaments.

Flow:
  1. Discover all tournament OverviewPages via Leaguepedia Tournaments table
  2. For each tournament not yet completed, run LeaguepediaPipeline
  3. Persist progress to data/backfill_{league}.json after every tournament

Estimated time:
  CBLOL (~30 tournaments, ~60 games each):
    60 games × 12s/game × 30 tournaments ≈ 6 hours
  (runs as a background task — do not wait synchronously)

Usage:
    python etl/historical_backfill.py --league CBLOL
    python etl/historical_backfill.py --league CBLOL --dry-run
    python etl/historical_backfill.py --league CBLOL --status
    python etl/historical_backfill.py --league CBLOL --min-year 2022
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from providers.leaguepedia import get_league_tournaments, RATE_LIMIT_SECONDS
from etl.leaguepedia_pipeline import LeaguepediaPipeline

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/historical_backfill.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tournament filters
# ---------------------------------------------------------------------------

# Leaguepedia has qualification/academy/all-star pages under the same prefix.
# These keywords in the OverviewPage indicate non-main-event pages to skip.
_SKIP_KEYWORDS = [
    "Qualifier",
    "qualifier",
    "All-Star",
    "All Star",
    "Tiebreaker",
    "Promotion",
    "Relegation",
    "Academy",
    "Showmatch",
    "Chrono",
    "Boost",
]

# Only include pages that have exactly two "/" (format: "LEAGUE/YYYY Season/EVENT")
# Deeply nested pages (e.g. "CBLOL/2026 Season/Cup/Qualifier") are sub-events.
_EXPECTED_SLASH_COUNT = 2


def _is_main_event(overview_page: str) -> bool:
    """Return True if the OverviewPage looks like a main competitive event."""
    slash_count = overview_page.count("/")
    if slash_count != _EXPECTED_SLASH_COUNT:
        return False
    for kw in _SKIP_KEYWORDS:
        if kw in overview_page:
            return False
    return True


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

TOURNAMENT_STATUS_PENDING = "pending"
TOURNAMENT_STATUS_IN_PROGRESS = "in_progress"
TOURNAMENT_STATUS_COMPLETED = "completed"
TOURNAMENT_STATUS_SKIPPED = "skipped"
TOURNAMENT_STATUS_ERROR = "error"

# Max retries for a tournament that returns 0 games (avoids infinite loop
# for future/genuinely-empty tournaments that have nothing in Leaguepedia yet)
MAX_FETCH_RETRIES = 3


def _progress_path(league: str) -> str:
    return os.path.join("data", f"backfill_{league.upper()}.json")


def _load_progress(league: str) -> Dict:
    path = _progress_path(league)
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read progress file {path}: {e}")
    return {}


def _save_progress(league: str, state: Dict) -> None:
    path = _progress_path(league)
    state["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    try:
        with open(path, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Could not save progress to {path}: {e}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class HistoricalBackfillPipeline:
    """
    Orchestrates full historical import for a league.

    The pipeline discovers all tournaments, filters to main events, then
    runs LeaguepediaPipeline for each one that hasn't been completed yet.
    Progress is written to data/backfill_{LEAGUE}.json after every tournament.
    """

    def __init__(self, league: str, dry_run: bool = False, min_year: int = 2013):
        self.league = league.upper()
        self.dry_run = dry_run
        self.min_year = min_year

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_tournaments(self) -> List[Dict]:
        """
        Query Leaguepedia for all tournament OverviewPages for this league,
        filter to main competitive events, and return sorted by date.
        """
        raw = get_league_tournaments(self.league, min_year=self.min_year)

        filtered = [t for t in raw if _is_main_event(t["overview_page"])]

        logger.info(
            f"Discovered {len(raw)} total pages, "
            f"{len(filtered)} main events after filtering"
        )
        return filtered

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> Dict:
        """Return current progress state from the progress file."""
        state = _load_progress(self.league)
        if not state:
            return {
                "league": self.league,
                "status": "not_started",
                "tournaments": [],
            }

        tournaments = state.get("tournaments", [])
        counts = {
            TOURNAMENT_STATUS_COMPLETED: 0,
            TOURNAMENT_STATUS_PENDING: 0,
            TOURNAMENT_STATUS_IN_PROGRESS: 0,
            TOURNAMENT_STATUS_ERROR: 0,
            TOURNAMENT_STATUS_SKIPPED: 0,
        }
        for t in tournaments:
            s = t.get("status", TOURNAMENT_STATUS_PENDING)
            counts[s] = counts.get(s, 0) + 1

        return {
            "league": self.league,
            "started_at": state.get("started_at"),
            "updated_at": state.get("updated_at"),
            "total_tournaments": len(tournaments),
            "completed": counts[TOURNAMENT_STATUS_COMPLETED],
            "pending": counts[TOURNAMENT_STATUS_PENDING],
            "in_progress": counts[TOURNAMENT_STATUS_IN_PROGRESS],
            "errors": counts[TOURNAMENT_STATUS_ERROR],
            "skipped": counts[TOURNAMENT_STATUS_SKIPPED],
            "total_games_indexed": sum(
                t.get("games_indexed", 0) for t in tournaments
            ),
            # Show how many are still actionable (pending + in_progress + retryable errors)
            "remaining": sum(
                1 for t in tournaments
                if t["status"] in (
                    TOURNAMENT_STATUS_PENDING,
                    TOURNAMENT_STATUS_IN_PROGRESS,
                    TOURNAMENT_STATUS_ERROR,
                ) and t.get("fetch_retries", 0) < MAX_FETCH_RETRIES
            ),
            "tournaments": tournaments,
        }

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self) -> Dict:
        """
        Execute the full historical backfill.

        Loads any existing progress, discovers tournaments (or reuses the
        cached list), then processes each pending tournament sequentially.
        Returns the final status dict.
        """
        logger.info("=" * 70)
        logger.info(f"Historical Backfill: {self.league} (min_year={self.min_year})")
        logger.info(f"Dry-run: {self.dry_run}")
        logger.info("=" * 70)

        state = _load_progress(self.league)

        # First run: populate tournament list from Leaguepedia
        if not state.get("tournaments"):
            logger.info("No existing progress — discovering tournaments...")
            tournaments = self.discover_tournaments()

            if not tournaments:
                logger.warning(f"No main-event tournaments found for '{self.league}'")
                return {"league": self.league, "status": "no_tournaments_found"}

            state = {
                "league": self.league,
                "started_at": datetime.now(tz=timezone.utc).isoformat(),
                "min_year": self.min_year,
                "tournaments": [
                    {
                        "overview_page": t["overview_page"],
                        "name": t["name"],
                        "date_start": t["date_start"],
                        "date_end": t["date_end"],
                        "year": t["year"],
                        "status": TOURNAMENT_STATUS_PENDING,
                        "games_indexed": 0,
                        "games_skipped": 0,
                        "errors": 0,
                        "started_at": None,
                        "completed_at": None,
                        "error_message": None,
                    }
                    for t in tournaments
                ],
            }
            _save_progress(self.league, state)
            logger.info(f"Saved tournament list ({len(tournaments)} entries) to progress file")

        pending = [
            t for t in state["tournaments"]
            if t["status"] in (
                TOURNAMENT_STATUS_PENDING,
                TOURNAMENT_STATUS_IN_PROGRESS,
                TOURNAMENT_STATUS_ERROR,  # Retry errored tournaments (e.g. rate-limit failures)
            )
        ]

        logger.info(
            f"Tournaments to process: {len(pending)} "
            f"(of {len(state['tournaments'])} total)"
        )

        if not pending:
            logger.info("All tournaments already completed. Nothing to do.")
            return self.get_status()

        for idx, entry in enumerate(state["tournaments"]):
            if entry["status"] not in (
                TOURNAMENT_STATUS_PENDING,
                TOURNAMENT_STATUS_IN_PROGRESS,
                TOURNAMENT_STATUS_ERROR,
            ):
                continue

            # Skip tournaments that exhausted retries with 0 games fetched
            fetch_retries = entry.get("fetch_retries", 0)
            if entry["status"] == TOURNAMENT_STATUS_ERROR and fetch_retries >= MAX_FETCH_RETRIES:
                logger.info(
                    f"  [{idx+1}] Skipping '{entry['overview_page']}' — "
                    f"exhausted {MAX_FETCH_RETRIES} fetch retries with 0 games"
                )
                entry["status"] = TOURNAMENT_STATUS_SKIPPED
                _save_progress(self.league, state)
                continue

            overview_page = entry["overview_page"]
            logger.info(
                f"\n[{idx + 1}/{len(state['tournaments'])}] "
                f"{overview_page} ({entry.get('date_start', '')[:4]})"
            )

            entry["status"] = TOURNAMENT_STATUS_IN_PROGRESS
            entry["started_at"] = datetime.now(tz=timezone.utc).isoformat()
            _save_progress(self.league, state)

            if self.dry_run:
                logger.info("  (dry-run) Would run LeaguepediaPipeline for this tournament")
                entry["status"] = TOURNAMENT_STATUS_COMPLETED
                entry["completed_at"] = datetime.now(tz=timezone.utc).isoformat()
                _save_progress(self.league, state)
                continue

            try:
                pipeline = LeaguepediaPipeline(dry_run=False)
                pipeline.run(overview_page)

                fetched = pipeline.stats.get("fetched", 0)

                if fetched == 0:
                    # Either a rate-limit failure or a genuinely empty tournament.
                    # Mark as ERROR so the next run will retry, up to MAX_FETCH_RETRIES.
                    entry["status"] = TOURNAMENT_STATUS_ERROR
                    entry["fetch_retries"] = entry.get("fetch_retries", 0) + 1
                    entry["error_message"] = (
                        f"Zero games fetched from Leaguepedia "
                        f"(attempt {entry['fetch_retries']}/{MAX_FETCH_RETRIES}). "
                        "Possible rate-limit or tournament not yet in Leaguepedia."
                    )
                    entry["completed_at"] = datetime.now(tz=timezone.utc).isoformat()
                    logger.warning(
                        f"  No games fetched for '{overview_page}' "
                        f"(retry {entry['fetch_retries']}/{MAX_FETCH_RETRIES})"
                    )
                else:
                    entry["status"] = TOURNAMENT_STATUS_COMPLETED
                    entry["games_indexed"] = pipeline.stats.get("indexed", 0)
                    entry["games_skipped"] = pipeline.stats.get("skipped_exists", 0)
                    entry["errors"] = pipeline.stats.get("errors", 0)
                    entry["completed_at"] = datetime.now(tz=timezone.utc).isoformat()
                    logger.info(
                        f"  Completed: {entry['games_indexed']} indexed, "
                        f"{entry['games_skipped']} already existed, "
                        f"{entry['errors']} errors"
                    )

            except Exception as e:
                logger.error(f"  Tournament failed: {e}", exc_info=True)
                entry["status"] = TOURNAMENT_STATUS_ERROR
                entry["fetch_retries"] = entry.get("fetch_retries", 0)
                entry["error_message"] = str(e)
                entry["completed_at"] = datetime.now(tz=timezone.utc).isoformat()

            _save_progress(self.league, state)

            # Brief pause between tournaments (Leaguepedia rate limit courtesy)
            remaining = [
                t for t in state["tournaments"]
                if t["status"] == TOURNAMENT_STATUS_PENDING
            ]
            if remaining:
                logger.info(f"  Cooling down {RATE_LIMIT_SECONDS}s before next tournament...")
                time.sleep(RATE_LIMIT_SECONDS)

        final = self.get_status()
        logger.info("=" * 70)
        logger.info(f"Backfill complete: {final['completed']} done, {final['errors']} errors")
        logger.info("=" * 70)
        return final


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Historical backfill: import all editions of a league from Leaguepedia"
    )
    parser.add_argument(
        "--league",
        default="CBLOL",
        help="League prefix on Leaguepedia OverviewPage (e.g. CBLOL, LCS)",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=2013,
        help="Ignore tournaments before this year (default: 2013)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and list tournaments without actually importing games",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print current progress for this league and exit",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete progress file and start from scratch",
    )
    args = parser.parse_args()

    pipeline = HistoricalBackfillPipeline(
        league=args.league,
        dry_run=args.dry_run,
        min_year=args.min_year,
    )

    if args.status:
        status = pipeline.get_status()
        print(json.dumps(status, indent=2, ensure_ascii=False))
        return

    if args.reset:
        path = _progress_path(args.league)
        if os.path.exists(path):
            os.remove(path)
            print(f"Deleted progress file: {path}")
        else:
            print(f"No progress file found at: {path}")

    pipeline.run()


if __name__ == "__main__":
    main()

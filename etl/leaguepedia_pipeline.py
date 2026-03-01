#!/usr/bin/env python3
"""
Leaguepedia-Native Tournament Pipeline

Imports ALL games from a specific tournament by querying the Leaguepedia
Cargo tables directly. This is the authoritative source for historical data
and bypasses the LoL Esports API which only exposes a rolling window of
recent events globally.

The LoL Esports getCompletedEvents endpoint covers ~300 events across all
leagues. When major leagues (LCK/LPL) are active daily, older regional games
(CBLOL regular season) are pushed out of the window within weeks.

This pipeline queries ScoreboardGames by OverviewPage to get the full
tournament history and is the correct approach for bulk historical import.

Usage:
    python etl/leaguepedia_pipeline.py --tournament "CBLOL/2026 Season/Cup"
    python etl/leaguepedia_pipeline.py --tournament "CBLOL/2026 Season/Cup" --dry-run
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional, Dict, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from providers.leaguepedia import (
    get_game_players,
    RATE_LIMIT_SECONDS,
    BACKFILL_COOLDOWN_SECONDS,
    LeaguepediaRateLimitError,
    _cargo_query,
)
from indexers.elasticsearch_client import ensure_index, get_client
from indexers.mappings import MATCHES_MAPPING

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/leaguepedia_pipeline.log'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

INDEX = "lol_pro_matches"


# ---------------------------------------------------------------------------
# Leaguepedia queries
# ---------------------------------------------------------------------------

def fetch_tournament_games(overview_page: str) -> List[Dict]:
    """Fetch all game records for a tournament from ScoreboardGames.

    Args:
        overview_page: Leaguepedia overview page (e.g. "CBLOL/2026 Season/Cup")

    Returns:
        List of row dicts from the Cargo API, sorted by DateTime_UTC ascending.

    Raises:
        LeaguepediaRateLimitError: if rate limited after all retries.
            Callers (e.g. historical_backfill) should catch this and schedule
            a longer cooldown before retrying the tournament.
    """
    escaped = overview_page.replace("'", "\\'")
    logger.info(f"Querying ScoreboardGames for OverviewPage='{overview_page}'...")

    all_rows: List[Dict] = []
    offset = 0
    page_size = 100

    while True:
        try:
            data = _cargo_query({
                "tables": "ScoreboardGames",
                "fields": (
                    "GameId,OverviewPage,WinTeam,Team1,Team2,"
                    "Patch,Gamelength,DateTime_UTC,N_GameInMatch,"
                    "Team1Score,Team2Score"
                ),
                "where": f"OverviewPage='{escaped}'",
                "limit": str(page_size),
                "offset": str(offset),
                "order_by": "DateTime_UTC ASC",
            })
        except LeaguepediaRateLimitError:
            logger.error(
                f"Rate limited while fetching games for '{overview_page}' "
                f"(offset={offset}). Propagating to caller for longer cooldown."
            )
            raise
        except Exception as e:
            logger.error(f"ScoreboardGames query failed at offset {offset}: {e}")
            break

        rows = data.get("cargoquery", [])
        if not rows:
            break

        all_rows.extend([r.get("title", {}) for r in rows])
        logger.info(f"  Fetched {len(rows)} rows (total so far: {len(all_rows)})")

        if len(rows) < page_size:
            break

        offset += page_size
        time.sleep(RATE_LIMIT_SECONDS)

    logger.info(f"Total games found for '{overview_page}': {len(all_rows)}")
    return all_rows


# ---------------------------------------------------------------------------
# Document builder
# ---------------------------------------------------------------------------

def _parse_stage(game_id: str, overview_page: str) -> str:
    """Extract stage name from Leaguepedia GameId.

    GameId format: "{OverviewPage}_{Stage}_{MatchNum}_{GameNum}"
    Examples:
      "CBLOL/2026 Season/Cup_Week 1_1_1"  -> "Week 1"
      "CBLOL/2026 Season/Cup_Playoffs_2_3" -> "Playoffs"
      "CBLOL/2026 Season/Cup_Play-In Round 1_1_2" -> "Play-In Round 1"
    """
    prefix = overview_page + "_"
    if game_id.startswith(prefix):
        remainder = game_id[len(prefix):]
        # remainder = "Week 1_1_1" or "Playoffs_2_3"
        # Remove last two "_N" segments
        parts = remainder.rsplit("_", 2)
        if len(parts) == 3:
            return parts[0]
        return remainder
    return "Unknown"


def _infer_best_of(stage: str) -> int:
    """Infer best-of format from stage name."""
    s = stage.lower()
    if any(w in s for w in ("week", "regular", "group", "swiss")):
        return 1
    if any(w in s for w in ("final", "semifinal", "quarter")):
        return 5
    return 3


def _parse_gamelength_seconds(gamelength: str) -> int:
    """Convert 'MM:SS' to total seconds."""
    if not gamelength:
        return 0
    try:
        parts = gamelength.strip().split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        pass
    return 0


def _parse_overview_page(overview_page: str) -> Dict:
    """Extract structured fields from a Leaguepedia OverviewPage path.

    Format: "LEAGUE/YYYY Season/EVENT"
    Examples:
      "CBLOL/2023 Season/Split 1"   -> year=2023, split_event="Split 1",
                                        tournament_name="2023 Season Split 1"
      "CBLOL/2026 Season/Cup"       -> year=2026, split_event="Cup",
                                        tournament_name="2026 Season Cup"
    """
    parts = overview_page.split("/")
    if len(parts) >= 3:
        season_part = parts[1]   # e.g. "2023 Season"
        split_event = parts[2]   # e.g. "Split 1", "Playoffs", "Cup"
        # Extract the numeric year from the season part
        try:
            year = int(season_part.split()[0])
        except (ValueError, IndexError):
            year = 0
        tournament_name = f"{season_part} {split_event}"
    elif len(parts) == 2:
        season_part = parts[1]
        split_event = parts[1]
        try:
            year = int(season_part.split()[0])
        except (ValueError, IndexError):
            year = 0
        tournament_name = season_part
    else:
        year = 0
        split_event = ""
        tournament_name = overview_page

    return {
        "year": year,
        "split_event": split_event,
        "tournament_name": tournament_name,
    }


def build_es_document(row: Dict, players: List[Dict], league_override: Optional[str] = None) -> Dict:
    """Build an Elasticsearch document from Leaguepedia data.

    The document format mirrors CompetitiveGame.to_dict() so that the
    existing ScraperImporterService in the Rails API can process it.

    Args:
        row:              ScoreboardGames row dict from Leaguepedia.
        players:          List of player dicts from ScoreboardPlayers.
        league_override:  When provided, overrides the league field instead of
                          extracting it from the OverviewPage prefix.  Used by
                          the historical backfill to normalize league aliases
                          (e.g. "LTA Sul" -> "CBLOL") so all documents for a
                          rebranded league use a consistent label.
    """
    game_id_lp = row.get("GameId", "")
    overview_page = row.get("OverviewPage", "")
    win_team = row.get("WinTeam", "")
    team1_name = row.get("Team1", "")
    team2_name = row.get("Team2", "")
    patch = row.get("Patch", "")
    gamelength = row.get("Gamelength", "")
    datetime_utc = row.get("DateTime UTC", "")
    game_number = int(row.get("N GameInMatch", 1) or 1)

    # Annotate players with win flag
    for player in players:
        player["win"] = (player.get("team_name") == win_team)

    stage = _parse_stage(game_id_lp, overview_page)
    best_of = _infer_best_of(stage)
    overview_meta = _parse_overview_page(overview_page)

    # Determine league label: prefer explicit override, fall back to OverviewPage prefix
    league_label = league_override or (
        overview_page.split("/")[0] if "/" in overview_page else overview_page
    )

    # Build team structures matching LoL Esports API format
    team1 = {
        "name": team1_name,
        "code": team1_name[:3].upper(),
        "image": None,
        "game_wins": int(row.get("Team1Score") or 0),
    }
    team2 = {
        "name": team2_name,
        "code": team2_name[:3].upper(),
        "image": None,
        "game_wins": int(row.get("Team2Score") or 0),
    }

    # Use the Leaguepedia GameId as the ES document ID
    # The Rails importer uses _id as external_match_id
    doc = {
        "_id": game_id_lp,
        # Fields mirroring CompetitiveGame
        "match_id": game_id_lp,            # No LoL Esports ID available
        "game_id": None,
        "game_number": game_number,
        "league": league_label,
        "stage": stage,
        "start_time": datetime_utc.replace(" ", "T") + "Z" if datetime_utc else "",
        "best_of": best_of,
        "team1": team1,
        "team2": team2,
        "winner_code": win_team,
        "vod_youtube_id": None,
        # Enrichment data (already populated from Leaguepedia)
        "riot_match_id": None,
        "game_duration_seconds": _parse_gamelength_seconds(gamelength),
        "patch": patch,
        "participants": players,
        "riot_enriched": True,
        # Extra Leaguepedia fields
        "leaguepedia_page": game_id_lp,
        "win_team": win_team,
        "gamelength": gamelength,
        "enrichment_source": "leaguepedia_pipeline",
        "enrichment_attempts": 0,
        "indexed_at": datetime.now(tz=timezone.utc).isoformat(),
        "source": "leaguepedia_pipeline",
        # Historical metadata — enables filtering by year/split in the API
        "year": overview_meta["year"],
        "split_event": overview_meta["split_event"],
        "tournament_name": overview_meta["tournament_name"],
    }
    return doc


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class LeaguepediaPipeline:
    """Imports all games from a Leaguepedia tournament page."""

    CHECKPOINT_SIZE = 10  # Index to ES every N enriched games (crash-safe)

    def __init__(self, dry_run: bool = False, league_override: Optional[str] = None):
        self.dry_run = dry_run
        self.league_override = league_override
        self.stats = {
            "fetched": 0,
            "enriched": 0,
            "indexed": 0,
            "skipped_no_players": 0,
            "skipped_exists": 0,
            "errors": 0,
        }

    def run(self, tournament_overview_page: str):
        """Import all games for the given tournament.

        Args:
            tournament_overview_page: e.g. "CBLOL/2026 Season/Cup"

        Raises:
            LeaguepediaRateLimitError: if rate limited during game list fetch.
                The caller (historical_backfill) should catch this and apply
                a longer cooldown before retrying.
        """
        logger.info("=" * 70)
        logger.info(f"Leaguepedia Pipeline: {tournament_overview_page}")
        logger.info(f"Dry-run: {self.dry_run}")
        logger.info("=" * 70)

        # Step 1: Get all game records from ScoreboardGames
        # LeaguepediaRateLimitError is intentionally NOT caught here —
        # it propagates to the caller so the backfill can apply a longer
        # cooldown and retry the entire tournament later.
        game_rows = fetch_tournament_games(tournament_overview_page)
        if not game_rows:
            logger.warning(f"No games found for '{tournament_overview_page}'")
            return

        self.stats["fetched"] = len(game_rows)
        logger.info(f"Found {len(game_rows)} games to import")

        if not self.dry_run:
            es = get_client()
            ensure_index(INDEX, MATCHES_MAPPING)

        # Step 2: For each game, fetch players and build document
        docs_to_index = []
        for i, row in enumerate(game_rows):
            game_id_lp = row.get("GameId", "")
            if not game_id_lp:
                logger.warning(f"Row {i} has no GameId, skipping")
                continue

            # Check if already indexed (skip if riot_enriched=true)
            if not self.dry_run:
                try:
                    existing = es.get(index=INDEX, id=game_id_lp, ignore=404)
                    if existing.get("found") and existing["_source"].get("riot_enriched"):
                        logger.debug(f"Already enriched: {game_id_lp}")
                        self.stats["skipped_exists"] += 1
                        continue
                except Exception:
                    pass

            logger.info(
                f"[{i+1}/{len(game_rows)}] {game_id_lp} | "
                f"{row.get('Team1')} vs {row.get('Team2')} "
                f"G{row.get('N GameInMatch')} @ {row.get('DateTime UTC', '')[:10]}"
            )

            if self.dry_run:
                logger.info("  (dry-run, skipping player fetch)")
                self.stats["enriched"] += 1
                continue

            # Fetch player stats from ScoreboardPlayers
            time.sleep(RATE_LIMIT_SECONDS)
            try:
                players = get_game_players(game_id_lp)
            except LeaguepediaRateLimitError:
                # Rate limited during player fetch — propagate so backfill
                # can cooldown and retry.  Already-indexed docs from this
                # tournament are safe (checkpoint saves were made).
                logger.error(
                    f"  Rate limited fetching players for {game_id_lp}. "
                    f"Propagating to caller."
                )
                # Flush any pending docs before propagating
                if docs_to_index:
                    self._bulk_index(es, docs_to_index)
                raise
            except Exception as e:
                logger.error(f"  Player fetch failed: {e}")
                self.stats["errors"] += 1
                continue

            if not players:
                logger.warning(f"  No players returned for {game_id_lp}")
                self.stats["skipped_no_players"] += 1
                continue

            doc = build_es_document(row, players, league_override=self.league_override)
            docs_to_index.append(doc)
            self.stats["enriched"] += 1

            logger.info(
                f"  Enriched: {len(players)} players | "
                f"winner={row.get('WinTeam')} | patch={row.get('Patch')}"
            )

            # Checkpoint: index every CHECKPOINT_SIZE games so progress is saved
            if len(docs_to_index) >= self.CHECKPOINT_SIZE:
                logger.info(f"  Checkpoint: indexing {len(docs_to_index)} docs...")
                self._bulk_index(es, docs_to_index)
                docs_to_index = []

            # Rate limit between games
            if i < len(game_rows) - 1:
                time.sleep(RATE_LIMIT_SECONDS)

        # Step 3: Bulk index remaining enriched documents
        if not self.dry_run and docs_to_index:
            self._bulk_index(es, docs_to_index)

        self._print_stats()

    def _bulk_index(self, es, docs: List[Dict]):
        """Bulk index documents to Elasticsearch."""
        from elasticsearch.helpers import bulk

        actions = []
        for doc in docs:
            doc_id = doc.pop("_id")
            actions.append({
                "_index": INDEX,
                "_id": doc_id,
                "_source": doc,
            })

        try:
            success, errors = bulk(es, actions)
            self.stats["indexed"] += success
            if errors:
                logger.error(f"Bulk index errors: {errors}")
                self.stats["errors"] += len(errors)
            else:
                logger.info(f"Indexed {success} documents to Elasticsearch")
        except Exception as e:
            logger.error(f"Bulk index failed: {e}")
            self.stats["errors"] += 1

    def _print_stats(self):
        logger.info("=" * 70)
        logger.info("Leaguepedia Pipeline — Final Stats")
        logger.info(f"  Fetched from Leaguepedia : {self.stats['fetched']}")
        logger.info(f"  Enriched (with players) : {self.stats['enriched']}")
        logger.info(f"  Indexed to ES           : {self.stats['indexed']}")
        logger.info(f"  Skipped (already in ES) : {self.stats['skipped_exists']}")
        logger.info(f"  Skipped (no players)    : {self.stats['skipped_no_players']}")
        logger.info(f"  Errors                  : {self.stats['errors']}")
        logger.info("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Import all games from a Leaguepedia tournament page"
    )
    parser.add_argument(
        "--tournament",
        required=True,
        help='Leaguepedia OverviewPage, e.g. "CBLOL/2026 Season/Cup"',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch game list from Leaguepedia but do not index to Elasticsearch",
    )
    args = parser.parse_args()

    pipeline = LeaguepediaPipeline(dry_run=args.dry_run)
    pipeline.run(args.tournament)


if __name__ == "__main__":
    main()

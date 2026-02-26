#!/usr/bin/env python3
"""
Competitive Matches ETL Pipeline

Extracts professional League of Legends match data from the LoL Esports API
and stores it in Elasticsearch for meta intelligence analysis.

Data flow:
  getCompletedEvents (LoL Esports) -> series + game metadata
  Optional: Riot Match-V5 API -> per-game stats (items, runes, KDA)
    NOTE: Riot enrichment is currently BLOCKED. LoL Esports internal game IDs
    (18-digit snowflake) do not map to Riot Match-V5 IDs through any public API.
    Items/runes data requires an alternative source (Leaguepedia, GRID API, etc.)
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Optional, Set
from pathlib import Path
import signal
from dataclasses import dataclass, field, asdict
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from providers.esports import get_leagues, get_completed_events
from indexers.elasticsearch_client import ensure_index, bulk_index, get_client
from indexers.mappings import MATCHES_MAPPING

# Ensure log directory exists before configuring file handler
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/competitive_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Professional leagues configuration
COMPETITIVE_LEAGUES = {
    # Tier 1 - Major Regions
    'CBLOL': {'region': 'BR', 'tier': 1, 'priority': 'high'},
    'LCS': {'region': 'NA', 'tier': 1, 'priority': 'high'},
    'LEC': {'region': 'EU', 'tier': 1, 'priority': 'high'},
    'LCK': {'region': 'KR', 'tier': 1, 'priority': 'high'},
    'LPL': {'region': 'CN', 'tier': 1, 'priority': 'high'},
    # Tier 2 - Minor Regions
    'VCS': {'region': 'VN', 'tier': 2, 'priority': 'medium'},
    'PCS': {'region': 'TW', 'tier': 2, 'priority': 'medium'},
    'LJL': {'region': 'JP', 'tier': 2, 'priority': 'medium'},
    'LLA': {'region': 'LA', 'tier': 2, 'priority': 'medium'},
    'TCL': {'region': 'TR', 'tier': 2, 'priority': 'medium'},
    'LCO': {'region': 'OC', 'tier': 2, 'priority': 'medium'},
    # Academy
    'CBLOL Academy': {'region': 'BR', 'tier': 3, 'priority': 'low'},
    'LCS Academy': {'region': 'NA', 'tier': 3, 'priority': 'low'},
    # International
    'Worlds': {'region': 'INTL', 'tier': 0, 'priority': 'critical'},
    'MSI': {'region': 'INTL', 'tier': 0, 'priority': 'critical'},
}

REGION_TO_PLATFORM = {
    'BR': 'BR1', 'NA': 'NA1', 'EU': 'EUW1', 'KR': 'KR',
    'CN': 'CN1', 'VN': 'VN2', 'TW': 'TW2', 'JP': 'JP1',
    'LA': 'LA1', 'TR': 'TR1', 'OC': 'OC1', 'INTL': 'BR1',
}

INDEX = "lol_pro_matches"


@dataclass
class CompetitiveGame:
    """
    A single game within a competitive series.

    Contains the series-level context (teams, result, stage) plus
    per-game data available from the LoL Esports API (game number, VOD).

    The 'participants' field is intentionally empty until Riot Match-V5
    enrichment becomes available via a public LoL Esports -> Riot ID mapping.
    """
    match_id: str           # LoL Esports series snowflake ID
    game_id: str            # LoL Esports game snowflake ID (NOT a Riot match ID)
    game_number: int        # 1-indexed position within the series
    league: str
    stage: str              # "Play-Ins", "Week 4", "Finals", etc.
    start_time: str         # ISO 8601 from LoL Esports (series start time)
    best_of: int            # 3 or 5
    team1: Dict             # name, code, image, game_wins (series totals)
    team2: Dict
    winner_code: str        # team code of the series winner
    vod_youtube_id: Optional[str]   # YouTube video ID if VOD available
    # Riot enrichment - blocked pending LoL Esports -> Riot ID mapping
    riot_match_id: Optional[str] = None
    game_duration_seconds: int = 0
    patch: str = "unknown"
    participants: List[Dict] = field(default_factory=list)
    riot_enriched: bool = False

    def to_dict(self) -> Dict:
        data = asdict(self)
        data['_id'] = f"{self.match_id}_{self.game_number}"
        data['indexed_at'] = datetime.utcnow().isoformat()
        return data


class CompetitivePipeline:
    """ETL Pipeline for competitive matches."""

    def __init__(self, leagues: List[str] = None, is_production: bool = False):
        self.leagues = leagues or ['CBLOL']
        self.is_production = is_production
        self.es_client = get_client()
        self.processed_games: Set[str] = set()
        self.stats = defaultdict(int)
        self.running = False

        os.makedirs('data/competitive', exist_ok=True)
        os.makedirs('logs', exist_ok=True)

        logger.info(f"CompetitivePipeline initialized for leagues: {self.leagues}")

    def extract_competitive_matches(self, league: str, limit: int = 100) -> List[CompetitiveGame]:
        """Extract completed competitive games for a league.

        Uses getCompletedEvents which returns real data (up to 300 series).
        Each played game (vods non-empty) becomes one CompetitiveGame document.
        """
        logger.info(f"Extracting competitive games for {league} (limit: {limit})")

        if league not in COMPETITIVE_LEAGUES:
            logger.warning(f"Unknown league: {league}. Skipping.")
            return []

        league_id = self._find_league_id(league)
        if not league_id:
            logger.error(f"League ID not found for {league}")
            return []

        data = get_completed_events(league_id)
        if not data:
            logger.warning(f"No completed events found for {league}")
            return []

        all_events = data.get('data', {}).get('schedule', {}).get('events', [])

        # getCompletedEvents returns events across ALL leagues in the response.
        # Filter to only the target league.
        events = [
            e for e in all_events
            if e.get('league', {}).get('name', '').upper() == league.upper()
        ]
        logger.info(f"Found {len(events)} completed series for {league} (out of {len(all_events)} total in response)")

        games = []
        processed_count = 0

        for event in events:
            if processed_count >= limit:
                break

            extracted = self._extract_games_from_event(event, league)
            games.extend(extracted)
            processed_count += len(extracted)

        logger.info(f"Extracted {len(games)} games from {league}")
        return games

    def _extract_games_from_event(self, event: Dict, league: str) -> List[CompetitiveGame]:
        """Extract individual games from a completed series event."""
        games = []

        match = event.get('match', {})
        match_id = match.get('id')
        if not match_id:
            return games

        teams = match.get('teams', [])
        if len(teams) != 2:
            return games

        team1_raw = teams[0]
        team2_raw = teams[1]

        # Determine series winner by comparing game_wins
        t1_wins = team1_raw.get('result', {}).get('gameWins', 0)
        t2_wins = team2_raw.get('result', {}).get('gameWins', 0)
        winner_code = team1_raw.get('code', '') if t1_wins > t2_wins else team2_raw.get('code', '')

        team1 = {
            'name': team1_raw.get('name'),
            'code': team1_raw.get('code'),
            'image': team1_raw.get('image'),
            'game_wins': t1_wins,
        }
        team2 = {
            'name': team2_raw.get('name'),
            'code': team2_raw.get('code'),
            'image': team2_raw.get('image'),
            'game_wins': t2_wins,
        }

        stage = event.get('blockName', 'Regular Season')
        start_time = event.get('startTime', '')
        best_of = match.get('strategy', {}).get('count', 3)

        event_games = event.get('games', [])

        for idx, game in enumerate(event_games, start=1):
            game_id = game.get('id')
            if not game_id:
                continue

            vods = game.get('vods', [])

            # A game with no VODs was not played (series ended before this game)
            if not vods:
                continue

            # Deduplicate
            doc_id = f"{match_id}_{idx}"
            if doc_id in self.processed_games:
                self.stats['duplicate_games'] += 1
                continue

            # Extract YouTube VOD ID (prefer YouTube over Twitch)
            vod_id = self._extract_youtube_vod(vods)

            competitive_game = CompetitiveGame(
                match_id=match_id,
                game_id=game_id,
                game_number=idx,
                league=league,
                stage=stage,
                start_time=start_time,
                best_of=best_of,
                team1=team1,
                team2=team2,
                winner_code=winner_code,
                vod_youtube_id=vod_id,
            )

            games.append(competitive_game)
            self.processed_games.add(doc_id)
            self.stats['successful_extractions'] += 1

        return games

    def _extract_youtube_vod(self, vods: List[Dict]) -> Optional[str]:
        """Extract YouTube video ID from VOD list.

        VOD parameters are YouTube video IDs (11 chars) or Twitch broadcast IDs.
        YouTube IDs are alphanumeric + hyphens, Twitch IDs are purely numeric.
        """
        if not vods:
            return None
        for vod in vods:
            param = vod.get('parameter', '')
            # YouTube IDs: 11 chars, mix of letters/numbers/hyphens/underscores
            if param and not param.isdigit() and len(param) <= 15:
                return param
        # Fall back to first available
        return vods[0].get('parameter') if vods else None

    def _find_league_id(self, league_name: str) -> Optional[str]:
        """Find league ID from LoL Esports API."""
        try:
            data = get_leagues()
            leagues = data.get('data', {}).get('leagues', [])
            for lg in leagues:
                if lg.get('name', '').lower() == league_name.lower():
                    return lg.get('id')
        except Exception as e:
            logger.error(f"Error finding league ID: {e}")
        return None

    def load_to_elasticsearch(self, games: List[CompetitiveGame]):
        """Load competitive games to Elasticsearch."""
        if not games:
            logger.warning("No games to load")
            return

        ensure_index(INDEX, MATCHES_MAPPING)

        docs = [g.to_dict() for g in games]

        try:
            bulk_index(INDEX, docs)
            logger.info(f"Successfully indexed {len(docs)} competitive games")
            self.stats['indexed_games'] += len(docs)
        except Exception as e:
            logger.error(f"Error indexing to Elasticsearch: {e}")
            self.stats['indexing_errors'] += 1

    def save_to_json(self, games: List[CompetitiveGame], league: str):
        """Save games to JSON file for backup/debugging."""
        if not games:
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data/competitive/{league}_{timestamp}.json"

        try:
            with open(filename, 'w') as f:
                json.dump([g.to_dict() for g in games], f, indent=2, default=str)
            logger.info(f"Saved {len(games)} games to {filename}")
            self.stats['saved_to_json'] += len(games)
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")

    def run_pipeline(self, limit_per_league: int = 50):
        """Run the complete ETL pipeline for all configured leagues."""
        logger.info("=" * 80)
        logger.info("Starting Competitive Matches ETL Pipeline")
        logger.info(f"Leagues: {', '.join(self.leagues)}")
        logger.info(f"Limit per league: {limit_per_league}")
        logger.info("=" * 80)

        self.running = True
        start_time = time.time()

        for league in self.leagues:
            if not self.running:
                logger.info("Pipeline stopped")
                break

            logger.info(f"\n--- Processing {league} ---")

            try:
                games = self.extract_competitive_matches(league, limit_per_league)

                if games:
                    self.load_to_elasticsearch(games)
                    self.save_to_json(games, league)
                else:
                    logger.warning(f"No games found for {league}")

            except Exception as e:
                logger.error(f"Error processing {league}: {e}")
                self.stats['league_errors'] += 1

        elapsed = time.time() - start_time
        self.print_stats(elapsed)

    def run_daemon(self, interval_hours: int = 1):
        """Run pipeline as a daemon, periodically syncing matches."""
        logger.info(f"Starting daemon mode - syncing every {interval_hours} hours")

        def signal_handler(signum, frame):
            logger.info("Received stop signal, shutting down...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        self.running = True
        while self.running:
            try:
                self.run_pipeline()
                logger.info(f"Sleeping for {interval_hours} hours...")
                time.sleep(interval_hours * 3600)
            except KeyboardInterrupt:
                logger.info("Daemon stopped by user")
                break
            except Exception as e:
                logger.error(f"Daemon error: {e}")
                time.sleep(300)

    def print_stats(self, elapsed_time: float = 0):
        """Print pipeline statistics."""
        print("\n" + "=" * 80)
        print("COMPETITIVE PIPELINE STATISTICS")
        print("=" * 80)
        print(f"Successful extractions: {self.stats['successful_extractions']}")
        print(f"Indexed to Elasticsearch: {self.stats['indexed_games']}")
        print(f"Saved to JSON: {self.stats['saved_to_json']}")
        print(f"Duplicate games skipped: {self.stats['duplicate_games']}")
        print(f"Extraction errors: {self.stats['extraction_errors']}")
        print(f"Indexing errors: {self.stats['indexing_errors']}")
        print(f"League errors: {self.stats['league_errors']}")
        if elapsed_time > 0:
            print(f"\nTotal time: {elapsed_time:.2f}s")
        print("=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Competitive Matches ETL Pipeline')
    parser.add_argument('--leagues', nargs='+', default=['CBLOL'],
                        help='Leagues to process (default: CBLOL)')
    parser.add_argument('--limit', type=int, default=50,
                        help='Game limit per league (default: 50)')
    parser.add_argument('--production', action='store_true',
                        help='Use production settings')
    parser.add_argument('--daemon', action='store_true',
                        help='Run as daemon with periodic sync')
    parser.add_argument('--interval', type=int, default=1,
                        help='Sync interval in hours for daemon mode (default: 1)')

    args = parser.parse_args()

    pipeline = CompetitivePipeline(
        leagues=args.leagues,
        is_production=args.production,
    )

    if args.daemon:
        pipeline.run_daemon(interval_hours=args.interval)
    else:
        pipeline.run_pipeline(limit_per_league=args.limit)


if __name__ == '__main__':
    main()

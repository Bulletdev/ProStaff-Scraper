#!/usr/bin/env python3
"""
Competitive Match Enrichment Pipeline

Enriches competitive game documents in Elasticsearch with per-game stats
(items, runes, KDA, champions) entirely from Leaguepedia.

Flow per game:
  1. Query Leaguepedia ScoreboardGames to find the internal page name
     (team names + date + game number are the search key)
  2. Query Leaguepedia ScoreboardPlayers using the page name to get
     per-player stats: champion, KDA, gold, CS, damage, items, runes,
     summoner spells, role, win/loss

No Riot Match-V5 dependency: competitive games run on Riot's internal
tournament servers and are not accessible via the public Match-V5 API.
Leaguepedia receives official data from Riot's esports disclosure program.

Rate limiting:
  Leaguepedia allows ~1 anonymous request per 8 seconds. Each game
  requires 2 requests (ScoreboardGames + ScoreboardPlayers) with a 9s
  sleep between them. The enrichment loop also sleeps 9s between games.
  Rough estimate: 50-game batch takes ~27 minutes.

Deduplication:
  Only documents with riot_enriched=false are processed.
  Successful enrichment sets riot_enriched=true, preventing re-processing.

Failed games:
  enrichment_attempts counter is incremented on each failure.
  After MAX_ATTEMPTS failures the game is marked unenrichable and skipped.
"""

import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional, Dict, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from providers.leaguepedia import get_game_data, RATE_LIMIT_SECONDS
from indexers.elasticsearch_client import query_unenriched, update_document

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/enrichment_pipeline.log'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

INDEX = "lol_pro_matches"
MAX_ATTEMPTS = 3  # Give up enriching a game after this many failures


def _extract_date(start_time: str) -> str:
    """Extract YYYY-MM-DD from an ISO 8601 start_time string."""
    return start_time[:10]


class EnrichmentPipeline:
    """Enriches unenriched competitive games with Leaguepedia data."""

    def __init__(self, batch_size: int = 50, is_production: bool = False):
        self.batch_size = batch_size
        self.is_production = is_production  # kept for API compatibility
        self.running = False
        self.stats = {
            'enriched': 0,
            'leaguepedia_miss': 0,
            'no_players': 0,
            'errors': 0,
            'skipped_max_attempts': 0,
        }

    def enrich_game(self, doc_id: str, source: Dict) -> bool:
        """Attempt to enrich a single game document using Leaguepedia.

        Returns True if enrichment succeeded, False otherwise.
        """
        team1_name = source.get('team1', {}).get('name', '')
        team2_name = source.get('team2', {}).get('name', '')
        start_time = source.get('start_time', '')
        game_number = source.get('game_number', 1)
        league = source.get('league', 'UNKNOWN')

        if not team1_name or not team2_name or not start_time:
            logger.warning(f"Skipping {doc_id}: missing team names or start_time")
            return False

        date_str = _extract_date(start_time)
        logger.info(
            f"Enriching {doc_id}: {team1_name} vs {team2_name} G{game_number} "
            f"({league} {date_str})"
        )

        # Fetch game data from Leaguepedia (2 requests: ScoreboardGames + ScoreboardPlayers)
        try:
            game_data = get_game_data(
                team1_name=team1_name,
                team2_name=team2_name,
                date_utc=date_str,
                game_number=game_number,
            )
        except Exception as e:
            logger.error(f"Leaguepedia error for {doc_id}: {e}")
            self.stats['errors'] += 1
            return False

        if not game_data:
            logger.info(f"Leaguepedia has no record for {doc_id} yet")
            self.stats['leaguepedia_miss'] += 1
            return False

        players = game_data.get('players', [])
        if not players:
            logger.warning(f"Leaguepedia has game record but no players for {doc_id}")
            self.stats['no_players'] += 1
            return False

        # Update the ES document with enriched data
        update_document(INDEX, doc_id, {
            'riot_enriched': True,
            'leaguepedia_page': game_data['page_name'],
            'participants': players,
            'patch': game_data.get('patch', ''),
            'win_team': game_data.get('win_team', ''),
            'gamelength': game_data.get('gamelength', ''),
            'game_duration_seconds': game_data.get('gamelength_seconds', 0),
            'enriched_at': datetime.now(tz=timezone.utc).isoformat(),
            'enrichment_source': 'leaguepedia',
        })

        logger.info(
            f"Enriched {doc_id}: {game_data['page_name']}, "
            f"{len(players)} players, patch {game_data.get('patch')}, "
            f"winner={game_data.get('win_team')}"
        )
        self.stats['enriched'] += 1
        return True

    def run_batch(self) -> int:
        """Process one batch of unenriched games.

        Returns the number of games processed (attempted, not necessarily enriched).
        """
        docs = query_unenriched(INDEX, size=self.batch_size)

        if not docs:
            logger.info("No unenriched games found.")
            return 0

        logger.info(f"Processing {len(docs)} unenriched games...")

        processed = 0
        for i, doc in enumerate(docs):
            if not self.running:
                logger.info("Enrichment batch stopped")
                break

            doc_id = doc['_id']
            source = doc['_source']

            # Skip games that have failed too many times
            attempts = source.get('enrichment_attempts', 0)
            if attempts >= MAX_ATTEMPTS:
                logger.debug(
                    f"Skipping {doc_id}: max attempts ({MAX_ATTEMPTS}) reached"
                )
                self.stats['skipped_max_attempts'] += 1
                continue

            try:
                success = self.enrich_game(doc_id, source)
            except Exception as e:
                logger.error(f"Unexpected error enriching {doc_id}: {e}")
                success = False
                self.stats['errors'] += 1

            if not success:
                # Increment attempt counter but don't mark as enriched
                update_document(INDEX, doc_id, {
                    'enrichment_attempts': attempts + 1,
                    'last_enrichment_attempt': datetime.now(tz=timezone.utc).isoformat(),
                })

            processed += 1

            # Rate limit: sleep between games (get_game_data already slept between
            # its two internal requests; this sleep separates games from each other)
            if i < len(docs) - 1 and self.running:
                logger.debug(f"Rate limit sleep ({RATE_LIMIT_SECONDS}s)...")
                time.sleep(RATE_LIMIT_SECONDS)

        self.print_stats()
        return processed

    def run_daemon(self, interval_minutes: int = 30):
        """Run enrichment continuously, checking for new unenriched games
        every `interval_minutes` after each batch completes.
        """
        logger.info(
            f"Starting enrichment daemon - interval: {interval_minutes}min, "
            f"batch: {self.batch_size} games"
        )

        def signal_handler(signum, frame):
            logger.info("Received stop signal, shutting down...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        self.running = True
        while self.running:
            processed = self.run_batch()

            if processed == 0:
                logger.info(
                    f"Nothing to enrich. Sleeping {interval_minutes}min..."
                )
            else:
                logger.info(
                    f"Batch done ({processed} games attempted). "
                    f"Sleeping {interval_minutes}min..."
                )

            # Sleep in 10s increments so we can respond to stop signals quickly
            for _ in range(interval_minutes * 6):
                if not self.running:
                    break
                time.sleep(10)

    def print_stats(self):
        logger.info(
            f"Enrichment stats: enriched={self.stats['enriched']}, "
            f"leaguepedia_miss={self.stats['leaguepedia_miss']}, "
            f"no_players={self.stats['no_players']}, "
            f"errors={self.stats['errors']}, "
            f"skipped_max_attempts={self.stats['skipped_max_attempts']}"
        )


def main():
    parser = argparse.ArgumentParser(description='Competitive Match Enrichment Pipeline')
    parser.add_argument('--batch', type=int, default=50,
                        help='Number of games to enrich per batch (default: 50)')
    parser.add_argument('--production', action='store_true',
                        help='(no-op, kept for compatibility)')
    parser.add_argument('--daemon', action='store_true',
                        help='Run as daemon with periodic enrichment')
    parser.add_argument('--interval', type=int, default=30,
                        help='Daemon interval in minutes between batches (default: 30)')

    args = parser.parse_args()

    pipeline = EnrichmentPipeline(
        batch_size=args.batch,
        is_production=args.production,
    )

    if args.daemon:
        pipeline.run_daemon(interval_minutes=args.interval)
    else:
        pipeline.running = True
        pipeline.run_batch()


if __name__ == '__main__':
    main()

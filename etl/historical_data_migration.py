#!/usr/bin/env python3
"""
Historical Match Data Migration Tool
Migrates old Match v3 API JSON files to current ETL pipeline
"""

import json
import os
import glob
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import logging
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DataQualityReport:
    """Data quality metrics for migration"""
    total_files: int = 0
    valid_files: int = 0
    invalid_files: int = 0
    duplicate_matches: int = 0
    missing_fields: Dict = None
    date_range: Tuple[datetime, datetime] = None
    platforms: Dict[str, int] = None
    game_versions: Dict[str, int] = None
    queue_types: Dict[str, int] = None
    average_game_duration: float = 0
    total_participants: int = 0
    errors: List[Dict] = None

    def __post_init__(self):
        if self.missing_fields is None:
            self.missing_fields = {}
        if self.platforms is None:
            self.platforms = {}
        if self.game_versions is None:
            self.game_versions = {}
        if self.queue_types is None:
            self.queue_types = {}
        if self.errors is None:
            self.errors = []

class MatchV3ToV5Converter:
    """Converts Match v3 API data to v5 format"""

    QUEUE_MAP = {
        420: 'RANKED_SOLO_5x5',
        440: 'RANKED_FLEX_5x5',
        400: 'NORMAL_DRAFT',
        430: 'NORMAL_BLIND',
        450: 'ARAM',
        0: 'CUSTOM'
    }

    @staticmethod
    def convert_match(v3_data: Dict) -> Dict:
        """Convert v3 match data to v5 format"""

        # Create metadata
        metadata = {
            "dataVersion": "2",
            "matchId": f"{v3_data.get('platformId', 'EUW1')}_{v3_data.get('gameId')}",
            "participants": []
        }

        # Extract participant PUUIDs (if available)
        for p_identity in v3_data.get('participantIdentities', []):
            player = p_identity.get('player', {})
            # In v3, we don't have PUUID, so we'll use account ID as placeholder
            metadata['participants'].append(
                f"LEGACY_{player.get('accountId', 'UNKNOWN')}"
            )

        # Create info section
        info = {
            "gameId": v3_data.get('gameId'),
            "gameCreation": v3_data.get('gameCreation'),
            "gameDuration": v3_data.get('gameDuration'),
            "gameStartTimestamp": v3_data.get('gameCreation'),
            "gameEndTimestamp": v3_data.get('gameCreation', 0) + (v3_data.get('gameDuration', 0) * 1000),
            "gameMode": v3_data.get('gameMode'),
            "gameName": f"Legacy Match {v3_data.get('gameId')}",
            "gameType": v3_data.get('gameType'),
            "gameVersion": v3_data.get('gameVersion'),
            "mapId": v3_data.get('mapId'),
            "platformId": v3_data.get('platformId'),
            "queueId": v3_data.get('queueId'),
            "teams": MatchV3ToV5Converter._convert_teams(v3_data.get('teams', [])),
            "participants": MatchV3ToV5Converter._convert_participants(
                v3_data.get('participants', []),
                v3_data.get('participantIdentities', [])
            )
        }

        return {
            "metadata": metadata,
            "info": info
        }

    @staticmethod
    def _convert_teams(v3_teams: List[Dict]) -> List[Dict]:
        """Convert v3 team data to v5 format"""
        v5_teams = []

        for team in v3_teams:
            v5_team = {
                "teamId": team.get('teamId'),
                "win": team.get('win') == 'Win',
                "bans": [
                    {
                        "championId": ban.get('championId'),
                        "pickTurn": ban.get('pickTurn')
                    }
                    for ban in team.get('bans', [])
                ],
                "objectives": {
                    "baron": {"first": team.get('firstBaron', False), "kills": team.get('baronKills', 0)},
                    "champion": {"first": team.get('firstBlood', False), "kills": 0},
                    "dragon": {"first": team.get('firstDragon', False), "kills": team.get('dragonKills', 0)},
                    "inhibitor": {"first": team.get('firstInhibitor', False), "kills": team.get('inhibitorKills', 0)},
                    "riftHerald": {"first": team.get('firstRiftHerald', False), "kills": team.get('riftHeraldKills', 0)},
                    "tower": {"first": team.get('firstTower', False), "kills": team.get('towerKills', 0)}
                }
            }
            v5_teams.append(v5_team)

        return v5_teams

    @staticmethod
    def _convert_participants(v3_participants: List[Dict], v3_identities: List[Dict]) -> List[Dict]:
        """Convert v3 participant data to v5 format"""
        v5_participants = []

        # Create identity map
        identity_map = {
            identity['participantId']: identity.get('player', {})
            for identity in v3_identities
        }

        for participant in v3_participants:
            stats = participant.get('stats', {})
            timeline = participant.get('timeline', {})
            player_info = identity_map.get(participant.get('participantId'), {})

            v5_participant = {
                "participantId": participant.get('participantId'),
                "puuid": f"LEGACY_{player_info.get('accountId', 'UNKNOWN')}",
                "summonerId": str(player_info.get('summonerId', '')),
                "summonerName": player_info.get('summonerName', 'Unknown'),
                "summonerLevel": 30,  # Default for old data
                "teamId": participant.get('teamId'),
                "championId": participant.get('championId'),
                "championName": f"Champion_{participant.get('championId')}",  # Would need champion mapping
                "role": timeline.get('role', 'NONE'),
                "lane": timeline.get('lane', 'NONE'),
                "individualPosition": MatchV3ToV5Converter._map_position(timeline),

                # Stats
                "kills": stats.get('kills', 0),
                "deaths": stats.get('deaths', 0),
                "assists": stats.get('assists', 0),
                "totalDamageDealtToChampions": stats.get('totalDamageDealtToChampions', 0),
                "totalDamageTaken": stats.get('totalDamageTaken', 0),
                "totalHeal": stats.get('totalHeal', 0),
                "totalMinionsKilled": stats.get('totalMinionsKilled', 0),
                "neutralMinionsKilled": stats.get('neutralMinionsKilled', 0),
                "goldEarned": stats.get('goldEarned', 0),
                "goldSpent": stats.get('goldSpent', 0),
                "champLevel": stats.get('champLevel', 1),
                "visionScore": stats.get('visionScore', 0),
                "wardsPlaced": stats.get('wardsPlaced', 0),
                "wardsKilled": stats.get('wardsKilled', 0),
                "firstBloodKill": stats.get('firstBloodKill', False),
                "firstTowerKill": stats.get('firstTowerKill', False),
                "win": stats.get('win', False),

                # Items
                "item0": stats.get('item0', 0),
                "item1": stats.get('item1', 0),
                "item2": stats.get('item2', 0),
                "item3": stats.get('item3', 0),
                "item4": stats.get('item4', 0),
                "item5": stats.get('item5', 0),
                "item6": stats.get('item6', 0),

                # Summoner spells
                "summoner1Id": participant.get('spell1Id', 0),
                "summoner2Id": participant.get('spell2Id', 0),

                # Perks (runes reforged - not available in v3)
                "perks": {
                    "statPerks": {},
                    "styles": []
                }
            }

            v5_participants.append(v5_participant)

        return v5_participants

    @staticmethod
    def _map_position(timeline: Dict) -> str:
        """Map v3 role/lane to v5 position"""
        role = timeline.get('role', '')
        lane = timeline.get('lane', '')

        position_map = {
            ('DUO_CARRY', 'BOTTOM'): 'BOTTOM',
            ('DUO_SUPPORT', 'BOTTOM'): 'UTILITY',
            ('SOLO', 'TOP'): 'TOP',
            ('SOLO', 'MIDDLE'): 'MIDDLE',
            ('NONE', 'JUNGLE'): 'JUNGLE'
        }

        return position_map.get((role, lane), 'UNKNOWN')

class HistoricalDataMigrator:
    """Main migration orchestrator"""

    def __init__(self, source_dir: str, target_dir: str, elasticsearch_url: str = None):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.elasticsearch_url = elasticsearch_url
        self.converter = MatchV3ToV5Converter()
        self.report = DataQualityReport()
        self.processed_matches = set()

    def scan_files(self) -> List[Path]:
        """Scan source directory for JSON files"""
        pattern = str(self.source_dir / "*.json")
        files = glob.glob(pattern)
        logger.info(f"Found {len(files)} JSON files to process")
        return [Path(f) for f in files]

    def validate_file(self, file_path: Path) -> Tuple[bool, Optional[Dict], Optional[str]]:
        """Validate a single JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Check required fields
            required_fields = ['gameId', 'platformId', 'gameCreation', 'participants', 'teams']
            missing = [field for field in required_fields if field not in data]

            if missing:
                return False, None, f"Missing fields: {missing}"

            # Check data integrity
            if len(data.get('participants', [])) != 10:
                return False, None, f"Invalid participant count: {len(data.get('participants', []))}"

            if len(data.get('teams', [])) != 2:
                return False, None, f"Invalid team count: {len(data.get('teams', []))}"

            return True, data, None

        except json.JSONDecodeError as e:
            return False, None, f"JSON decode error: {e}"
        except Exception as e:
            return False, None, f"Unexpected error: {e}"

    def process_file(self, file_path: Path) -> Optional[Dict]:
        """Process a single file"""
        is_valid, data, error = self.validate_file(file_path)

        if not is_valid:
            logger.warning(f"Invalid file {file_path.name}: {error}")
            self.report.invalid_files += 1
            self.report.errors.append({
                'file': str(file_path),
                'error': error
            })
            return None

        # Check for duplicates
        match_id = f"{data.get('platformId')}_{data.get('gameId')}"
        if match_id in self.processed_matches:
            self.report.duplicate_matches += 1
            return None

        self.processed_matches.add(match_id)

        # Update statistics
        self._update_stats(data)

        # Convert to v5 format
        try:
            v5_data = self.converter.convert_match(data)
            self.report.valid_files += 1
            return v5_data
        except Exception as e:
            logger.error(f"Conversion error for {file_path.name}: {e}")
            self.report.invalid_files += 1
            return None

    def _update_stats(self, data: Dict):
        """Update data quality statistics"""
        # Platform distribution
        platform = data.get('platformId', 'UNKNOWN')
        self.report.platforms[platform] = self.report.platforms.get(platform, 0) + 1

        # Game version distribution
        version = data.get('gameVersion', 'UNKNOWN')
        if version:
            major_version = '.'.join(version.split('.')[:2])
            self.report.game_versions[major_version] = self.report.game_versions.get(major_version, 0) + 1

        # Queue type distribution
        queue_id = data.get('queueId', 0)
        queue_name = MatchV3ToV5Converter.QUEUE_MAP.get(queue_id, f'QUEUE_{queue_id}')
        self.report.queue_types[queue_name] = self.report.queue_types.get(queue_name, 0) + 1

        # Participant count
        self.report.total_participants += len(data.get('participants', []))

    def migrate_batch(self, files: List[Path], batch_size: int = 100) -> List[Dict]:
        """Process files in batches with parallel processing"""
        all_results = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            # Process in chunks
            for i in range(0, len(files), batch_size):
                batch = files[i:i+batch_size]

                futures = {
                    executor.submit(self.process_file, file_path): file_path
                    for file_path in batch
                }

                # Collect results
                for future in tqdm(as_completed(futures), total=len(futures), desc=f"Batch {i//batch_size + 1}"):
                    result = future.result()
                    if result:
                        all_results.append(result)

        return all_results

    def save_converted_data(self, converted_data: List[Dict], output_format: str = 'parquet'):
        """Save converted data to files"""
        self.target_dir.mkdir(parents=True, exist_ok=True)

        if output_format == 'parquet':
            # Convert to DataFrame for Parquet
            df_data = []
            for match in converted_data:
                match_flat = {
                    'match_id': match['metadata']['matchId'],
                    'game_id': match['info']['gameId'],
                    'platform_id': match['info']['platformId'],
                    'game_creation': match['info']['gameCreation'],
                    'game_duration': match['info']['gameDuration'],
                    'game_version': match['info']['gameVersion'],
                    'queue_id': match['info']['queueId']
                }
                df_data.append(match_flat)

            df = pd.DataFrame(df_data)
            output_path = self.target_dir / f"historical_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.to_parquet(output_path)
            logger.info(f"Saved {len(df)} matches to {output_path}")

        elif output_format == 'json':
            # Save as JSONL for streaming
            output_path = self.target_dir / f"historical_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            with open(output_path, 'w') as f:
                for match in converted_data:
                    f.write(json.dumps(match) + '\n')
            logger.info(f"Saved {len(converted_data)} matches to {output_path}")

    def generate_report(self) -> str:
        """Generate migration report"""
        report_lines = [
            "=" * 80,
            "HISTORICAL DATA MIGRATION REPORT",
            "=" * 80,
            "",
            f"Total Files Scanned: {self.report.total_files:,}",
            f"Valid Files: {self.report.valid_files:,}",
            f"Invalid Files: {self.report.invalid_files:,}",
            f"Duplicate Matches: {self.report.duplicate_matches:,}",
            "",
            "Platform Distribution:",
        ]

        for platform, count in sorted(self.report.platforms.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"  {platform}: {count:,}")

        report_lines.extend([
            "",
            "Game Version Distribution (Top 10):",
        ])

        version_items = sorted(self.report.game_versions.items(), key=lambda x: x[1], reverse=True)[:10]
        for version, count in version_items:
            report_lines.append(f"  {version}: {count:,}")

        report_lines.extend([
            "",
            "Queue Type Distribution:",
        ])

        for queue, count in sorted(self.report.queue_types.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"  {queue}: {count:,}")

        if self.report.errors:
            report_lines.extend([
                "",
                f"Errors (showing first 10 of {len(self.report.errors)}):",
            ])
            for error in self.report.errors[:10]:
                report_lines.append(f"  - {Path(error['file']).name}: {error['error']}")

        report_lines.extend([
            "",
            "=" * 80,
            f"Report generated at: {datetime.now().isoformat()}",
            "=" * 80,
        ])

        return "\n".join(report_lines)

    def run_migration(self):
        """Execute the full migration pipeline"""
        logger.info("Starting historical data migration...")

        # Scan files
        files = self.scan_files()
        self.report.total_files = len(files)

        if not files:
            logger.warning("No files found to process")
            return

        # Process files in batches
        logger.info("Processing files...")
        converted_data = self.migrate_batch(files)

        # Save converted data
        logger.info("Saving converted data...")
        self.save_converted_data(converted_data, 'parquet')
        self.save_converted_data(converted_data, 'json')

        # Generate and save report
        report = self.generate_report()
        report_path = self.target_dir / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w') as f:
            f.write(report)

        print(report)
        logger.info(f"Migration complete! Report saved to {report_path}")

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Migrate historical LoL match data to ETL pipeline')
    parser.add_argument('--source',
                       default='/media/bullet/ACB8ED81B8ED4B02/Documents and Settings/Bullet/Documents/1lolscrap/jsons',
                       help='Source directory containing JSON files')
    parser.add_argument('--target',
                       default='/home/bullet/PROJETOS/ProStaff-Scraper/data/historical',
                       help='Target directory for converted data')
    parser.add_argument('--elasticsearch',
                       default=None,
                       help='Elasticsearch URL for direct indexing')
    parser.add_argument('--sample',
                       type=int,
                       default=None,
                       help='Process only a sample of N files')

    args = parser.parse_args()

    # Initialize migrator
    migrator = HistoricalDataMigrator(
        source_dir=args.source,
        target_dir=args.target,
        elasticsearch_url=args.elasticsearch
    )

    # Run migration
    migrator.run_migration()

if __name__ == '__main__':
    main()
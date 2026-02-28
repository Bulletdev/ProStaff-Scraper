#!/usr/bin/env python3
"""
Quick validation script for historical match data
Run this to check your JSON files quickly!
"""

import json
import os
import random
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import sys

def format_size(bytes):
    """Format bytes to human readable size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.1f} TB"

def analyze_historical_data(json_dir: str, sample_size: int = 100):
    """Quick analysis of historical JSON files"""

    print("=" * 80)
    print("HISTORICAL DATA VALIDATION TOOL")
    print("=" * 80)
    print(f"\n📁 Scanning directory: {json_dir}\n")

    # Check if directory exists
    if not os.path.exists(json_dir):
        print(f"❌ ERROR: Directory not found: {json_dir}")
        return

    # Get all JSON files
    json_files = list(Path(json_dir).glob("*.json"))
    total_files = len(json_files)

    if total_files == 0:
        print("❌ No JSON files found in directory")
        return

    print(f"✅ Found {total_files:,} JSON files")

    # Calculate total size
    total_size = sum(f.stat().st_size for f in json_files)
    print(f"💾 Total size: {format_size(total_size)}")
    print(f"📊 Average file size: {format_size(total_size / total_files)}")

    print("\n" + "-" * 80)
    print(f"ANALYZING SAMPLE OF {min(sample_size, total_files)} FILES...")
    print("-" * 80 + "\n")

    # Sample files for analysis
    sample_files = random.sample(json_files, min(sample_size, total_files))

    # Statistics collectors
    valid_files = 0
    invalid_files = 0
    errors = []
    platforms = defaultdict(int)
    patches = defaultdict(int)
    queues = defaultdict(int)
    durations = []
    dates = []
    champions = defaultdict(int)
    total_players = 0

    # Analyze each file
    for i, file_path in enumerate(sample_files, 1):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Validate structure
            required_fields = ['gameId', 'platformId', 'gameCreation', 'gameDuration', 'participants', 'teams']
            missing = [field for field in required_fields if field not in data]

            if missing:
                invalid_files += 1
                errors.append(f"{file_path.name}: Missing {missing}")
                continue

            # Validate data integrity
            if len(data.get('participants', [])) != 10:
                invalid_files += 1
                errors.append(f"{file_path.name}: Invalid participant count")
                continue

            valid_files += 1

            # Collect statistics
            platforms[data.get('platformId', 'UNKNOWN')] += 1

            # Parse patch version
            version = data.get('gameVersion', '')
            if version:
                major_patch = '.'.join(version.split('.')[:2])
                patches[major_patch] += 1

            # Queue type
            queue_map = {
                420: 'Ranked Solo',
                440: 'Ranked Flex',
                400: 'Normal Draft',
                430: 'Normal Blind',
                450: 'ARAM',
                0: 'Custom'
            }
            queue_id = data.get('queueId', 0)
            queue_name = queue_map.get(queue_id, f'Queue {queue_id}')
            queues[queue_name] += 1

            # Game duration
            durations.append(data.get('gameDuration', 0))

            # Game date
            creation_time = data.get('gameCreation', 0)
            if creation_time:
                dates.append(datetime.fromtimestamp(creation_time / 1000))

            # Champion picks
            for participant in data.get('participants', []):
                champ_id = participant.get('championId')
                if champ_id:
                    champions[champ_id] += 1

            total_players += len(data.get('participants', []))

            # Progress indicator
            if i % 10 == 0:
                print(f"  Processed {i}/{len(sample_files)} files...", end='\r')

        except json.JSONDecodeError:
            invalid_files += 1
            errors.append(f"{file_path.name}: Invalid JSON")
        except Exception as e:
            invalid_files += 1
            errors.append(f"{file_path.name}: {str(e)}")

    print("\n")

    # Display results
    print("=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    print(f"\n✅ Valid files: {valid_files}/{len(sample_files)}")
    print(f"❌ Invalid files: {invalid_files}/{len(sample_files)}")

    if valid_files > 0:
        success_rate = (valid_files / len(sample_files)) * 100
        print(f"📈 Success rate: {success_rate:.1f}%")

        # Extrapolate for full dataset
        estimated_valid = int((valid_files / len(sample_files)) * total_files)
        print(f"\n📊 Estimated valid files in full dataset: {estimated_valid:,} / {total_files:,}")

    if errors and len(errors) <= 10:
        print("\n⚠️  Errors found:")
        for error in errors[:10]:
            print(f"  - {error}")

    # Display statistics
    if platforms:
        print("\n" + "=" * 80)
        print("DATA STATISTICS")
        print("=" * 80)

        print("\n🌍 Platforms:")
        for platform, count in sorted(platforms.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(sample_files)) * 100
            print(f"  {platform}: {count} ({percentage:.1f}%)")

        print("\n🎮 Game Versions:")
        for patch, count in sorted(patches.items(), key=lambda x: x[1], reverse=True)[:5]:
            percentage = (count / len(sample_files)) * 100
            print(f"  Patch {patch}: {count} ({percentage:.1f}%)")

        print("\n🏆 Queue Types:")
        for queue, count in sorted(queues.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(sample_files)) * 100
            print(f"  {queue}: {count} ({percentage:.1f}%)")

        if durations:
            avg_duration = sum(durations) / len(durations)
            min_duration = min(durations)
            max_duration = max(durations)
            print(f"\n⏱️  Game Duration:")
            print(f"  Average: {avg_duration/60:.1f} minutes")
            print(f"  Min: {min_duration/60:.1f} minutes")
            print(f"  Max: {max_duration/60:.1f} minutes")

        if dates:
            dates.sort()
            print(f"\n📅 Date Range:")
            print(f"  From: {dates[0].strftime('%Y-%m-%d')}")
            print(f"  To: {dates[-1].strftime('%Y-%m-%d')}")
            print(f"  Span: {(dates[-1] - dates[0]).days} days")

        print(f"\n👥 Total player performances analyzed: {total_players:,}")
        print(f"🏅 Unique champions picked: {len(champions)}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if success_rate >= 95:
        print("\n✅ EXCELLENT: Your data is in great shape!")
        print("   The structure is valid and ready for ETL pipeline integration.")
    elif success_rate >= 80:
        print("\n⚠️  GOOD: Most files are valid, some cleanup may be needed.")
        print("   You can proceed with migration, invalid files will be skipped.")
    else:
        print("\n❌ NEEDS ATTENTION: Many files have issues.")
        print("   Review the errors and consider data cleanup before migration.")

    print("\n📝 Next steps:")
    print("   1. Run full migration: python etl/historical_data_migration.py")
    print("   2. Check migration report for detailed analysis")
    print("   3. Load data into your ETL pipeline")

    print("\n" + "=" * 80)
    print(f"Analysis complete! Sampled {len(sample_files)} of {total_files:,} files")
    print("=" * 80)

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Validate historical LoL match JSON files')
    parser.add_argument(
        '--dir',
        default='/media/bullet/ACB8ED81B8ED4B02/Documents and Settings/Bullet/Documents/1lolscrap/jsons',
        help='Directory containing JSON files'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=100,
        help='Number of files to sample for analysis (default: 100)'
    )

    args = parser.parse_args()

    try:
        analyze_historical_data(args.dir, args.sample)
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Quick test script to verify production setup
Run this to test your configuration before deploying
"""

import os
import sys
from dotenv import load_dotenv

# Load environment
load_dotenv()

def test_configuration():
    """Test all components are configured correctly"""

    print("="*60)
    print("PROSTAFF SCRAPER - PRODUCTION TEST")
    print("="*60)

    results = []

    # 1. Test environment variables
    print("\n1. Testing environment variables...")
    riot_key = os.getenv('RIOT_API_KEY')
    esports_key = os.getenv('ESPORTS_API_KEY')

    if riot_key and len(riot_key) > 20:
        print("   ✅ RIOT_API_KEY configured")
        results.append(True)
    else:
        print("   ❌ RIOT_API_KEY missing or invalid")
        results.append(False)

    if esports_key and len(esports_key) > 20:
        print("   ✅ ESPORTS_API_KEY configured")
        results.append(True)
    else:
        print("   ❌ ESPORTS_API_KEY missing or invalid")
        results.append(False)

    # 2. Test imports
    print("\n2. Testing imports...")
    try:
        from providers.riot_rate_limited import RiotAPIClient
        print("   ✅ Rate-limited Riot client imported")
        results.append(True)
    except ImportError as e:
        print(f"   ❌ Failed to import Riot client: {e}")
        results.append(False)

    try:
        from etl.competitive_pipeline import CompetitivePipeline
        print("   ✅ Competitive pipeline imported")
        results.append(True)
    except ImportError as e:
        print(f"   ❌ Failed to import pipeline: {e}")
        results.append(False)

    # 3. Test Riot API connection
    print("\n3. Testing Riot API connection...")
    if riot_key:
        try:
            from providers.riot_rate_limited import get_riot_client
            client = get_riot_client(is_production=False)
            # Test with a known match
            test_match = "BR1_2896989505"  # Example match ID
            print(f"   Testing with match {test_match}...")

            # This will use cache if available
            result = client.get_match_details(test_match, "BR1")
            if result:
                print("   ✅ Riot API connection successful")
                results.append(True)
            else:
                print("   ⚠️  Match not found (API works but no data)")
                results.append(True)  # API works, just no data
        except Exception as e:
            print(f"   ❌ Riot API error: {e}")
            results.append(False)
    else:
        print("   ⏭️  Skipping (no API key)")

    # 4. Test Elasticsearch connection
    print("\n4. Testing Elasticsearch...")
    es_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
    try:
        import httpx
        response = httpx.get(f"{es_url}/_cluster/health", timeout=5)
        if response.status_code == 200:
            health = response.json()
            status = health.get('status', 'unknown')
            print(f"   ✅ Elasticsearch connected (status: {status})")
            results.append(True)
        else:
            print(f"   ⚠️  Elasticsearch returned {response.status_code}")
            results.append(False)
    except Exception as e:
        print(f"   ⚠️  Elasticsearch not available: {e}")
        print("   (This is OK - scraper will cache to disk)")
        results.append(True)  # Not critical

    # 5. Test directories
    print("\n5. Testing directories...")
    dirs = ['data/competitive', 'data/cache/matches', 'logs']
    for dir_path in dirs:
        if os.path.exists(dir_path):
            print(f"   ✅ {dir_path} exists")
            results.append(True)
        else:
            print(f"   ❌ {dir_path} missing")
            results.append(False)

    # 6. Test competitive pipeline initialization
    print("\n6. Testing competitive pipeline...")
    if all(results[:4]):  # If basic tests passed
        try:
            from etl.competitive_pipeline import CompetitivePipeline
            pipeline = CompetitivePipeline(
                leagues=['CBLOL'],
                is_production=False
            )
            print("   ✅ Pipeline initialized successfully")
            results.append(True)

            # Print available leagues
            from etl.competitive_pipeline import COMPETITIVE_LEAGUES
            print("\n   Available leagues:")
            for league, config in list(COMPETITIVE_LEAGUES.items())[:10]:
                print(f"      • {league}: {config['region']} (Tier {config['tier']})")

        except Exception as e:
            print(f"   ❌ Pipeline initialization failed: {e}")
            results.append(False)
    else:
        print("   ⏭️  Skipping (prerequisites not met)")

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(results)
    total = len(results)
    percentage = (passed / total) * 100 if total > 0 else 0

    print(f"\nTests passed: {passed}/{total} ({percentage:.0f}%)")

    if percentage >= 80:
        print("\n✅ READY FOR PRODUCTION!")
        print("\nNext steps:")
        print("1. Run: ./deploy_production.sh")
        print("2. Or test manually: python etl/competitive_pipeline.py --leagues CBLOL --limit 5")
        return True
    elif percentage >= 60:
        print("\n⚠️  ALMOST READY")
        print("Fix the failed tests above before deploying")
        return False
    else:
        print("\n❌ NOT READY FOR PRODUCTION")
        print("Please fix the issues above")
        return False

if __name__ == "__main__":
    success = test_configuration()
    sys.exit(0 if success else 1)
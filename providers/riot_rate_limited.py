"""
Riot API Client with Production-Ready Rate Limiting
Handles all Riot API rate limits properly
"""

import os
import time
import httpx
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from collections import deque
import threading
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RateLimitException(Exception):
    """Custom exception for rate limit errors"""
    pass

class RiotRateLimiter:
    """
    Production-ready rate limiter for Riot API

    Riot API Rate Limits:
    - Development: 20 requests per second, 100 requests per 2 minutes
    - Production: 500 requests per 10 seconds (varies by key)
    """

    def __init__(self, is_production=False):
        # Rate limit windows
        if is_production:
            # Production key limits (adjust based on your actual key)
            self.limits = {
                '10s': {'max': 500, 'window': 10},
                '10m': {'max': 30000, 'window': 600}
            }
        else:
            # Development key limits
            self.limits = {
                '1s': {'max': 20, 'window': 1},
                '2m': {'max': 100, 'window': 120}
            }

        # Request tracking
        self.requests = {key: deque() for key in self.limits.keys()}
        self.lock = threading.Lock()

        # Rate limit headers tracking
        self.app_rate_limit = None
        self.app_rate_limit_count = None
        self.method_rate_limit = None
        self.method_rate_limit_count = None

        logger.info(f"RateLimiter initialized in {'production' if is_production else 'development'} mode")
        logger.info(f"Rate limits: {self.limits}")

    def can_make_request(self) -> Tuple[bool, float]:
        """Check if we can make a request now"""
        with self.lock:
            current_time = time.time()

            for key, limit_info in self.limits.items():
                # Clean old requests
                cutoff_time = current_time - limit_info['window']
                while self.requests[key] and self.requests[key][0] < cutoff_time:
                    self.requests[key].popleft()

                # Check if we've hit the limit
                if len(self.requests[key]) >= limit_info['max']:
                    # Calculate wait time
                    oldest_request = self.requests[key][0]
                    wait_time = (oldest_request + limit_info['window']) - current_time
                    return False, wait_time

            return True, 0

    def record_request(self):
        """Record that a request was made"""
        with self.lock:
            current_time = time.time()
            for key in self.requests:
                self.requests[key].append(current_time)

    def update_from_headers(self, headers: dict):
        """Update rate limits from response headers"""
        # Riot returns rate limit info in headers
        if 'X-App-Rate-Limit' in headers:
            self.app_rate_limit = headers['X-App-Rate-Limit']
        if 'X-App-Rate-Limit-Count' in headers:
            self.app_rate_limit_count = headers['X-App-Rate-Limit-Count']
        if 'X-Method-Rate-Limit' in headers:
            self.method_rate_limit = headers['X-Method-Rate-Limit']
        if 'X-Method-Rate-Limit-Count' in headers:
            self.method_rate_limit_count = headers['X-Method-Rate-Limit-Count']

        # Check if we're close to limits
        if self.app_rate_limit_count:
            self._check_limit_proximity(self.app_rate_limit_count, self.app_rate_limit, "App")
        if self.method_rate_limit_count:
            self._check_limit_proximity(self.method_rate_limit_count, self.method_rate_limit, "Method")

    def _check_limit_proximity(self, count_str: str, limit_str: str, limit_type: str):
        """Check if we're close to rate limits and log warnings"""
        try:
            # Parse "count:window,count:window" format
            counts = count_str.split(',')
            limits = limit_str.split(',')

            for count, limit in zip(counts, limits):
                current, window = map(int, count.split(':'))
                max_allowed, _ = map(int, limit.split(':'))

                usage_percent = (current / max_allowed) * 100
                if usage_percent > 80:
                    logger.warning(f"{limit_type} rate limit at {usage_percent:.1f}% ({current}/{max_allowed})")
        except Exception as e:
            logger.error(f"Error parsing rate limit headers: {e}")

class RiotAPIClient:
    """
    Production-ready Riot API client with rate limiting
    """

    REGIONAL_HOSTS = {
        "americas": "https://americas.api.riotgames.com",
        "europe": "https://europe.api.riotgames.com",
        "asia": "https://asia.api.riotgames.com",
        "sea": "https://sea.api.riotgames.com"
    }

    PLATFORM_TO_REGION = {
        "BR1": "americas", "NA1": "americas", "LA1": "americas", "LA2": "americas",
        "EUW1": "europe", "EUN1": "europe", "TR1": "europe", "RU": "europe",
        "KR": "asia", "JP1": "asia",
        "OC1": "sea", "PH2": "sea", "SG2": "sea", "TH2": "sea", "TW2": "sea", "VN2": "sea"
    }

    def __init__(self, api_key: str = None, is_production: bool = False, cache_dir: str = None):
        self.api_key = api_key or os.getenv("RIOT_API_KEY")
        if not self.api_key:
            raise ValueError("RIOT_API_KEY not provided")

        self.rate_limiter = RiotRateLimiter(is_production=is_production)
        default_cache = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "cache"
        )
        self.cache_dir = cache_dir or os.getenv("CACHE_DIR", default_cache)

        # Create cache directory
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(f"{self.cache_dir}/matches", exist_ok=True)
        os.makedirs(f"{self.cache_dir}/timelines", exist_ok=True)

        # Statistics
        self.stats = {
            'requests_made': 0,
            'requests_cached': 0,
            'rate_limit_waits': 0,
            'errors': 0
        }

        logger.info(f"RiotAPIClient initialized with cache at {self.cache_dir}")

    def _get_headers(self) -> dict:
        """Get request headers"""
        return {
            "X-Riot-Token": self.api_key,
            "Accept": "application/json",
            "User-Agent": "ProStaff-Scraper/1.0"
        }

    def _get_cache_path(self, cache_type: str, key: str) -> str:
        """Get cache file path"""
        return f"{self.cache_dir}/{cache_type}/{key}.json"

    def _load_from_cache(self, cache_type: str, key: str) -> Optional[dict]:
        """Load data from cache if exists and is fresh"""
        cache_path = self._get_cache_path(cache_type, key)

        if os.path.exists(cache_path):
            # Check if cache is fresh (24 hours for match data)
            file_age = time.time() - os.path.getmtime(cache_path)
            if file_age < 86400:  # 24 hours
                try:
                    with open(cache_path, 'r') as f:
                        self.stats['requests_cached'] += 1
                        logger.debug(f"Cache hit for {cache_type}/{key}")
                        return json.load(f)
                except Exception as e:
                    logger.error(f"Error loading cache {cache_path}: {e}")

        return None

    def _save_to_cache(self, cache_type: str, key: str, data: dict):
        """Save data to cache"""
        cache_path = self._get_cache_path(cache_type, key)
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Cached {cache_type}/{key}")
        except Exception as e:
            logger.error(f"Error saving cache {cache_path}: {e}")

    @retry(
        retry=retry_if_exception_type(RateLimitException),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    def _make_request(self, url: str, cache_type: str = None, cache_key: str = None) -> dict:
        """Make a rate-limited request to Riot API"""

        # Check cache first
        if cache_type and cache_key:
            cached = self._load_from_cache(cache_type, cache_key)
            if cached:
                return cached

        # Wait for rate limit if needed
        can_request, wait_time = self.rate_limiter.can_make_request()
        if not can_request:
            self.stats['rate_limit_waits'] += 1
            logger.info(f"Rate limit reached, waiting {wait_time:.2f}s...")
            time.sleep(wait_time + 0.1)  # Add small buffer
            # Retry
            can_request, wait_time = self.rate_limiter.can_make_request()
            if not can_request:
                raise RateLimitException(f"Still rate limited after waiting {wait_time}s")

        # Make request
        try:
            self.rate_limiter.record_request()
            self.stats['requests_made'] += 1

            with httpx.Client(timeout=30) as client:
                response = client.get(url, headers=self._get_headers())

                # Update rate limits from headers
                self.rate_limiter.update_from_headers(dict(response.headers))

                # Handle different status codes
                if response.status_code == 200:
                    data = response.json()

                    # Cache successful response
                    if cache_type and cache_key:
                        self._save_to_cache(cache_type, cache_key, data)

                    return data

                elif response.status_code == 429:
                    # Rate limited
                    retry_after = response.headers.get('Retry-After', '10')
                    logger.warning(f"Rate limited (429), retry after {retry_after}s")
                    raise RateLimitException(f"Rate limited, retry after {retry_after}s")

                elif response.status_code == 404:
                    logger.warning(f"Not found (404): {url}")
                    return None

                else:
                    self.stats['errors'] += 1
                    logger.error(f"API error {response.status_code}: {response.text}")
                    response.raise_for_status()

        except httpx.TimeoutException:
            self.stats['errors'] += 1
            logger.error(f"Request timeout for {url}")
            raise
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Request error for {url}: {e}")
            raise

    def get_match_details(self, match_id: str, platform: str = "BR1") -> Optional[dict]:
        """Get match details with caching and rate limiting"""
        region = self.PLATFORM_TO_REGION.get(platform, "americas")
        url = f"{self.REGIONAL_HOSTS[region]}/lol/match/v5/matches/{match_id}"

        logger.info(f"Fetching match {match_id} from {region}")
        return self._make_request(url, cache_type="matches", cache_key=match_id)

    def get_timeline(self, match_id: str, platform: str = "BR1") -> Optional[dict]:
        """Get match timeline with caching and rate limiting"""
        region = self.PLATFORM_TO_REGION.get(platform, "americas")
        url = f"{self.REGIONAL_HOSTS[region]}/lol/match/v5/matches/{match_id}/timeline"

        logger.info(f"Fetching timeline for {match_id} from {region}")
        return self._make_request(url, cache_type="timelines", cache_key=f"{match_id}_timeline")

    def get_match_list(self, puuid: str, platform: str = "BR1", count: int = 20) -> Optional[list]:
        """Get match list for a player"""
        region = self.PLATFORM_TO_REGION.get(platform, "americas")
        url = f"{self.REGIONAL_HOSTS[region]}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        url += f"?count={count}"

        logger.info(f"Fetching match list for {puuid}")
        # Don't cache match lists as they change frequently
        return self._make_request(url)

    def print_stats(self):
        """Print usage statistics"""
        print("\n" + "="*60)
        print("RIOT API CLIENT STATISTICS")
        print("="*60)
        print(f"Requests made: {self.stats['requests_made']}")
        print(f"Cache hits: {self.stats['requests_cached']}")
        cache_rate = (self.stats['requests_cached'] / max(1, self.stats['requests_made'] + self.stats['requests_cached'])) * 100
        print(f"Cache hit rate: {cache_rate:.1f}%")
        print(f"Rate limit waits: {self.stats['rate_limit_waits']}")
        print(f"Errors: {self.stats['errors']}")
        print("="*60 + "\n")

# Global client instance (singleton pattern)
_client = None

def get_riot_client(is_production: bool = False) -> RiotAPIClient:
    """Get or create the global Riot API client"""
    global _client
    if _client is None:
        _client = RiotAPIClient(is_production=is_production)
    return _client

def get_match_details(match_id: str, platform: str = "BR1") -> Optional[dict]:
    """Convenience function for getting match details"""
    client = get_riot_client()
    return client.get_match_details(match_id, platform)

def get_timeline(match_id: str, platform: str = "BR1") -> Optional[dict]:
    """Convenience function for getting match timeline"""
    client = get_riot_client()
    return client.get_timeline(match_id, platform)
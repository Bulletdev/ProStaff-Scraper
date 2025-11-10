import os
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

REGIONAL_HOSTS = {
    "americas": "https://americas.api.riotgames.com",
    "europe": "https://europe.api.riotgames.com",
    "asia": "https://asia.api.riotgames.com",
}

AMERICAS = {"NA1", "BR1", "LA1", "LA2", "OC1"}
EUROPE = {"EUW1", "EUN1", "TR1", "RU"}
ASIA = {"KR", "JP1"}


def riot_headers():
    return {
        "X-Riot-Token": os.getenv("RIOT_API_KEY", ""),
        "Accept": "application/json",
    }


def regional_endpoint(platform_region: str) -> str:
    if platform_region in EUROPE:
        return "europe"
    if platform_region in ASIA:
        return "asia"
    return "americas"


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=16))
def get_match_details(match_id: str, platform_region: str) -> dict:
    region = regional_endpoint(platform_region)
    base = REGIONAL_HOSTS[region]
    url = f"{base}/lol/match/v5/matches/{match_id}"
    with httpx.Client(timeout=30) as client:
        r = client.get(url, headers=riot_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=16))
def get_timeline(match_id: str, platform_region: str) -> dict:
    region = regional_endpoint(platform_region)
    base = REGIONAL_HOSTS[region]
    url = f"{base}/lol/match/v5/matches/{match_id}/timeline"
    with httpx.Client(timeout=30) as client:
        r = client.get(url, headers=riot_headers())
        r.raise_for_status()
        return r.json()
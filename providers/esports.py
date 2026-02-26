import os
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://esports-api.lolesports.com/persisted/gw"
# hl (locale) is required by all LoL Esports API endpoints
HL = "en-US"


def _headers():
    return {
        "x-api-key": os.getenv("ESPORTS_API_KEY", ""),
        "Accept": "application/json",
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_leagues():
    url = f"{BASE_URL}/getLeagues?hl={HL}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_schedule(league_id: str):
    url = f"{BASE_URL}/getSchedule?hl={HL}&leagueId={league_id}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_completed_events(league_id: str):
    """Fetch completed events for a league.

    Returns up to 300 completed series with team results and per-game VOD IDs.
    NOTE: game IDs here are LoL Esports internal snowflake IDs, NOT Riot Match-V5 IDs.
    There is no public endpoint to map these IDs to Riot Match-V5 IDs.
    """
    url = f"{BASE_URL}/getCompletedEvents?hl={HL}&leagueId={league_id}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_tournaments(league_id: str):
    """Fetch all tournaments for a league."""
    url = f"{BASE_URL}/getTournaments?hl={HL}&leagueId={league_id}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_event_details(event_id: str):
    """Fetch event details by event ID.

    NOTE: As of 2026, this endpoint returns 'Invalid request parameters'
    for all known ID formats (match.id from getSchedule, game.id from
    getCompletedEvents). The endpoint may require an internal ID format
    not exposed in the public API. Use get_completed_events instead.
    """
    url = f"{BASE_URL}/getEventDetails?hl={HL}&eventId={event_id}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()

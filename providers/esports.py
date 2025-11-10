import os
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://esports-api.lolesports.com/persisted/gw"


def _headers():
    return {
        "x-api-key": os.getenv("ESPORTS_API_KEY", ""),
        "Accept": "application/json",
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_leagues():
    url = f"{BASE_URL}/getLeagues"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_schedule(league_id: str):
    url = f"{BASE_URL}/getSchedule?leagueId={league_id}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def get_event_details(event_id: str):
    url = f"{BASE_URL}/getEventDetails?eventId={event_id}"
    with httpx.Client(timeout=20) as client:
        r = client.get(url, headers=_headers())
        r.raise_for_status()
        return r.json()
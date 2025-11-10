import os
import argparse
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv

from scraper.providers.esports import get_leagues, get_schedule, get_event_details
from scraper.providers.riot import get_match_details, get_timeline
from scraper.indexers.elasticsearch_client import ensure_index, bulk_index
from scraper.indexers.mappings import MATCHES_MAPPING, TIMELINE_MAPPING


def find_league_id(name: str) -> str:
    data = get_leagues()
    leagues = data.get("data", {}).get("leagues", [])
    for lg in leagues:
        if lg.get("name", "").lower() == name.lower():
            return lg.get("id")
    raise RuntimeError(f"League not found: {name}")


def compose_match_id(platform_id: str, game_id: str) -> str:
    return f"{platform_id}_{game_id}"


def normalize_match(match_json: Dict) -> Dict:
    info = match_json.get("info", {})
    meta = match_json.get("metadata", {})
    platform_id = meta.get("platformId", os.getenv("DEFAULT_PLATFORM_REGION", "BR1"))
    start_ms = info.get("gameStartTimestamp")
    start = datetime.utcfromtimestamp(start_ms / 1000) if start_ms else None

    participants = []
    for p in info.get("participants", []):
        kda = (p.get("kills", 0) + p.get("assists", 0)) / max(1, p.get("deaths", 0))
        participants.append(
            {
                "puuid": p.get("puuid"),
                "summoner_name": p.get("summonerName"),
                "team": "BLUE" if p.get("teamId") == 100 else "RED",
                "role": p.get("individualPosition"),
                "champion": p.get("championName"),
                "kda": round(kda, 2),
                "cs": p.get("totalMinionsKilled", 0) + p.get("neutralMinionsKilled", 0),
                "gold": p.get("goldEarned", 0),
                "dmg": p.get("totalDamageDealtToChampions", 0),
            }
        )

    teams = []
    for t in info.get("teams", []):
        teams.append(
            {
                "id": t.get("teamId"),
                "name": None,
                "result": "win" if t.get("win") else "loss",
                "dragons": len([o for o in t.get("objectives", {}).get("dragon", {}).get("kills", [])])
                if isinstance(t.get("objectives", {}).get("dragon", {}).get("kills", []), list)
                else t.get("objectives", {}).get("dragon", {}).get("kills", 0),
                "barons": t.get("objectives", {}).get("baron", {}).get("kills", 0),
                "towers": t.get("objectives", {}).get("tower", {}).get("kills", 0),
            }
        )

    return {
        "_id": meta.get("matchId"),
        "match_id": meta.get("matchId"),
        "league": "CBLOL",
        "split": None,
        "stage": None,
        "platform_id": platform_id,
        "regional_endpoint": None,
        "game_start": start.isoformat() if start else None,
        "patch": info.get("gameVersion"),
        "teams": teams,
        "participants": participants,
    }


def pipeline(league_name: str, limit: int = 50):
    ensure_index("lol_pro_matches", MATCHES_MAPPING)
    ensure_index("lol_timelines", TIMELINE_MAPPING)

    league_id = find_league_id(league_name)
    schedule = get_schedule(league_id)
    events = schedule.get("data", {}).get("schedule", {}).get("events", [])

    docs_matches: List[Dict] = []

    count = 0
    for ev in events:
        if count >= limit:
            break
        ev_id = ev.get("id")
        details = get_event_details(ev_id)
        series = details.get("data", {}).get("event", {}).get("match", {}).get("games", [])
        for g in series:
            game_id = g.get("id") or g.get("gameId")
            platform_id = os.getenv("DEFAULT_PLATFORM_REGION", "BR1")
            if not game_id:
                continue
            match_id = compose_match_id(platform_id, game_id)

            try:
                md = get_match_details(match_id, platform_id)
                docs_matches.append(normalize_match(md))
                count += 1
                if count >= limit:
                    break
            except Exception as e:
                print(f"Failed {match_id}: {e}")

    if docs_matches:
        bulk_index("lol_pro_matches", docs_matches)
        print(f"Indexed matches: {len(docs_matches)}")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", default="CBLOL")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    pipeline(args.league, args.limit)


if __name__ == "__main__":
    main()
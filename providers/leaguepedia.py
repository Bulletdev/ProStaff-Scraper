"""
Leaguepedia (lol.fandom.com) Cargo API client.

Leaguepedia receives official esports match data from Riot's data disclosure
program and maintains a public MediaWiki/Cargo database.

Two tables are used:
  - ScoreboardGames: game-level data (winner, patch, duration, team names)
  - ScoreboardPlayers: per-player stats (champion, KDA, items, runes, gold)

This approach bypasses Riot Match-V5 entirely:
  Competitive games are on Riot's internal tournament servers and are NOT
  accessible via the public Match-V5 API. Leaguepedia is the canonical
  source for this data.

Rate limit: anonymous access is approximately 1 request per 8-9 seconds.
All callers must sleep RATE_LIMIT_SECONDS between sequential requests.
"""

import time
import logging
from typing import Optional, Dict, List, Any

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://lol.fandom.com/api.php"

# Anonymous rate limit observed in practice: ~1 req / 10-12s.
# Using 12s to avoid consecutive-request bursts that trigger longer cooldowns.
RATE_LIMIT_SECONDS = 12.0

# Longer cooldown used between tournaments during backfill to avoid
# accumulating rate-limit debt across many sequential queries.
BACKFILL_COOLDOWN_SECONDS = 30.0

_HEADERS = {
    "User-Agent": "ProStaff-Scraper/1.0 (competitive data research; non-commercial)",
    "Accept": "application/json",
}


class LeaguepediaRateLimitError(Exception):
    """Raised when Leaguepedia returns a rate-limit error.

    This is a distinct exception so callers can differentiate between
    'no data exists' (empty result set) and 'request was rejected'
    (rate limited — should retry after a longer cooldown).
    """
    pass


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=RATE_LIMIT_SECONDS, min=RATE_LIMIT_SECONDS, max=120),
    retry=retry_if_exception_type((LeaguepediaRateLimitError, httpx.HTTPStatusError, httpx.ConnectError)),
)
def _cargo_query(params: Dict) -> Dict:
    """Execute a single Cargo API query with retry on rate limit / transient failures.

    Uses exponential backoff: 12s -> 24s -> 48s -> 96s -> 120s (capped).
    This prevents accumulated rate-limit debt when running bulk imports.

    Raises:
        LeaguepediaRateLimitError: when all retries are exhausted on rate limit.
    """
    base_params = {
        "action": "cargoquery",
        "format": "json",
    }
    base_params.update(params)

    with httpx.Client(timeout=20) as client:
        r = client.get(BASE_URL, params=base_params, headers=_HEADERS)
        r.raise_for_status()
        data = r.json()

    if "error" in data:
        code = data["error"].get("code", "")
        info = data["error"].get("info", "")
        if code == "ratelimited":
            logger.warning(f"Leaguepedia rate limited, will retry with backoff: {info}")
            raise LeaguepediaRateLimitError(
                f"Leaguepedia rate limited: {info}"
            )
        logger.warning(f"Leaguepedia API error: {code} - {info}")
        return {}

    return data


def _parse_items(raw: str) -> List[str]:
    """Parse semicolon-separated item names into a list (empty slots removed)."""
    if not raw:
        return []
    return [item.strip() for item in raw.split(";") if item.strip()]


def _parse_runes(raw: str) -> Dict[str, Any]:
    """Parse comma-separated rune string into structured rune data.

    Leaguepedia rune format (9 entries):
      [Keystone, Row2, Row3, Row4, Secondary1, Secondary2, Shard1, Shard2, Shard3]
    """
    if not raw:
        return {"keystone": None, "primary_runes": [], "secondary_runes": [], "stat_shards": []}

    parts = [r.strip() for r in raw.split(",")]

    # Guard against malformed data
    while len(parts) < 9:
        parts.append("")

    return {
        "keystone": parts[0] or None,
        "primary_runes": [p for p in parts[1:4] if p],   # rows 2-4 of primary tree
        "secondary_runes": [p for p in parts[4:6] if p],  # 2 from secondary tree
        "stat_shards": [p for p in parts[6:9] if p],      # 3 stat shards
    }


def _parse_summoner_spells(raw: str) -> List[str]:
    """Parse comma-separated summoner spell names."""
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_scoreboard_schema() -> List[str]:
    """Return all column names available in the ScoreboardPlayers Cargo table.

    Use this once to verify which optional fields (VisionScore, DamageTaken,
    WardsPlaced, WardsKilled) are exposed by the current Leaguepedia schema
    before adding them to the production query.

    Usage::

        from providers.leaguepedia import get_scoreboard_schema
        import json
        print(json.dumps(get_scoreboard_schema(), indent=2))
    """
    try:
        data = _cargo_query({
            "tables": "ScoreboardPlayers",
            "fields": "*",
            "limit": "1",
        })
    except Exception as e:
        logger.error(f"get_scoreboard_schema failed: {e}")
        return []

    results = data.get("cargoquery", [])
    if not results:
        return []

    return list(results[0].get("title", {}).keys())


def get_game_scoreboard(
    team1_name: str,
    team2_name: str,
    date_utc: str,
    game_number: int,
) -> Optional[Dict]:
    """Fetch game-level data from Leaguepedia ScoreboardGames.

    Args:
        team1_name:  Team name as returned by LoL Esports API (e.g. "paiN Gaming")
        team2_name:  Other team name
        date_utc:    Date string in YYYY-MM-DD format
        game_number: 1-indexed game number within the series

    Returns:
        Dict with keys: page_name, win_team, team1, team2, patch, gamelength,
        gamelength_number — or None if no record found.
    """
    t1 = team1_name.replace("'", "\\'")
    t2 = team2_name.replace("'", "\\'")

    where = (
        f"(Team1='{t1}' AND Team2='{t2}' OR Team1='{t2}' AND Team2='{t1}')"
        f" AND DateTime_UTC LIKE '{date_utc}%'"
        f" AND N_GameInMatch={game_number}"
    )

    try:
        data = _cargo_query({
            "tables": "ScoreboardGames",
            "fields": "GameId,WinTeam,Team1,Team2,Patch,Gamelength,DateTime_UTC",
            "where": where,
            "limit": "5",
            "order_by": "DateTime_UTC ASC",
        })
    except Exception as e:
        logger.error(
            f"Leaguepedia ScoreboardGames query failed for "
            f"{team1_name} vs {team2_name} G{game_number}: {e}"
        )
        return None

    results = data.get("cargoquery", [])
    if not results:
        logger.debug(
            f"Leaguepedia: no game record for {team1_name} vs {team2_name} "
            f"G{game_number} on {date_utc}"
        )
        return None

    row = results[0].get("title", {})
    page_name = row.get("GameId", "")

    if not page_name:
        logger.warning(
            f"Leaguepedia: empty GameId for {team1_name} vs {team2_name} G{game_number}"
        )
        return None

    gamelength_str = row.get("Gamelength", "")
    gamelength_seconds = _parse_gamelength(gamelength_str)

    logger.info(
        f"Leaguepedia game found: {page_name} | winner={row.get('WinTeam')} "
        f"patch={row.get('Patch')} duration={row.get('Gamelength')}"
    )

    return {
        "page_name": page_name,
        "win_team": row.get("WinTeam", ""),
        "team1": row.get("Team1", ""),
        "team2": row.get("Team2", ""),
        "patch": row.get("Patch", ""),
        "gamelength": row.get("Gamelength", ""),
        "gamelength_seconds": gamelength_seconds,
        "datetime_utc": row.get("DateTime UTC", ""),
    }


def get_game_players(page_name: str, game_duration_seconds: int = 0) -> List[Dict]:
    """Fetch per-player stats from Leaguepedia ScoreboardPlayers.

    Args:
        page_name:              The Leaguepedia GameId
                                (e.g. "CBLOL/2026 Season/Cup_Play-In Last Chance_1_2")
        game_duration_seconds:  Total game duration in seconds.  When provided,
                                derived per-minute fields (cs_per_min, gold_per_min,
                                damage_per_min) are computed and stored alongside the
                                raw stats.  Pass 0 (default) to skip computation.

    Returns:
        List of player dicts with champion, KDA, items, runes, summoner spells, role,
        and optional extended stats (damage_taken, vision_score, wards_placed,
        wards_killed) when available from Leaguepedia.
        Empty list if not found.

    Note on optional fields:
        DamageTaken, VisionScore, WardsPlaced and WardsKilled are included in the
        Cargo query but may be absent from older tournament records.  Missing values
        are stored as 0 rather than None so downstream code can do arithmetic safely.
    """
    page_name_escaped = page_name.replace("'", "\\'")

    try:
        data = _cargo_query({
            "tables": "ScoreboardPlayers",
            "fields": (
                "GameId,Name,Team,Champion,Role,"
                "Kills,Deaths,Assists,Gold,CS,DamageToChampions,"
                "DamageTaken,VisionScore,WardsPlaced,WardsKilled,"
                "Items,Runes,SummonerSpells"
            ),
            "where": f"GameId='{page_name_escaped}'",
            "limit": "10",
        })
    except Exception as e:
        logger.error(f"Leaguepedia ScoreboardPlayers query failed for {page_name}: {e}")
        return []

    results = data.get("cargoquery", [])
    if not results:
        logger.debug(f"Leaguepedia: no player records for {page_name}")
        return []

    game_minutes = game_duration_seconds / 60.0 if game_duration_seconds > 0 else 0.0

    players = []
    for entry in results:
        row = entry.get("title", {})

        rune_data = _parse_runes(row.get("Runes", ""))

        cs     = _safe_int(row.get("CS"))
        gold   = _safe_int(row.get("Gold"))
        damage = _safe_int(row.get("DamageToChampions"))

        player_data: Dict[str, Any] = {
            "summoner_name":   row.get("Name", ""),
            "team_name":       row.get("Team", ""),
            "champion_name":   row.get("Champion", ""),
            "role":            row.get("Role", ""),
            "kills":           _safe_int(row.get("Kills")),
            "deaths":          _safe_int(row.get("Deaths")),
            "assists":         _safe_int(row.get("Assists")),
            "gold":            gold,
            "cs":              cs,
            "damage":          damage,
            # Extended stats — present when Leaguepedia exposes them
            "damage_taken":    _safe_int(row.get("DamageTaken")),
            "vision_score":    _safe_int(row.get("VisionScore")),
            "wards_placed":    _safe_int(row.get("WardsPlaced")),
            "wards_killed":    _safe_int(row.get("WardsKilled")),
            "items":           _parse_items(row.get("Items", "")),
            "summoner_spells": _parse_summoner_spells(row.get("SummonerSpells", "")),
            "keystone":        rune_data["keystone"],
            "primary_runes":   rune_data["primary_runes"],
            "secondary_runes": rune_data["secondary_runes"],
            "stat_shards":     rune_data["stat_shards"],
        }

        # Derived per-minute fields (only when duration is known)
        if game_minutes > 0:
            player_data["cs_per_min"]     = round(cs     / game_minutes, 2)
            player_data["gold_per_min"]   = round(gold   / game_minutes, 2)
            player_data["damage_per_min"] = round(damage / game_minutes, 2)

        players.append(player_data)

    logger.info(f"Leaguepedia: fetched {len(players)} players for {page_name}")
    return players


def get_game_data(
    team1_name: str,
    team2_name: str,
    date_utc: str,
    game_number: int,
) -> Optional[Dict]:
    """Fetch complete game data (game stats + all 10 players) from Leaguepedia.

    Makes two sequential requests (ScoreboardGames, ScoreboardPlayers) with
    RATE_LIMIT_SECONDS sleep between them.

    Args:
        team1_name:  Team name as returned by LoL Esports API
        team2_name:  Other team name
        date_utc:    Date string in YYYY-MM-DD format
        game_number: 1-indexed game number within the series

    Returns:
        Dict with game-level stats and 'players' list, or None if game not found.
    """
    # Request 1: game-level stats
    game_info = get_game_scoreboard(team1_name, team2_name, date_utc, game_number)
    if not game_info:
        return None

    # Rate limit between the two requests
    time.sleep(RATE_LIMIT_SECONDS)

    # Request 2: player stats (pass duration so derived per-min fields are computed)
    players = get_game_players(
        game_info["page_name"],
        game_duration_seconds=game_info.get("gamelength_seconds", 0),
    )
    if not players:
        logger.warning(
            f"Leaguepedia: game record exists ({game_info['page_name']}) "
            f"but no player records found yet"
        )
        return None

    win_team = game_info["win_team"]

    # Annotate each player with win flag
    for player in players:
        player["win"] = (player["team_name"] == win_team)

    return {
        "page_name": game_info["page_name"],
        "win_team": win_team,
        "team1": game_info["team1"],
        "team2": game_info["team2"],
        "patch": game_info["patch"],
        "gamelength": game_info["gamelength"],
        "gamelength_seconds": game_info["gamelength_seconds"],
        "players": players,
    }


def _safe_int(value: Any) -> int:
    """Safely convert a value to int, returning 0 on failure."""
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _parse_gamelength(gamelength: str) -> int:
    """Parse Leaguepedia Gamelength field (MM:SS format) to total seconds."""
    if not gamelength:
        return 0
    try:
        parts = gamelength.strip().split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        pass
    return 0


def get_league_tournaments(league_prefix: str, min_year: int = 2013) -> List[Dict]:
    """Fetch all tournament OverviewPages for a league from Leaguepedia.

    Queries the Tournaments cargo table filtering by OverviewPage prefix
    (e.g. "CBLOL/") to discover every edition ever recorded.

    Args:
        league_prefix: OverviewPage prefix, e.g. "CBLOL" (without trailing slash).
        min_year:      Ignore tournaments before this year (default 2013, CBLOL S1).

    Returns:
        List of dicts sorted by DateStart ascending:
          {
            "overview_page": str,   # e.g. "CBLOL/2025 Season/Split 1"
            "name":          str,   # e.g. "CBLOL 2025 Split 1"
            "date_start":    str,   # "YYYY-MM-DD"
            "date_end":      str,   # "YYYY-MM-DD"
            "region":        str,
            "year":          int,
          }
    """
    escaped_prefix = league_prefix.replace("'", "\\'")
    all_rows: List[Dict] = []
    offset = 0
    page_size = 100

    logger.info(f"Discovering tournaments for league prefix='{league_prefix}' (min_year={min_year})...")

    while True:
        try:
            data = _cargo_query({
                "tables": "Tournaments",
                "fields": "OverviewPage,Name,DateStart,Date,Region,Year",
                "where": (
                    f"OverviewPage LIKE '{escaped_prefix}/%'"
                    f" AND Year >= {min_year}"
                ),
                "limit": str(page_size),
                "offset": str(offset),
                "order_by": "DateStart ASC",
            })
        except Exception as e:
            logger.error(f"Tournaments query failed at offset {offset}: {e}")
            break

        rows = data.get("cargoquery", [])
        if not rows:
            break

        for r in rows:
            t = r.get("title", {})
            overview_page = t.get("OverviewPage", "").strip()
            if not overview_page:
                continue
            all_rows.append({
                "overview_page": overview_page,
                "name": t.get("Name", "").strip(),
                "date_start": t.get("DateStart", "").strip(),
                "date_end": t.get("Date", "").strip(),
                "region": t.get("Region", "").strip(),
                "year": _safe_int(t.get("Year")),
            })

        logger.info(f"  Fetched {len(rows)} tournament rows (total so far: {len(all_rows)})")

        if len(rows) < page_size:
            break

        offset += page_size
        time.sleep(RATE_LIMIT_SECONDS)

    logger.info(f"Discovered {len(all_rows)} tournaments for '{league_prefix}'")
    return all_rows

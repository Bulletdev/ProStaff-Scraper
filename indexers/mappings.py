"""
Elasticsearch index mappings for ProStaff Scraper.

NOTE: Mappings cannot be changed on an existing index without reindexing.
If you update this file you must also recreate the index:
  $ DELETE lol_pro_matches
  The pipeline will recreate it via ensure_index() on next run.
"""

MATCHES_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            # Series identifiers
            "match_id": {"type": "keyword"},
            "game_id": {"type": "keyword"},
            "game_number": {"type": "integer"},
            # Classification
            "league": {"type": "keyword"},
            "stage": {"type": "keyword"},
            "best_of": {"type": "integer"},
            # Timing
            "start_time": {"type": "date"},
            "indexed_at": {"type": "date"},
            # Teams (series-level result from LoL Esports)
            "winner_code": {"type": "keyword"},
            "team1": {
                "properties": {
                    "name": {"type": "keyword"},
                    "code": {"type": "keyword"},
                    "image": {"type": "keyword", "index": False},
                    "game_wins": {"type": "integer"},
                }
            },
            "team2": {
                "properties": {
                    "name": {"type": "keyword"},
                    "code": {"type": "keyword"},
                    "image": {"type": "keyword", "index": False},
                    "game_wins": {"type": "integer"},
                }
            },
            # VOD
            "vod_youtube_id": {"type": "keyword"},
            # Enrichment metadata
            "riot_enriched": {"type": "boolean"},
            "enrichment_source": {"type": "keyword"},   # "leaguepedia"
            "enrichment_attempts": {"type": "integer"},
            "last_enrichment_attempt": {"type": "date"},
            "enriched_at": {"type": "date"},
            # Historical metadata (Leaguepedia-sourced games only)
            "year": {"type": "integer"},                 # e.g. 2023
            "split_event": {"type": "keyword"},          # e.g. "Split 1", "Playoffs", "Cup"
            "tournament_name": {"type": "keyword"},      # e.g. "2023 Season Split 1"
            # Game-level stats (from Leaguepedia ScoreboardGames)
            "leaguepedia_page": {"type": "keyword"},     # Leaguepedia internal page name
            "patch": {"type": "keyword"},
            "win_team": {"type": "keyword"},
            "gamelength": {"type": "keyword"},           # human-readable "MM:SS"
            "game_duration_seconds": {"type": "integer"},
            # Per-game participant stats from Leaguepedia ScoreboardPlayers
            # All fields use names (strings), not Riot integer IDs
            "participants": {
                "type": "nested",
                "properties": {
                    "summoner_name": {"type": "keyword"},
                    "team_name": {"type": "keyword"},
                    "champion_name": {"type": "keyword"},
                    "role": {"type": "keyword"},
                    "kills": {"type": "integer"},
                    "deaths": {"type": "integer"},
                    "assists": {"type": "integer"},
                    "cs": {"type": "integer"},
                    "gold": {"type": "integer"},
                    "damage": {"type": "integer"},
                    "win": {"type": "boolean"},
                    # Items: list of item name strings (empty slots excluded)
                    "items": {"type": "keyword"},
                    # Summoner spells: list of spell name strings
                    "summoner_spells": {"type": "keyword"},
                    # Runes: keystone + per-row breakdown (all name strings)
                    "keystone": {"type": "keyword"},
                    "primary_runes": {"type": "keyword"},
                    "secondary_runes": {"type": "keyword"},
                    "stat_shards": {"type": "keyword"},
                },
            },
        }
    },
}

TIMELINE_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "match_id": {"type": "keyword"},
            "events": {
                "type": "nested",
                "properties": {
                    "timestamp": {"type": "long"},
                    "type": {"type": "keyword"},
                    "participant": {"type": "integer"},
                    "position": {
                        "properties": {
                            "x": {"type": "integer"},
                            "y": {"type": "integer"},
                        },
                    },
                    "objective": {"type": "keyword"},
                },
            },
        }
    },
}

MATCHES_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "match_id": {"type": "keyword"},
            "league": {"type": "keyword"},
            "split": {"type": "keyword"},
            "stage": {"type": "keyword"},
            "platform_id": {"type": "keyword"},
            "regional_endpoint": {"type": "keyword"},
            "game_start": {"type": "date"},
            "patch": {"type": "keyword"},
            "teams": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "result": {"type": "keyword"},
                    "dragons": {"type": "integer"},
                    "barons": {"type": "integer"},
                    "towers": {"type": "integer"},
                },
            },
            "participants": {
                "type": "nested",
                "properties": {
                    "puuid": {"type": "keyword"},
                    "summoner_name": {"type": "keyword"},
                    "team": {"type": "keyword"},
                    "role": {"type": "keyword"},
                    "champion": {"type": "keyword"},
                    "kda": {"type": "float"},
                    "cs": {"type": "integer"},
                    "gold": {"type": "integer"},
                    "dmg": {"type": "integer"},
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
                        "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
                    },
                    "objective": {"type": "keyword"},
                },
            },
        }
    },
}
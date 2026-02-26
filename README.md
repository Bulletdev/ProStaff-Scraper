![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.x-005571?logo=elasticsearch)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)

# ProStaff Scraper - Professional Match Data API

> FastAPI service that collects and serves League of Legends professional match data.
> Fetches schedules from LoL Esports API, enriches with per-player stats from Leaguepedia,
> and stores everything in Elasticsearch for fast REST queries.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [API Endpoints](#api-endpoints)
- [Quick Start](#quick-start)
- [Production Deployment](#production-deployment)
- [Stack](#stack)
- [File Structure](#file-structure)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Features

- **FastAPI REST API** — serve professional match data via HTTP endpoints
- **Two-phase pipeline** — sync (LoL Esports) + background enrichment (Leaguepedia)
- **Full player stats** — champion, KDA, gold, CS, items (names), runes (names), summoner spells
- **Leaguepedia integration** — only public source for competitive game data (Riot Match-V5 does not expose tournament server games)
- **Enrichment daemon** — background job processes pending games every 30 minutes, respects rate limits
- **Deduplication** — `riot_enriched` flag prevents re-processing; `enrichment_attempts` counter abandons after 3 failures
- **Multi-league** — CBLOL, LCS, LEC, LCK, LPL, and more
- **Production ready** — Docker Compose with Traefik/SSL for Coolify deployment

---

## Architecture

The system runs in two independent phases:

```
Phase 1 — Sync (scraper-cron, every 1h)
  LoL Esports API
    └─ getCompletedEvents → series with games + YouTube VOD IDs
         └─ competitive_pipeline.py
              └─ bulk_index → ES (riot_enriched: false)

Phase 2 — Enrichment (enrichment-daemon, every 30min)
  query_unenriched(ES) → pending games
    └─ For each game (2 Leaguepedia requests + 9s sleep each):
         1. ScoreboardGames  → page_name, winner, patch, gamelength
         2. ScoreboardPlayers → 10 players with champion/KDA/items/runes
         └─ update_document(ES, riot_enriched: true, participants: [...])
```

Why Leaguepedia instead of Riot Match-V5: competitive games run on Riot's internal
tournament servers and **do not appear in the public Match-V5 API**. Leaguepedia
receives official data from Riot's esports disclosure program and is the only
public source for these stats.

For the full architecture diagram and detailed flow, see [`docs/Arquitetura.md`](./docs/Arquitetura.md).

---

## API Endpoints

### Public

```bash
GET /health                        # Health check (Elasticsearch connectivity)
GET /                              # Service info
GET /api/v1/leagues                # List leagues from LoL Esports
GET /api/v1/matches?league=CBLOL   # Query matches (paginated)
GET /api/v1/matches/{match_id}     # Single match with full participant stats
GET /api/v1/stats/leagues          # Match count per league
```

### Protected (requires `X-API-Key` header)

```bash
POST /api/v1/sync?league=CBLOL&limit=50        # Trigger manual sync
POST /api/v1/enrich?batch=10                   # Trigger background enrichment
GET  /api/v1/enrich/status                     # Enrichment progress (pending/enriched counts)
```

### Example — Enriched Match

`GET /api/v1/matches/115565621821672075_2`

```json
{
  "match_id": "115565621821672075",
  "game_number": 2,
  "league": "CBLOL",
  "patch": "26.02",
  "win_team": "Leviatan",
  "gamelength": "32:43",
  "game_duration_seconds": 1963,
  "riot_enriched": true,
  "participants": [
    {
      "summoner_name": "tinowns",
      "team_name": "paiN Gaming",
      "champion_name": "Ahri",
      "role": "Mid",
      "kills": 4, "deaths": 1, "assists": 3,
      "gold": 14320, "cs": 245, "damage": 22100,
      "win": false,
      "items": ["Rabadon's Deathcap", "Shadowflame", "Void Staff"],
      "keystone": "Electrocute",
      "primary_runes": ["Cheap Shot", "Eyeball Collection", "Treasure Hunter"],
      "secondary_runes": ["Presence of Mind", "Cut Down"],
      "stat_shards": ["Adaptive Force", "Adaptive Force", "Health"],
      "summoner_spells": ["Flash", "Ignite"]
    }
  ]
}
```

See full Swagger UI at `https://scraper.prostaff.gg/docs`

---

## Quick Start

```bash
# 1. Copy and configure environment
cp .env.example .env
# Edit .env: add RIOT_API_KEY, ESPORTS_API_KEY, SCRAPER_API_KEY

# 2. Start services (Elasticsearch + API + enrichment daemon)
docker compose up -d

# 3. Verify health
curl http://localhost:8000/health

# 4. Sync CBLOL matches
curl -X POST "http://localhost:8000/api/v1/sync?league=CBLOL&limit=20" \
  -H "X-API-Key: your-key"

# 5. Check enrichment progress (daemon runs automatically every 30min)
curl "http://localhost:8000/api/v1/enrich/status" \
  -H "X-API-Key: your-key"

# 6. Query enriched matches
curl "http://localhost:8000/api/v1/matches?league=CBLOL&limit=5"
```

---

## Production Deployment

**Deploy to Coolify**: see [`DEPLOYMENT.md`](./DEPLOYMENT.md) for full guide.

### Summary

1. Create Docker Compose application in Coolify
2. Point to repository with `docker-compose.production.yml`
3. Configure environment variables (see [Environment Variables](#environment-variables))
4. Set domain: `scraper.prostaff.gg`
5. Deploy and verify: `curl https://scraper.prostaff.gg/health`

### First deploy — index creation

The `lol_pro_matches` Elasticsearch index is created automatically on first sync.
If deploying over an existing installation with the old schema (pre-Leaguepedia),
delete the index first so it is recreated with the updated mapping:

```bash
curl -X DELETE https://your-elasticsearch-host:9200/lol_pro_matches
```

---

## Stack

| Component | Technology |
|---|---|
| Framework | FastAPI 0.115 (async REST API) |
| Server | Uvicorn (ASGI) |
| Language | Python 3.11 |
| HTTP client | httpx + tenacity (retry/backoff) |
| Data validation | Pydantic 2.9 |
| Storage | Elasticsearch 8.x |
| Deployment | Docker Compose + Traefik (Coolify) |
| Data sources | LoL Esports Persisted Gateway, Leaguepedia Cargo API |

---

## File Structure

```
ProStaff-Scraper/
├── api/
│   └── main.py                      # FastAPI: all endpoints
├── providers/
│   ├── esports.py                   # LoL Esports Gateway API client
│   ├── leaguepedia.py               # Leaguepedia Cargo API client
│   │                                #   get_game_scoreboard() + get_game_players()
│   ├── riot.py                      # Riot Account/Match V5 client
│   └── riot_rate_limited.py         # Riot client with rate limit tiers
├── etl/
│   ├── competitive_pipeline.py      # Phase 1: sync from LoL Esports
│   └── enrichment_pipeline.py       # Phase 2: enrich from Leaguepedia (daemon)
├── indexers/
│   ├── elasticsearch_client.py      # ES helpers (bulk, update, query_unenriched)
│   └── mappings.py                  # Index mappings (participant fields are strings)
├── docs/
│   └── Arquitetura.md               # Full architecture documentation
├── docker-compose.yml               # Development (ES + Kibana + API + enrichment)
├── docker-compose.production.yml    # Production (Coolify + Traefik, 3 services)
├── Dockerfile.production            # Production Docker image
├── DEPLOYMENT.md                    # Coolify deployment guide
├── QUICKSTART.md                    # 5-minute setup guide
├── requirements.txt                 # Python dependencies
└── .env.example                     # Environment variables template
```

---

## Environment Variables

See `.env.example` for the full template.

### Required

| Variable | Description |
|---|---|
| `ESPORTS_API_KEY` | LoL Esports Persisted Gateway key (for sync) |
| `RIOT_API_KEY` | Riot Games API key (for sync, not needed for enrichment) |
| `SCRAPER_API_KEY` | Secret key to protect write endpoints (sync, enrich) |

### Optional

| Variable | Default | Description |
|---|---|---|
| `ELASTICSEARCH_URL` | `http://elasticsearch:9200` | ES connection URL |
| `DEFAULT_PLATFORM_REGION` | `BR1` | Default Riot platform region |
| `API_PORT` | `8000` | FastAPI server port |
| `CORS_ALLOWED_ORIGINS` | `https://api.prostaff.gg,...` | Comma-separated allowed origins |

### Scraper cron settings

| Variable | Default | Description |
|---|---|---|
| `SYNC_LEAGUES` | `CBLOL` | Space-separated leagues to sync |
| `SYNC_INTERVAL_HOURS` | `1` | Sync interval in hours |
| `SYNC_LIMIT` | `100` | Match limit per league per run |

> Note: `RIOT_API_KEY` is only used by the sync pipeline to call LoL Esports endpoints.
> The enrichment daemon uses Leaguepedia anonymously — no API key required.

---

## Troubleshooting

**`GET /health` returns 503**

Elasticsearch is still starting. Wait 30s and retry.

```bash
docker logs prostaff-scraper-elasticsearch-1 | tail -20
```

**`GET /api/v1/matches` returns empty**

Run a sync first:

```bash
curl -X POST "http://localhost:8000/api/v1/sync?league=CBLOL&limit=20" \
  -H "X-API-Key: your-key"
```

**Enrichment stuck — all games at `enrichment_attempts: 3`**

Leaguepedia may not have data for these games yet (common for very recent matches).
They will be picked up automatically on the next daemon run after Leaguepedia updates.
To reset attempts and force retry:

```bash
# Reset attempts for all games (use with care)
curl -X POST http://localhost:9200/lol_pro_matches/_update_by_query \
  -H "Content-Type: application/json" \
  -d '{"query":{"range":{"enrichment_attempts":{"gte":3}}},"script":{"source":"ctx._source.enrichment_attempts=0"}}'
```

**Leaguepedia rate limit errors in logs**

Expected behavior during rapid testing. The enrichment daemon respects 9s between
requests. Errors automatically retry up to 3 times before incrementing `enrichment_attempts`.

**`401 Unauthorized` on sync/enrich endpoints**

Ensure `X-API-Key` header matches `SCRAPER_API_KEY` in your `.env`.

**Elasticsearch mapping conflict after upgrading from old schema**

The participant fields changed from integer IDs to string names. Delete and recreate:

```bash
curl -X DELETE http://localhost:9200/lol_pro_matches
# Restart API and run sync — index is recreated automatically
```

---

## Integration with ProStaff API

1. Set `SCRAPER_API_URL=https://scraper.prostaff.gg` in the Rails API environment
2. Implement a client service to call `/api/v1/matches` and import to PostgreSQL
3. See `PROSTAFF_SCRAPER_INTEGRATION_ANALYSIS.md` for the full integration guide

---

## Resources

- **Full deployment guide**: [`DEPLOYMENT.md`](./DEPLOYMENT.md)
- **Quick start**: [`QUICKSTART.md`](./QUICKSTART.md)
- **Architecture**: [`docs/Arquitetura.md`](./docs/Arquitetura.md)
- **API docs (Swagger)**: `https://scraper.prostaff.gg/docs`

---

## License

CC BY-NC-SA 4.0 — Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International

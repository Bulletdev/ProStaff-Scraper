![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.x-005571?logo=elasticsearch)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)

# ProStaff Scraper - Professional Match Data API

> FastAPI service that scrapes and serves League of Legends professional match data via REST endpoints.
> Integrates with LoL Esports API and Riot Match-V5, storing data in Elasticsearch for fast queries.

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Production Deployment (Coolify)](#production-deployment)
- [Stack Tecnológico](#stack-tecnológico)
- [Arquitetura](#arquitetura)
- [API Endpoints](#api-endpoints)
- [Development Setup](#development-setup)
- [Estrutura](#estrutura)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Troubleshooting](#troubleshooting)
- [Licença](#licença)

## Features

✅ **FastAPI REST API** - Serve professional match data via HTTP endpoints
✅ **Elasticsearch Backend** - Fast queries and analytics on match data
✅ **Automated Syncing** - Cron job for periodic data updates
✅ **Multi-League Support** - CBLOL, LCS, LEC, LCK, and more
✅ **Production Ready** - Docker Compose with Traefik/SSL for Coolify deployment
✅ **Health Checks** - Built-in monitoring endpoints

## Production Deployment

**🚀 Deploy to Coolify**: See detailed instructions in [`DEPLOYMENT.md`](./DEPLOYMENT.md)

**⚡ Quick Start**: See [`QUICKSTART.md`](./QUICKSTART.md) for rapid setup guide

### Summary

1. Create Docker Compose application in Coolify
2. Point to repository with `docker-compose.production.yml`
3. Configure environment variables (API keys, sync settings)
4. Set domain: `scraper.prostaff.gg`
5. Deploy and verify: `curl https://scraper.prostaff.gg/health`

---

## API Endpoints

### Health & Status

```bash
GET /health                    # Health check for Coolify
GET /                          # Root endpoint
GET /api/v1/stats/leagues      # Match statistics per league
```

### Match Data

```bash
GET  /api/v1/leagues                           # List available leagues
GET  /api/v1/matches?league=CBLOL&limit=50     # Query matches
GET  /api/v1/matches/{match_id}                # Get specific match
POST /api/v1/sync?league=CBLOL&limit=50        # Trigger manual sync
```

**Example Response** (`GET /api/v1/matches?league=CBLOL&limit=2`):

```json
{
  "total": 150,
  "league": "CBLOL",
  "limit": 2,
  "skip": 0,
  "count": 2,
  "matches": [
    {
      "match_id": "BR1_123456789",
      "league": "CBLOL",
      "platform_id": "BR1",
      "game_start": "2026-02-10T18:00:00",
      "patch": "14.3",
      "teams": [...],
      "participants": [...]
    }
  ]
}
```

See full API documentation at `https://scraper.prostaff.gg/docs` (Swagger UI)

---

<details>
<summary> Development Setup (click to expand) </summary>

### Option 1: Docker (Recommended)

```bash
# Copy environment variables
cp .env.example .env
# Edit .env and add your API keys

# Start Elasticsearch and API server
docker compose up -d

# Access services
# API: http://localhost:8000
# API Docs (Swagger): http://localhost:8000/docs
# Elasticsearch: http://localhost:9200
# Kibana: http://localhost:5601
```

### Option 2: Local Development (No Docker)

```bash
# Create virtualenv and install dependencies
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows

pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys

# Start Elasticsearch separately (or use existing instance)
# Update ELASTICSEARCH_URL in .env

# Run FastAPI server
uvicorn api.main:app --reload --port 8000

# Or run scraper pipeline directly
python pipelines/cblol.py --league CBLOL --limit 20
```

### Test the API

```bash
# Health check
curl http://localhost:8000/health

# List leagues
curl http://localhost:8000/api/v1/leagues

# Sync matches (this will take a few minutes)
curl -X POST "http://localhost:8000/api/v1/sync?league=CBLOL&limit=10"

# Query matches
curl "http://localhost:8000/api/v1/matches?league=CBLOL&limit=5"
```

</details>

## Stack Tecnológico

- **Framework**: FastAPI 0.115 (async REST API)
- **Server**: Uvicorn (ASGI server)
- **Language**: Python 3.11
- **HTTP Client**: httpx with `tenacity` (backoff/retry)
- **Data Validation**: Pydantic 2.9
- **JSON**: orjson (fast serialization)
- **Config**: python-dotenv
- **Storage**: Elasticsearch 8.x
- **Deployment**: Docker Compose + Traefik (Coolify)

## Arquitetura

```
┌─────────────────┐
│  LoL Esports    │
│  Gateway API    │
└────────┬────────┘
         │
         │ (leagues, schedules, events)
         ▼
┌─────────────────┐      ┌──────────────────┐
│   Riot Match    │─────▶│  ProStaff        │
│   V5 API        │      │  Scraper (API)   │
└─────────────────┘      │                  │
                         │  - FastAPI       │
                         │  - Scraper Cron  │
                         └────────┬─────────┘
                                  │
                         (index & cache)
                                  ▼
                         ┌──────────────────┐
                         │  Elasticsearch   │
                         │  (lol_pro_matches)│
                         └──────────────────┘
                                  │
                         (serve via REST)
                                  ▼
                         ┌──────────────────┐
                         │  ProStaff API    │
                         │  (Rails)         │
                         └──────────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   PostgreSQL     │
                         └──────────────────┘
```

For detailed architecture, see:
- `docs/Arquitetura.md`
- `PROSTAFF_SCRAPER_INTEGRATION_ANALYSIS.md`

### Data Flow

1. **Scraper Cron** runs daily (configurable interval)
2. Fetches league schedules from **LoL Esports API**
3. Gets match details from **Riot Match-V5 API**
4. Normalizes and indexes to **Elasticsearch**
5. **FastAPI** serves data via REST endpoints
6. **ProStaff Rails API** consumes data and stores in **PostgreSQL**

## Estrutura

```
ProStaff-Scraper/
├── api/
│   ├── __init__.py
│   └── main.py                      # FastAPI application
├── providers/
│   ├── esports.py                   # LoL Esports Gateway API client
│   ├── riot.py                      # Riot Match-V5 API client
│   └── ddragon.py                   # Data Dragon (champion data)
├── indexers/
│   ├── elasticsearch_client.py      # Elasticsearch helpers
│   └── mappings.py                  # Index mappings
├── pipelines/
│   └── cblol.py                     # Scraping pipeline orchestration
├── docs/
│   └── Arquitetura.md               # Architecture documentation
├── docker-compose.yml               # Development compose
├── docker-compose.production.yml    # Production compose (Coolify)
├── Dockerfile.production            # Production Docker image
├── DEPLOYMENT.md                    # Full deployment guide
├── QUICKSTART.md                    # Quick setup guide
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variables template
└── README.md                        # This file
```

## Variáveis de Ambiente

See `.env.example` for full template. Key variables:

### Required

- `RIOT_API_KEY` - Riot Games API key for Match-V5
- `ESPORTS_API_KEY` - LoL Esports Persisted Gateway key

### Optional (with defaults)

- `ELASTICSEARCH_URL` - Elasticsearch URL (default: `http://elasticsearch:9200`)
- `DEFAULT_PLATFORM_REGION` - Default region (default: `BR1`)
- `API_PORT` - FastAPI server port (default: `8000`)

### Cron Job Settings

- `SYNC_INTERVAL` - Sync frequency in seconds (default: `86400` = 24h)
- `SYNC_LEAGUE` - League to sync (default: `CBLOL`)
- `SYNC_LIMIT` - Matches per sync (default: `100`)

### Production Only

See `.env.production.example` for Coolify-specific settings.

## Troubleshooting

### Common Issues

**503 Service Unavailable**
- Wait ~30 seconds for Elasticsearch to fully initialize
- Check health: `curl http://localhost:8000/health`

**No matches returned from `/api/v1/matches`**
- Elasticsearch is empty, trigger a sync first:
  ```bash
  curl -X POST "http://localhost:8000/api/v1/sync?league=CBLOL&limit=10"
  ```

**401/403 Unauthorized from Riot API**
- Verify `RIOT_API_KEY` is correct and not expired
- Development keys expire every 24 hours
- Get a new key from https://developer.riotgames.com/

**Rate Limit Errors**
- Scraper uses exponential backoff with `tenacity`
- Reduce `--limit` or `SYNC_LIMIT` to sync fewer matches

**Elasticsearch Connection Failed**
- Check `ELASTICSEARCH_URL` is correct
- Ensure Elasticsearch container is running: `docker ps`
- Check Elasticsearch logs: `docker logs prostaff-scraper-elasticsearch-1`

**ProStaff API cannot reach scraper**
- Ensure using external URL (`https://scraper.prostaff.gg`)
- Check DNS resolution: `nslookup scraper.prostaff.gg`
- Verify Traefik labels in `docker-compose.production.yml`

For more troubleshooting, see `DEPLOYMENT.md`.

---

## Integration with ProStaff API

Once the scraper is deployed, integrate it with your Rails API:

1. **Add environment variable** in ProStaff API:
   ```bash
   SCRAPER_API_URL=https://scraper.prostaff.gg
   ```

2. **Implement client service** (see `PROSTAFF_SCRAPER_INTEGRATION_ANALYSIS.md` for full code)

3. **Import matches** to PostgreSQL via background jobs

See full integration guide in `PROSTAFF_SCRAPER_INTEGRATION_ANALYSIS.md`.

---

## Resources

- **📖 Full Deployment Guide**: [`DEPLOYMENT.md`](./DEPLOYMENT.md)
- **⚡ Quick Start**: [`QUICKSTART.md`](./QUICKSTART.md)
- **🏗️ Integration Analysis**: [`PROSTAFF_SCRAPER_INTEGRATION_ANALYSIS.md`](../PROSTAFF_SCRAPER_INTEGRATION_ANALYSIS.md)
- **🔧 Architecture**: [`docs/Arquitetura.md`](./docs/Arquitetura.md)

---

## License

CC BY-NC-SA 4.0 - Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International

---

## Support

- **Issues**: Open an issue in the repository
- **Questions**: Check the documentation files listed above
- **API Docs**: Visit `/docs` endpoint for interactive Swagger UI
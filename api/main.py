"""
ProStaff Scraper API
FastAPI service to serve professional match data from Elasticsearch
"""
import os
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Security, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from dotenv import load_dotenv

from providers.esports import get_leagues
from indexers.elasticsearch_client import get_client, query_unenriched
from etl.competitive_pipeline import CompetitivePipeline
from etl.enrichment_pipeline import EnrichmentPipeline
from etl.leaguepedia_pipeline import LeaguepediaPipeline
from etl.historical_backfill import HistoricalBackfillPipeline

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

INDEX = "lol_pro_matches"

_cors_origins = os.getenv(
    "CORS_ALLOWED_ORIGINS",
    "https://api.prostaff.gg,https://prostaff.gg,https://www.prostaff.gg"
).split(",")

app = FastAPI(
    title="ProStaff Scraper API",
    description="API for serving League of Legends professional match data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

# API key authentication for write/sync operations
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _require_api_key(api_key: Optional[str] = Security(_api_key_header)) -> str:
    expected = os.getenv("SCRAPER_API_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="SCRAPER_API_KEY not configured on server")
    if api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key


@app.get("/")
def root():
    """Root endpoint"""
    return {
        "service": "ProStaff Scraper API",
        "version": "1.0.0",
        "status": "operational"
    }


@app.get("/health")
def health_check():
    """Health check endpoint for Coolify"""
    try:
        es = get_client()
        es.ping()
        return {
            "status": "healthy",
            "service": "prostaff-scraper",
            "elasticsearch": "connected"
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Elasticsearch unavailable: {str(e)}"
        )


@app.get("/api/v1/leagues")
def list_leagues():
    """List all available leagues from LoL Esports"""
    try:
        data = get_leagues()
        leagues = data.get("data", {}).get("leagues", [])

        return {
            "total": len(leagues),
            "leagues": [
                {
                    "id": lg.get("id"),
                    "name": lg.get("name"),
                    "slug": lg.get("slug"),
                    "region": lg.get("region")
                }
                for lg in leagues
            ]
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch leagues: {str(e)}"
        )


@app.get("/api/v1/matches")
def get_matches(
    league: str = Query("CBLOL", description="League name (e.g., CBLOL, LCS, LEC)"),
    limit: int = Query(50, ge=1, le=500, description="Number of matches to return"),
    skip: int = Query(0, ge=0, description="Number of matches to skip (pagination)")
):
    """
    Query Elasticsearch for cached professional matches

    - **league**: League name (CBLOL, LCS, LEC, etc.)
    - **limit**: Maximum number of matches to return (1-500)
    - **skip**: Number of matches to skip for pagination
    """
    try:
        es = get_client()

        query = {
            "query": {
                "match": {"league": league}
            },
            "from": skip,
            "size": limit,
            "sort": [{"start_time": {"order": "desc", "unmapped_type": "date"}}]
        }

        result = es.search(index=INDEX, **query)
        hits = result["hits"]["hits"]

        return {
            "total": result["hits"]["total"]["value"],
            "league": league,
            "limit": limit,
            "skip": skip,
            "count": len(hits),
            "matches": [hit["_source"] for hit in hits]
        }
    except Exception as e:
        logger.error(f"get_matches error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch matches: {str(e)}"
        )


@app.get("/api/v1/matches/{match_id}")
def get_match_details(match_id: str):
    """
    Get specific match details from Elasticsearch

    - **match_id**: The match ID (e.g., BR1_123456789)
    """
    try:
        es = get_client()

        try:
            result = es.get(index=INDEX, id=match_id)
            return result["_source"]
        except Exception:
            query = {
                "query": {
                    "term": {"match_id.keyword": match_id}
                }
            }
            result = es.search(index=INDEX, **query)

            if result["hits"]["total"]["value"] == 0:
                raise HTTPException(status_code=404, detail="Match not found")

            return result["hits"]["hits"][0]["_source"]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_match_details error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch match details: {str(e)}"
        )


@app.post("/api/v1/sync")
def trigger_sync(
    league: str = Query("CBLOL", description="League name to sync"),
    limit: int = Query(50, ge=1, le=200, description="Number of matches to scrape"),
    api_key: str = Security(_require_api_key)
):
    """
    Trigger scraping pipeline for a specific league (requires X-API-Key header)

    This endpoint will:
    1. Fetch league schedule from LoL Esports API
    2. Get match details from Riot API
    3. Index matches to Elasticsearch

    - **league**: League name (CBLOL, LCS, LEC, etc.)
    - **limit**: Number of matches to scrape (1-200)
    """
    try:
        logger.info(f"Manual sync triggered for {league} (limit={limit})")
        pipeline = CompetitivePipeline(leagues=[league], is_production=True)
        pipeline.run_pipeline(limit_per_league=limit)

        return {
            "message": f"Sync completed for {league}",
            "league": league,
            "limit": limit,
            "status": "completed",
            "stats": dict(pipeline.stats)
        }
    except Exception as e:
        logger.error(f"Sync failed for {league}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Sync failed: {str(e)}"
        )


@app.post("/api/v1/enrich")
def trigger_enrichment(
    background_tasks: BackgroundTasks,
    batch: int = Query(10, ge=1, le=50, description="Games to enrich per run"),
    production: bool = Query(False, description="(no-op, kept for compatibility)"),
    api_key: str = Security(_require_api_key),
):
    """
    Trigger background enrichment of unenriched competitive games.

    Fetches per-player stats (champion, KDA, gold, CS, items, runes) from
    Leaguepedia ScoreboardGames + ScoreboardPlayers tables.
    No Riot Match-V5 dependency: competitive games are not accessible via
    the public API and Leaguepedia is the authoritative source.

    Runs in the background - returns immediately while enrichment proceeds.
    Each game requires 2 Leaguepedia requests (~18s total with rate limit).

    Deduplication is automatic: only riot_enriched=false games are processed.

    - **batch**: How many games to attempt in this run (1-50)
    """
    try:
        pending = query_unenriched(INDEX, size=1)
        pending_count_check = len(pending)
    except Exception:
        pending_count_check = -1

    def _run_enrichment():
        pipeline = EnrichmentPipeline(is_production=production, batch_size=batch)
        pipeline.running = True
        pipeline.run_batch()

    background_tasks.add_task(_run_enrichment)

    return {
        "message": "Enrichment started in background",
        "batch_size": batch,
        "status": "running",
        "note": f"Each game takes ~{9}s (Leaguepedia rate limit). "
                f"Check logs/enrichment_pipeline.log for progress.",
    }


@app.get("/api/v1/enrich/status")
def enrichment_status(api_key: str = Security(_require_api_key)):
    """
    Show how many games are pending enrichment.

    - **riot_enriched=false**: waiting for Leaguepedia + Riot data
    - **riot_enriched=true**: fully enriched with items/runes/KDA
    """
    try:
        es = get_client()

        query = {
            "size": 0,
            "aggs": {
                "enrichment_status": {
                    "terms": {"field": "riot_enriched"}
                },
                "max_attempts_reached": {
                    "filter": {"range": {"enrichment_attempts": {"gte": 3}}}
                },
            },
        }
        result = es.search(index=INDEX, **query)

        buckets = result["aggregations"]["enrichment_status"]["buckets"]
        status_map = {str(b["key_as_string"]).lower(): b["doc_count"] for b in buckets}

        return {
            "total": result["hits"]["total"]["value"],
            "enriched": status_map.get("true", 0),
            "pending": status_map.get("false", 0),
            "max_attempts_reached": result["aggregations"]["max_attempts_reached"]["doc_count"],
        }
    except Exception as e:
        logger.error(f"enrichment_status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/sync-leaguepedia")
def trigger_leaguepedia_sync(
    background_tasks: BackgroundTasks,
    tournament: str = Query(
        ...,
        description='Leaguepedia OverviewPage, e.g. "CBLOL/2026 Season/Cup"',
    ),
    api_key: str = Security(_require_api_key),
):
    """
    Import ALL games for a tournament directly from Leaguepedia.

    Use this when the LoL Esports API no longer has historical data
    (regular season games disappear from getCompletedEvents after ~weeks
    because it only returns the 300 most recent global events).

    This endpoint queries Leaguepedia ScoreboardGames by OverviewPage to
    get every game in the tournament, including regular season BO1s.
    All games are indexed to Elasticsearch with riot_enriched=true since
    the player data comes from Leaguepedia.

    Runs in background — each game takes ~24s (2 Leaguepedia requests).
    A 50-game tournament takes ~20 minutes.

    - **tournament**: Leaguepedia OverviewPage (e.g. "CBLOL/2026 Season/Cup")
    """
    def _run():
        pipeline = LeaguepediaPipeline(dry_run=False)
        pipeline.run(tournament)

    background_tasks.add_task(_run)

    return {
        "message": "Leaguepedia sync started in background",
        "tournament": tournament,
        "status": "running",
        "note": (
            f"Each game takes ~{int(12 * 2)}s (2 Leaguepedia requests). "
            "Check logs/leaguepedia_pipeline.log for progress."
        ),
    }


@app.get("/api/v1/tournaments")
def list_tournaments(
    league: str = Query(
        "CBLOL",
        description="League prefix on Leaguepedia (e.g. CBLOL, LCS, LEC)",
    ),
    min_year: int = Query(
        2013,
        ge=2012,
        le=2030,
        description="Ignore tournaments before this year",
    ),
    api_key: str = Security(_require_api_key),
):
    """
    Discover and list all main-event tournament OverviewPages for a league.

    Queries the Leaguepedia Tournaments cargo table directly and applies the
    same filter used by the historical backfill pipeline (skips qualifiers,
    academy, all-star and sub-event pages).

    Useful to preview which editions exist before triggering the full backfill.
    Note: this makes live Leaguepedia API calls — may take a few seconds.
    """
    from providers.leaguepedia import get_league_tournaments
    from etl.historical_backfill import _is_main_event

    try:
        raw = get_league_tournaments(league, min_year=min_year)
        filtered = [t for t in raw if _is_main_event(t["overview_page"])]

        return {
            "league": league,
            "min_year": min_year,
            "total_discovered": len(raw),
            "total_main_events": len(filtered),
            "tournaments": filtered,
        }
    except Exception as e:
        logger.error(f"list_tournaments error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/historical-backfill")
def trigger_historical_backfill(
    background_tasks: BackgroundTasks,
    league: str = Query(
        "CBLOL",
        description="League prefix on Leaguepedia (e.g. CBLOL, LCS, LEC)",
    ),
    min_year: int = Query(
        2013,
        ge=2012,
        le=2030,
        description="Ignore tournaments before this year",
    ),
    api_key: str = Security(_require_api_key),
):
    """
    Trigger a full historical backfill for a league.

    Discovers every tournament edition on Leaguepedia (e.g. all CBLOL splits
    since 2013) and imports all games for each one into Elasticsearch.

    The pipeline is **resumable**: progress is saved to
    `data/backfill_{LEAGUE}.json` after each tournament. If the server
    restarts mid-run, re-calling this endpoint will continue from where
    it left off, skipping already-completed tournaments.

    Each game takes ~24 seconds (2 Leaguepedia requests at 12s each).
    A full CBLOL history (~30 tournaments × ~60 games) takes roughly 6 hours.

    Check progress with `GET /api/v1/historical-backfill/status?league=CBLOL`.
    """
    def _run():
        pipeline = HistoricalBackfillPipeline(league=league, min_year=min_year)
        pipeline.run()

    background_tasks.add_task(_run)

    return {
        "message": f"Historical backfill started for '{league}' (min_year={min_year})",
        "league": league,
        "min_year": min_year,
        "status": "running",
        "progress_file": f"data/backfill_{league.upper()}.json",
        "note": (
            "Runs in background. Each game ~24s (Leaguepedia rate limit). "
            f"Check GET /api/v1/historical-backfill/status?league={league} for progress."
        ),
    }


@app.get("/api/v1/historical-backfill/status")
def historical_backfill_status(
    league: str = Query("CBLOL", description="League prefix (e.g. CBLOL, LCS)"),
    api_key: str = Security(_require_api_key),
):
    """
    Return current progress of the historical backfill for a league.

    Reads from `data/backfill_{LEAGUE}.json` (written after each tournament).

    Response includes:
    - `total_tournaments`: how many main-event editions were discovered
    - `completed` / `pending` / `errors`: counts by status
    - `total_games_indexed`: cumulative games loaded into Elasticsearch
    - `tournaments`: per-tournament breakdown with dates, game counts, status
    """
    pipeline = HistoricalBackfillPipeline(league=league)
    return pipeline.get_status()


@app.get("/api/v1/stats/leagues")
def get_league_stats():
    """Get statistics about indexed matches per league"""
    try:
        es = get_client()

        query = {
            "size": 0,
            "aggs": {
                "leagues": {
                    "terms": {
                        "field": "league",
                        "size": 50
                    }
                }
            }
        }

        result = es.search(index=INDEX, **query)
        buckets = result["aggregations"]["leagues"]["buckets"]

        return {
            "total_matches": result["hits"]["total"]["value"],
            "leagues": [
                {
                    "league": bucket["key"],
                    "match_count": bucket["doc_count"]
                }
                for bucket in buckets
            ]
        }
    except Exception as e:
        logger.error(f"get_league_stats error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch league stats: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

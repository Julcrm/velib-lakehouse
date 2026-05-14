"""
API FastAPI — Expose les métriques du pipeline Vélib Lakehouse et les insights données.

Sécurité :
- API Key via header X-API-Key sur tous les endpoints de données
- HTTP Basic Auth sur /docs
"""
import os
import secrets
from datetime import date
import duckdb
import httpx
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import APIKeyHeader, HTTPBasic, HTTPBasicCredentials
from src.config import BUCKET

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000").replace("https://", "").replace("http://", "")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DAGSTER_URL = os.getenv("DAGSTER_URL")
API_KEY = os.getenv("API_KEY")
DOCS_USER = os.getenv("DOCS_USER")
DOCS_PASSWORD = os.getenv("DOCS_PASSWORD")

# --- App ---
app = FastAPI(
    title="Vélib Lakehouse API",
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Sécurité API Key ---
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def verify_api_key(key: str = Security(api_key_header)):
    """Vérifie que le header X-API-Key est valide."""
    if not API_KEY or not secrets.compare_digest(key, API_KEY):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API Key",
        )

# --- Sécurité HTTP Basic Auth pour /docs ---
basic_security = HTTPBasic()

def verify_docs_credentials(credentials: HTTPBasicCredentials = Depends(basic_security)):
    """Vérifie les credentials pour accéder à /docs."""
    correct_user = secrets.compare_digest(credentials.username, DOCS_USER)
    correct_pass = secrets.compare_digest(credentials.password, DOCS_PASSWORD or "")
    if not (correct_user and correct_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

# --- Route /docs protégée ---
@app.get("/docs", include_in_schema=False)
async def custom_swagger(credentials: HTTPBasicCredentials = Depends(verify_docs_credentials)):
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Vélib Lakehouse API")


# --- Helpers DuckDB et Dagster ---

def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Crée une connexion DuckDB configurée pour lire depuis MinIO."""
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    return con


async def query_dagster_graphql(query: str) -> dict:
    """Appelle l'API GraphQL de Dagster et retourne la réponse."""
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            f"{DAGSTER_URL}/graphql",
            json={"query": query},
        )
        response.raise_for_status()
        return response.json()


# =============================================================================
# SECTION 1 — PIPELINE MONITOR
# =============================================================================

@app.get("/pipeline/status", dependencies=[Depends(verify_api_key)])
async def get_pipeline_status():
    """Statut de la dernière exécution par asset (Bronze / Silver / Gold)."""
    query = """
    {
      runsOrError(filter: {pipelineName: "velib_pipeline_job"}, limit: 10) {
        ... on Runs {
          results {
            runId
            status
            startTime
            endTime
            stepStats {
              stepKey
              status
              startTime
              endTime
            }
          }
        }
      }
    }
    """
    try:
        data = await query_dagster_graphql(query)
        runs = data.get("data", {}).get("runsOrError", {}).get("results", [])

        if not runs:
            return {"status": "no_runs", "assets": {}}

        latest_run = runs[0]
        duration = None
        if latest_run.get("startTime") and latest_run.get("endTime"):
            duration = round(latest_run["endTime"] - latest_run["startTime"], 1)

        assets_status = {}
        for step in latest_run.get("stepStats", []):
            key = step["stepKey"].replace("_1", "")
            assets_status[key] = {
                "status": step["status"],
                "duration_seconds": round(step["endTime"] - step["startTime"], 1)
                if step.get("startTime") and step.get("endTime") else None,
            }

        return {
            "run_id": latest_run["runId"],
            "status": latest_run["status"],
            "started_at": latest_run.get("startTime"),
            "ended_at": latest_run.get("endTime"),
            "duration_seconds": duration,
            "assets": assets_status,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pipeline/metrics", dependencies=[Depends(verify_api_key)])
async def get_pipeline_metrics():
    """Métriques du pipeline : snapshots Bronze aujourd'hui, stations ingérées."""
    try:
        con = get_duckdb_connection()
        today = date.today().isoformat()

        snapshots = con.execute(f"""
            SELECT COUNT(*) as count
            FROM glob('s3://{BUCKET}/bronze/velib/date={today}/*.parquet')
        """).fetchone()[0]

        stations = con.execute(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('s3://{BUCKET}/bronze/velib/date={today}/*.parquet')
        """).fetchone()[0]

        silver_rows = con.execute(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('s3://{BUCKET}/silver/velib/velib_silver.parquet')
            WHERE date = '{today}'
        """).fetchone()[0]

        return {
            "date": today,
            "bronze_snapshots_today": snapshots,
            "stations_ingested": stations,
            "silver_rows_today": silver_rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SECTION 2 — VÉLIB INSIGHTS
# =============================================================================

@app.get("/velib/summary", dependencies=[Depends(verify_api_key)])
async def get_velib_summary():
    """Métriques globales Vélib : total vélos, répartition mécanique/électrique."""
    try:
        con = get_duckdb_connection()
        today = date.today().isoformat()

        result = con.execute(f"""
            WITH latest AS (
                SELECT *
                FROM read_parquet('s3://{BUCKET}/silver/velib/velib_silver.parquet')
                WHERE date = '{today}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY station_code
                    ORDER BY last_reported DESC
                ) = 1
            )
            SELECT
                COUNT(*)                    AS total_stations,
                SUM(bikes_available)        AS total_bikes,
                SUM(bikes_mechanical)       AS total_mechanical,
                SUM(bikes_electric)         AS total_electric,
                SUM(docks_available)        AS total_docks_available,
                COUNT(*) FILTER (WHERE bikes_available = 0) AS empty_stations
            FROM latest
        """).fetchone()

        return {
            "total_stations": result[0],
            "total_bikes": result[1],
            "total_mechanical": result[2],
            "total_electric": result[3],
            "total_docks_available": result[4],
            "empty_stations": result[5],
            "pct_electric": round(result[3] / result[1] * 100, 1) if result[1] else 0,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/velib/at-risk", dependencies=[Depends(verify_api_key)])
async def get_stations_at_risk():
    """Top stations à risque de vidage dans les 30 prochaines minutes."""
    try:
        con = get_duckdb_connection()

        results = con.execute(f"""
            SELECT
                station_name,
                bikes_available,
                ROUND(depletion_rate_per_minute, 4)  AS depletion_rate_per_minute,
                minutes_until_empty,
                arrondissement
            FROM read_parquet('s3://{BUCKET}/gold/velib/velib_stations_at_risk.parquet')
            ORDER BY minutes_until_empty ASC
            LIMIT 10
        """).fetchall()

        stations = [
            {
                "station_name": r[0],
                "bikes_available": r[1],
                "depletion_rate_per_minute": r[2],
                "minutes_until_empty": r[3],
                "arrondissement": r[4],
            }
            for r in results
        ]

        return {
            "count": len(stations),
            "stations": stations,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/velib/top-depletion", dependencies=[Depends(verify_api_key)])
async def get_top_depletion():
    """Top 5 stations qui se vident le plus vite en ce moment."""
    try:
        con = get_duckdb_connection()
        today = date.today().isoformat()

        results = con.execute(f"""
            WITH latest AS (
                SELECT *
                FROM read_parquet('s3://{BUCKET}/silver/velib/velib_silver.parquet')
                WHERE date = '{today}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY station_code
                    ORDER BY last_reported DESC
                ) = 1
            )
            SELECT
                station_name,
                bikes_available,
                ROUND(depletion_rate_per_minute, 4) AS depletion_rate_per_minute,
                arrondissement
            FROM latest
            WHERE depletion_rate_per_minute < 0
            ORDER BY depletion_rate_per_minute ASC
            LIMIT 5
        """).fetchall()

        return {
            "stations": [
                {
                    "station_name": r[0],
                    "bikes_available": r[1],
                    "depletion_rate_per_minute": r[2],
                    "arrondissement": r[3],
                }
                for r in results
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    """Endpoint de santé — pas d'auth requise."""
    return {"status": "ok"}
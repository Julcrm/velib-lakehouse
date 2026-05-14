"""
API FastAPI - Expose les métriques du pipeline Vélib Lakehouse et les insights données.

Section 1 — Pipeline Monitor : statut et métriques d'exécution via Dagster GraphQL
Section 2 — Vélib Insights : métriques données via DuckDB + MinIO
"""

import os
from datetime import date
import duckdb
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from src.config import BUCKET

app = FastAPI(title="Vélib Lakehouse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL").replace("https://", "").replace("http://", "")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DAGSTER_URL = os.getenv("DAGSTER_URL")


# --- Helpers ---

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

@app.get("/pipeline/status")
async def get_pipeline_status():
    """
    Statut de la dernière exécution par asset (Bronze / Silver / Gold).
    Source : Dagster GraphQL API.
    """
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

        # Statut par asset
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


@app.get("/pipeline/metrics")
async def get_pipeline_metrics():
    """
    Métriques du pipeline : snapshots Bronze aujourd'hui, stations ingérées.
    Source : DuckDB + MinIO.
    """
    try:
        con = get_duckdb_connection()
        today = date.today().isoformat()

        # Nb de snapshots Bronze aujourd'hui
        snapshots = con.execute(f"""
            SELECT COUNT(*) as count
            FROM glob('s3://{BUCKET}/bronze/velib/date={today}/*.parquet')
        """).fetchone()[0]

        # Nb de stations ingérées dans le dernier snapshot
        stations = con.execute(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('s3://{BUCKET}/bronze/velib/date={today}/*.parquet')
        """).fetchone()[0]

        # Nb de lignes Silver aujourd'hui
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

@app.get("/velib/summary")
async def get_velib_summary():
    """
    Métriques globales Vélib : total vélos, répartition mécanique/électrique.
    Source : dernier snapshot Silver.
    """
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


@app.get("/velib/at-risk")
async def get_stations_at_risk():
    """
    Top stations à risque de vidage dans les 30 prochaines minutes.
    Source : Gold velib_stations_at_risk.
    """
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


@app.get("/velib/top-depletion")
async def get_top_depletion():
    """
    Top 5 stations qui se vident le plus vite en ce moment.
    Source : Silver — dernier snapshot avec depletion_rate négatif.
    """
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
    """Endpoint de santé — vérifie que l'API répond."""
    return {"status": "ok"}
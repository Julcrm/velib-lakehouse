# Vélib Lakehouse

A production-grade data lakehouse pipeline deployed on a VPS — real-time ingestion of Vélib station data, dbt transformations, and a FastAPI serving layer.

**Live dashboard** → [julien-castellano.fr/projets/velib-lakehouse](https://julien-castellano.fr/projets/velib-lakehouse)

## Architecture

```
Vélib API (OpenData Paris)
        ↓
Bronze — raw Parquet snapshots on MinIO
        ↓
Silver — dbt + DuckDB (cleaning, window functions)
        ↓
Gold   — dbt + DuckDB (alerts, aggregations)
        ↓
FastAPI — pipeline metrics and Vélib insights
```

Orchestrated by **Dagster** on a 10-minute schedule.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Dagster |
| Ingestion | Python, requests |
| Storage | MinIO (S3-compatible) |
| Transformation | dbt + DuckDB |
| Serving | FastAPI |
| Packaging | uv |
| CI/CD | GitHub Actions, Coolify |

## Project structure

```
src/
├── config.py                   # Centralized constants and API URLs
├── ingestion/
│   └── producer.py             # Vélib API → Bronze Parquet ingestion
├── dagster/
│   ├── assets.py               # Dagster assets (Bronze, Silver, Gold)
│   └── definitions.py          # Dagster Definitions wiring
├── resources/
│   └── minio.py                # Shared MinIO resource (s3fs)
├── maintenance/
│   └── cleaner.py              # Retention cleanup — Bronze 7d / Silver+Gold 30d
└── serving/
    └── api.py                  # FastAPI — pipeline and Vélib insight endpoints

dbt/
├── models/
│   ├── silver/
│   │   └── velib_silver.sql                  # Cleaning + depletion_rate (LAG)
│   └── gold/
│       ├── velib_stations_at_risk.sql         # Stations at risk within 30 min
│       ├── velib_stations_empty_duration.sql  # Stations empty for 60+ min
│       └── velib_stats_arrondissement.sql     # Stats aggregated by district
docker/
├── dagster/Dockerfile          # gRPC code server (port 4000)
└── api/Dockerfile              # FastAPI serving (port 8000)
```

## Tests

```bash
# Lint
uv run ruff check .

# Unit tests (ingestion + API)
uv run pytest tests/ -v

# dbt data quality tests (Silver layer)
cd dbt
uv run dbt build --select silver
```

## CI

GitHub Actions runs on every push and pull request to `main` (`.github/workflows/ci.yml`).

| Step | Command |
|---|---|
| Lint | `uv run ruff check .` |
| Unit tests | `uv run pytest tests/ -v` |
| Lock file integrity | `uv lock --check` |

The `deploy` job (Coolify webhook) only triggers on push to `main` and is gated on the `lint` job passing.

## Deployment

Deployed on a VPS via Coolify with automatic deployment on push to `main` (GitHub Actions).


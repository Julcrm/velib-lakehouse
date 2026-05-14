"""
Assets Dagster qui orchestrent le pipeline Vélib Lakehouse.
Couches : Bronze (ingestion) → Silver (dbt) → Gold (serving DuckDB).
Les assets ne contiennent pas de logique métier — ils délèguent à producer.py.
"""
import subprocess

import dagster as dg
from loguru import logger

from src.ingestion.producer import run as run_producer
from src.resources.minio import MinioResource


@dg.asset(group_name="bronze", description="Snapshot brut des stations Vélib sur MinIO Bronze.")
def velib_bronze(context: dg.AssetExecutionContext, minio: MinioResource) -> dg.MaterializeResult:
    """Étape 1 — Ingestion : appel API Vélib + écriture Parquet sur MinIO Bronze."""
    fs = minio.get_filesystem()
    path = run_producer(fs=fs)
    context.log.info(f"Ingestion terminée → {path}")
    return dg.MaterializeResult(metadata={"path": path})


@dg.asset(
    group_name="silver",
    deps=[velib_bronze],
    description="Transformation dbt : Bronze → Silver.",
)
def velib_silver(context: dg.AssetExecutionContext) -> None:
    result = subprocess.run(
        ["uv", "run", "dbt", "run", "--select", "silver"],
        capture_output=True,
        text=True,
        cwd="/opt/dagster/app/dbt",
    )
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError("dbt run (silver) a échoué.")
    context.log.info(result.stdout)
    context.log.info("dbt silver terminé.")


@dg.asset(
    group_name="gold",
    deps=[velib_silver],
    description="Agrégation dbt : Silver → Gold.",
)
def velib_gold(context: dg.AssetExecutionContext) -> None:
    result = subprocess.run(
        ["uv", "run", "dbt", "run", "--select", "gold"],
        capture_output=True,
        text=True,
        cwd="/opt/dagster/app/dbt",
    )
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError("dbt run (gold) a échoué.")
    context.log.info(result.stdout)
    context.log.info("dbt gold terminé.")
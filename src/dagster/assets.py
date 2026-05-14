"""
Assets Dagster qui orchestrent le pipeline Vélib Lakehouse.
Couches : Bronze (ingestion) → Silver (dbt) → Gold (serving DuckDB).
"""
import os
import subprocess

import dagster as dg
import requests
from loguru import logger

from src.ingestion.producer import run as run_producer
from src.resources.minio import MinioResource


@dg.asset(group_name="bronze", description="Snapshot brut des stations Vélib sur MinIO Bronze.")
def velib_bronze(context: dg.AssetExecutionContext, minio: MinioResource) -> dg.MaterializeResult:
    """Étape 1 — Ingestion : appel API Vélib + écriture Parquet sur MinIO Bronze."""
    context.log.info("Asset velib_bronze: démarrage de l'ingestion.")
    fs = minio.get_filesystem()
    path = run_producer(fs=fs)
    context.log.info(f"Asset velib_bronze: ingestion terminée → {path}")
    return dg.MaterializeResult(metadata={"path": path})


@dg.asset(group_name="bronze", description="Données de référence des stations Vélib (noms, capacités).")
def velib_reference_bronze(context: dg.AssetExecutionContext, minio: MinioResource) -> dg.MaterializeResult:
    """Étape 1b — Ingère station_information.json depuis l'API GBFS Vélib vers Bronze."""
    url = "https://velib-metropole-opendata.smovengo.com/opendata/Velib_Metropole/station_information.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    stations = payload.get("data", {}).get("stations", [])
    if not stations:
        raise ValueError("Données de référence vides !")
    bucket = os.getenv("BUCKET", "velib-lakehouse")
    path = minio.upload_json(bucket, "bronze/velib/reference/station_information.json", payload)
    context.log.info(f"{len(stations)} stations de référence ingérées → {path}")
    return dg.MaterializeResult(metadata={"path": path, "station_count": len(stations)})


@dg.asset(
    group_name="silver",
    deps=[velib_bronze],
    description="Transformation dbt : Bronze → Silver (nettoyage, typage, dédoublonnage).",
)
def velib_silver(context: dg.AssetExecutionContext) -> None:
    """Étape 2 — Transformation : exécute les modèles dbt Silver."""
    context.log.info("Asset velib_silver: lancement de dbt run (silver).")
    result = subprocess.run(
        ["dbt", "run", "--select", "silver"],
        capture_output=True,
        text=True,
        cwd="dbt",
    )
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run (silver) a échoué.")
    context.log.info("Asset velib_silver: dbt silver terminé.")


@dg.asset(
    group_name="gold",
    deps=[velib_silver],
    description="Agrégation dbt : Silver → Gold (métriques, alertes).",
)
def velib_gold(context: dg.AssetExecutionContext) -> None:
    """Étape 3 — Agrégation : exécute les modèles dbt Gold."""
    context.log.info("Asset velib_gold: lancement de dbt run (gold).")
    result = subprocess.run(
        ["dbt", "run", "--select", "gold"],
        capture_output=True,
        text=True,
        cwd="dbt",
    )
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run (gold) a échoué.")
    context.log.info("Asset velib_gold: dbt gold terminé.")

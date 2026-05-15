"""
Assets Dagster qui orchestrent le pipeline Vélib Lakehouse.
Couches : Bronze (ingestion) → Silver (dbt) → Gold (serving DuckDB).
Les assets ne contiennent pas de logique métier — ils délèguent à producer.py.
"""
import subprocess
import dagster as dg
from loguru import logger
from src.ingestion.producer import run as run_producer
from src.maintenance.cleaner import run_cleanup
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
        ["uv", "run", "dbt", "build", "--select", "silver"],
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

@dg.asset(
    group_name="maintenance",
    description="Supprime les fichiers expirés sur MinIO — Bronze 7j, Silver/Gold 30j.",
)
def velib_cleanup(context: dg.AssetExecutionContext, minio: MinioResource) -> dg.MaterializeResult:
    """Nettoyage — délègue à cleaner.py."""
    fs = minio.get_filesystem()
    deleted = run_cleanup(fs)
    context.log.info(f"Nettoyage terminé : {deleted}")
    return dg.MaterializeResult(
        metadata={
            "bronze_deleted": deleted["bronze"],
            "silver_deleted": deleted["silver"],
            "gold_deleted": deleted["gold"],
        }
    )

# --- Schedule ---

@dg.schedule(
    cron_schedule="*/10 * * * *",
    job=dg.define_asset_job(
        name="velib_pipeline_job",
        selection=[velib_bronze, velib_silver, velib_gold],
    ),
    description="Lance le pipeline Vélib complet toutes les 10 minutes.",
)
def velib_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest()

@dg.schedule(
    cron_schedule="0 2 * * *",
    job=dg.define_asset_job(
        name="velib_cleanup_job",
        selection=[velib_cleanup],
    ),
    description="Nettoyage quotidien des données expirées à 2h du matin.",
)
def velib_cleanup_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest()
"""
Dagster assets orchestrating the Vélib Lakehouse pipeline.
Layers: Bronze (ingestion) → Silver (dbt) → Gold (DuckDB serving).
Assets contain no business logic — they delegate to producer.py and cleaner.py.
"""
import subprocess

import dagster as dg
from src.ingestion.producer import run as run_producer
from src.maintenance.cleaner import run_cleanup
from src.resources.minio import MinioResource


@dg.asset(group_name="bronze", description="Raw Vélib station snapshot written to MinIO Bronze.")
def velib_bronze(context: dg.AssetExecutionContext, minio: MinioResource) -> dg.MaterializeResult:
    """Step 1 — Ingestion: call the Vélib API and write a Parquet snapshot to MinIO Bronze."""
    fs = minio.get_filesystem()
    path = run_producer(fs=fs)
    context.log.info(f"Ingestion complete → {path}")
    return dg.MaterializeResult(metadata={"path": path})


@dg.asset(
    group_name="silver",
    deps=[velib_bronze],
    description="dbt build (run + tests): Bronze → Silver.",
)
def velib_silver(context: dg.AssetExecutionContext) -> None:
    """Step 2 — Transformation: run dbt Silver models and their data quality tests."""
    result = subprocess.run(
        ["uv", "run", "dbt", "build", "--select", "silver"],
        capture_output=True,
        text=True,
        cwd="/opt/dagster/app/dbt",
    )

    if result.stdout:
        context.log.info(result.stdout)

    if result.returncode != 0:
        if result.stderr:
            context.log.error(result.stderr)
        raise RuntimeError(
            "dbt Silver build failed: table could not be created or tests did not pass."
        )

    context.log.info("dbt Silver build (run + tests) completed successfully.")


@dg.asset(
    group_name="gold",
    deps=[velib_silver],
    description="dbt aggregation: Silver → Gold.",
)
def velib_gold(context: dg.AssetExecutionContext) -> None:
    """Step 3 — Aggregation: run dbt Gold models to produce analytics-ready tables."""
    result = subprocess.run(
        ["uv", "run", "dbt", "run", "--select", "gold"],
        capture_output=True,
        text=True,
        cwd="/opt/dagster/app/dbt",
    )
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError("dbt Gold run failed.")
    context.log.info(result.stdout)
    context.log.info("dbt Gold run completed successfully.")


@dg.asset(
    group_name="maintenance",
    description="Delete expired files from MinIO — Bronze 7 days, Silver/Gold 30 days.",
)
def velib_cleanup(context: dg.AssetExecutionContext, minio: MinioResource) -> dg.MaterializeResult:
    """Step 4 — Cleanup: delegate to cleaner.py and report deleted file counts."""
    fs = minio.get_filesystem()
    deleted = run_cleanup(fs)
    context.log.info(f"Cleanup complete: {deleted}")
    return dg.MaterializeResult(
        metadata={
            "bronze_deleted": deleted["bronze"],
            "silver_deleted": deleted["silver"],
            "gold_deleted": deleted["gold"],
        }
    )


# --- Schedules ---

@dg.schedule(
    cron_schedule="*/10 * * * *",
    job=dg.define_asset_job(
        name="velib_pipeline_job",
        selection=[velib_bronze, velib_silver, velib_gold],
    ),
    description="Trigger the full Vélib pipeline every 10 minutes.",
)
def velib_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Emit a RunRequest on each 10-minute tick to execute the Bronze → Gold pipeline."""
    return dg.RunRequest()


@dg.schedule(
    cron_schedule="0 2 * * *",
    job=dg.define_asset_job(
        name="velib_cleanup_job",
        selection=[velib_cleanup],
    ),
    description="Daily cleanup of expired data at 2 AM.",
)
def velib_cleanup_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Emit a RunRequest at 2 AM every day to run the retention cleanup job."""
    return dg.RunRequest()

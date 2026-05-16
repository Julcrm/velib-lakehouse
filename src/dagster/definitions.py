"""
Dagster Definitions entry point for the Vélib Lakehouse pipeline.

Assembles assets (bronze / silver / gold), the recurring schedule,
and the MinIO resource. Environment variables S3_ENDPOINT_URL,
AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY are read at startup.
"""
import os

from dagster import Definitions
from src.dagster.assets import (
    velib_bronze,
    velib_cleanup,
    velib_cleanup_schedule,
    velib_gold,
    velib_schedule,
    velib_silver,
)
from src.resources.minio import MinioResource

defs = Definitions(
    assets=[velib_bronze, velib_silver, velib_gold, velib_cleanup],
    schedules=[velib_schedule, velib_cleanup_schedule],
    resources={
        "minio": MinioResource(
            endpoint=os.getenv("S3_ENDPOINT_URL"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    },
)

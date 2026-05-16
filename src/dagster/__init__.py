"""
Dagster entry point for the Vélib Lakehouse project.
Exposes assets via a gRPC server on port 4000.
"""
import os

from dagster import Definitions
from src.dagster.assets import velib_bronze, velib_gold, velib_silver
from src.resources.minio import MinioResource

defs = Definitions(
    assets=[velib_bronze, velib_silver, velib_gold],
    resources={
        "minio": MinioResource(
            endpoint=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        )
    },
)

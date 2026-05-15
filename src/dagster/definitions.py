"""
Point d'entrée Dagster Definitions pour le pipeline Velib Lakehouse.

Assemble les assets (bronze / silver / gold), le schedule horaire
et la ressource MinIO. Les variables d'environnement S3_ENDPOINT_URL,
AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY sont lues au démarrage.
"""
import os
from dagster import Definitions
from src.resources.minio import MinioResource
from src.dagster.assets import velib_bronze, velib_silver, velib_test, velib_gold, velib_cleanup, velib_schedule, velib_cleanup_schedule


defs = Definitions(
    assets=[velib_bronze, velib_silver, velib_test, velib_gold, velib_cleanup],
    schedules=[velib_schedule, velib_cleanup_schedule],
    resources={
        "minio": MinioResource(
            endpoint=os.getenv("S3_ENDPOINT_URL"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    }
)
"""
Data retention manager for the Vélib Lakehouse.
Bronze: 7 days — Silver / Gold: 30 days.
"""
import re
from datetime import datetime, timedelta

import s3fs
from loguru import logger

from src.config import BRONZE_RETENTION_DAYS, BUCKET, GOLD_RETENTION_DAYS, SILVER_RETENTION_DAYS


def _delete_files_by_age(fs: s3fs.S3FileSystem, files: list, cutoff: datetime) -> int:
    """Delete every file whose last-modified timestamp is older than *cutoff*."""
    deleted = 0
    for path in files:
        file_info = fs.info(path)
        file_mtime = datetime.fromtimestamp(file_info["LastModified"].timestamp())
        if file_mtime < cutoff:
            fs.rm(path)
            logger.info(f"Deleted: {path}")
            deleted += 1
    return deleted


def clean_bronze(fs: s3fs.S3FileSystem) -> int:
    """Delete Bronze Parquet files older than BRONZE_RETENTION_DAYS days."""
    cutoff = datetime.now() - timedelta(days=BRONZE_RETENTION_DAYS)
    files = fs.glob(f"{BUCKET}/bronze/velib/date=*/*.parquet")
    deleted = 0
    for path in files:
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})", path)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            if file_date < cutoff:
                fs.rm(path)
                logger.info(f"Deleted Bronze: {path}")
                deleted += 1
    return deleted


def clean_silver(fs: s3fs.S3FileSystem) -> int:
    """Delete Silver Parquet files older than SILVER_RETENTION_DAYS days."""
    cutoff = datetime.now() - timedelta(days=SILVER_RETENTION_DAYS)
    files = fs.glob(f"{BUCKET}/silver/velib/*.parquet")
    return _delete_files_by_age(fs, files, cutoff)


def clean_gold(fs: s3fs.S3FileSystem) -> int:
    """Delete Gold Parquet files older than GOLD_RETENTION_DAYS days."""
    cutoff = datetime.now() - timedelta(days=GOLD_RETENTION_DAYS)
    files = fs.glob(f"{BUCKET}/gold/velib/*.parquet")
    return _delete_files_by_age(fs, files, cutoff)


def run_cleanup(fs: s3fs.S3FileSystem) -> dict:
    """Run retention cleanup on all layers and return a count summary."""
    return {
        "bronze": clean_bronze(fs),
        "silver": clean_silver(fs),
        "gold": clean_gold(fs),
    }

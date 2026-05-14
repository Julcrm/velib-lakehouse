"""
Nettoyage de la rétention des données sur MinIO.
Bronze : 7 jours — Silver/Gold : 30 jours.
"""
import re
from datetime import datetime, timedelta
import s3fs
from loguru import logger
from src.config import BUCKET, BRONZE_RETENTION_DAYS, SILVER_RETENTION_DAYS, GOLD_RETENTION_DAYS


def _delete_files_by_age(fs: s3fs.S3FileSystem, files: list, cutoff: datetime) -> int:
    """Supprime les fichiers dont la date de modification est antérieure au cutoff."""
    deleted = 0
    for path in files:
        file_info = fs.info(path)
        file_mtime = datetime.fromtimestamp(file_info["LastModified"].timestamp())
        if file_mtime < cutoff:
            fs.rm(path)
            logger.info(f"Supprimé : {path}")
            deleted += 1
    return deleted


def clean_bronze(fs: s3fs.S3FileSystem) -> int:
    """Supprime les fichiers Bronze plus vieux que BRONZE_RETENTION_DAYS jours."""
    cutoff = datetime.now() - timedelta(days=BRONZE_RETENTION_DAYS)
    files = fs.glob(f"{BUCKET}/bronze/velib/date=*/*.parquet")
    deleted = 0
    for path in files:
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})", path)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            if file_date < cutoff:
                fs.rm(path)
                logger.info(f"Supprimé Bronze : {path}")
                deleted += 1
    return deleted


def clean_silver(fs: s3fs.S3FileSystem) -> int:
    """Supprime les fichiers Silver plus vieux que SILVER_RETENTION_DAYS jours."""
    cutoff = datetime.now() - timedelta(days=SILVER_RETENTION_DAYS)
    files = fs.glob(f"{BUCKET}/silver/velib/*.parquet")
    return _delete_files_by_age(fs, files, cutoff)


def clean_gold(fs: s3fs.S3FileSystem) -> int:
    """Supprime les fichiers Gold plus vieux que GOLD_RETENTION_DAYS jours."""
    cutoff = datetime.now() - timedelta(days=GOLD_RETENTION_DAYS)
    files = fs.glob(f"{BUCKET}/gold/velib/*.parquet")
    return _delete_files_by_age(fs, files, cutoff)


def run_cleanup(fs: s3fs.S3FileSystem) -> dict:
    """Point d'entrée — nettoie toutes les couches et retourne le bilan."""
    return {
        "bronze": clean_bronze(fs),
        "silver": clean_silver(fs),
        "gold": clean_gold(fs),
    }
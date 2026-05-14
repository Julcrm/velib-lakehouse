"""
Ingestion des données Vélib depuis les APIs open data.
Écrit les snapshots bruts sur MinIO (couche Bronze).
"""
import json
import os
from datetime import datetime

import pandas as pd
import requests
import s3fs
from loguru import logger

from src.config import (
    BUCKET,
    VELIB_API_URL,
    VELIB_HEADERS,
    VELIB_PARAMS,
    VELIB_REFERENCE_URL,
)


# --- Fonctions génériques ---

def fetch_json(url: str, headers: dict = None, params: dict = None) -> dict | list:
    """Appel HTTP GET générique — retourne le JSON brut."""
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def write_parquet(df: pd.DataFrame, path: str, fs: s3fs.S3FileSystem) -> str:
    """Écriture Parquet sur MinIO."""
    with fs.open(path, "wb") as f:
        df.to_parquet(f, index=False)
    logger.info(f"Written to s3://{path}")
    return path


def write_json(payload: dict, path: str, fs: s3fs.S3FileSystem) -> str:
    """Écriture JSON sur MinIO."""
    with fs.open(path, "w") as f:
        json.dump(payload, f)
    logger.info(f"Written to s3://{path}")
    return path


def _build_filesystem(fs: s3fs.S3FileSystem | None) -> s3fs.S3FileSystem:
    """Retourne le filesystem fourni, ou en crée un depuis les variables d'env."""
    if fs is not None:
        return fs
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=endpoint,
        client_kwargs={"endpoint_url": endpoint},
    )


# --- Pipeline statut stations ---

def run(fs: s3fs.S3FileSystem | None = None) -> str:
    """Pipeline Bronze — snapshot statut des stations Vélib."""
    filesystem = _build_filesystem(fs)
    records = fetch_json(VELIB_API_URL, headers=VELIB_HEADERS, params=VELIB_PARAMS)
    df = pd.json_normalize(records)
    df["ingested_at"] = datetime.now()
    now = datetime.now()
    path = f"{BUCKET}/bronze/velib/date={now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.parquet"
    return write_parquet(df, path, filesystem)


# --- Pipeline référence stations ---

def run_reference(fs: s3fs.S3FileSystem | None = None) -> tuple[str, int]:
    """Pipeline Bronze — données de référence des stations (noms, capacités)."""
    filesystem = _build_filesystem(fs)
    payload = fetch_json(VELIB_REFERENCE_URL)
    stations = payload.get("data", {}).get("stations", [])
    if not stations:
        raise ValueError("Données de référence vides !")
    path = f"{BUCKET}/bronze/velib/reference/station_information.json"
    return write_json(payload, path, filesystem), len(stations)


if __name__ == "__main__":
    run()

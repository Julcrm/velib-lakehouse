"""
Ingestion des données Vélib depuis l'API Open Data Paris.
Écrit les snapshots bruts en Parquet sur MinIO (couche Bronze).
"""
import os
from datetime import datetime

import pandas as pd
import requests
import s3fs
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

VELIB_API_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
    "/velib-disponibilite-en-temps-reel/exports/json"
)
# Headers "ninja" — évite le blocage par l'API Open Data Paris
VELIB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
# Toutes les colonnes source — la sélection est déléguée à dbt (couche Silver)
VELIB_PARAMS = {
    "limit": -1,
}
BUCKET = os.getenv("BUCKET", "velib-lakehouse")


def fetch_velib_data() -> list[dict]:
    """Étape 1 — Appeler l'API Vélib open data Paris (endpoint export, mode ninja)."""
    logger.info("Fetching Vélib data from Open Data Paris API...")
    response = requests.get(VELIB_API_URL, headers=VELIB_HEADERS, params=VELIB_PARAMS, timeout=30)
    response.raise_for_status()
    records = response.json()
    logger.info(f"Fetched {len(records)} station records.")
    return records


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    """Étape 2 — Convertir la réponse JSON en DataFrame pandas."""
    df = pd.json_normalize(records)
    df["ingested_at"] = datetime.now()
    logger.info(f"DataFrame built: {len(df)} rows, {len(df.columns)} columns.")
    return df


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


def write_to_bronze(df: pd.DataFrame, fs: s3fs.S3FileSystem | None = None) -> str:
    """Étape 3 — Écrire en Parquet sur MinIO bucket bronze."""
    filesystem = _build_filesystem(fs)
    now = datetime.now()
    path = f"{BUCKET}/bronze/velib/date={now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.parquet"
    with filesystem.open(path, "wb") as f:
        df.to_parquet(f, index=False)
    logger.info(f"Written to s3://{path}")
    return path


def run(fs: s3fs.S3FileSystem | None = None) -> str:
    """Point d'entrée du producteur — enchaîne les 3 étapes. fs optionnel pour l'injection Dagster."""
    records = fetch_velib_data()
    df = build_dataframe(records)
    return write_to_bronze(df, fs=fs)


if __name__ == "__main__":
    run()

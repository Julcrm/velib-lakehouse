"""
Vélib data ingestion from the Paris Open Data API.
Writes raw snapshots to MinIO as Parquet files (Bronze layer).
"""
import json
import os
from datetime import datetime

import pandas as pd
import requests
import s3fs
from loguru import logger

from src.config import BUCKET, VELIB_API_URL, VELIB_HEADERS

# --- Generic helpers ---

def fetch_json(url: str, headers: dict = None, params: dict = None) -> dict | list:
    """Perform a generic HTTP GET and return the parsed JSON body."""
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def write_parquet(df: pd.DataFrame, path: str, fs: s3fs.S3FileSystem) -> str:
    """Write a DataFrame to a Parquet file on MinIO and return the path."""
    with fs.open(path, "wb") as f:
        df.to_parquet(f, index=False)
    logger.info(f"Written to s3://{path}")
    return path


def write_json(payload: dict, path: str, fs: s3fs.S3FileSystem) -> str:
    """Write a dict as a JSON file to MinIO and return the path."""
    with fs.open(path, "w") as f:
        json.dump(payload, f)
    logger.info(f"Written to s3://{path}")
    return path


def _build_filesystem(fs: s3fs.S3FileSystem | None) -> s3fs.S3FileSystem:
    """Return the provided filesystem, or build one from environment variables."""
    if fs is not None:
        return fs
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=endpoint,
        client_kwargs={"endpoint_url": endpoint},
    )


# --- Station status pipeline ---

def run(fs: s3fs.S3FileSystem | None = None) -> str:
    """Fetch a Vélib station snapshot and write it to the Bronze layer as Parquet."""
    filesystem = _build_filesystem(fs)
    records = fetch_json(VELIB_API_URL, headers=VELIB_HEADERS)
    df = pd.json_normalize(records)
    df["ingested_at"] = datetime.now()

    # Normalise coordinates — the API may return either flat or nested format
    if "coordonnees_geo" in df.columns and "coordonnees_geo.lon" not in df.columns:
        df["coordonnees_geo.lon"] = df["coordonnees_geo"].apply(
            lambda x: x.get("lon") if isinstance(x, dict) else None
        )
        df["coordonnees_geo.lat"] = df["coordonnees_geo"].apply(
            lambda x: x.get("lat") if isinstance(x, dict) else None
        )
        df = df.drop(columns=["coordonnees_geo"])

    now = datetime.now()
    date_part = now.strftime("%Y-%m-%d")
    time_part = now.strftime("%H-%M-%S")
    path = f"{BUCKET}/bronze/velib/date={date_part}/{time_part}.parquet"
    return write_parquet(df, path, filesystem)


if __name__ == "__main__":
    run()

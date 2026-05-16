"""
Centralized configuration for the Vélib Lakehouse project.
All URLs, constants, and parameters are defined here.
"""
import os

# --- Vélib Open Data Paris API ---
VELIB_API_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
)

VELIB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# --- Data retention (days) ---
BRONZE_RETENTION_DAYS = 7
SILVER_RETENTION_DAYS = 30
GOLD_RETENTION_DAYS = 30

# --- MinIO / S3 ---
BUCKET = os.getenv("BUCKET", "velib-lakehouse")

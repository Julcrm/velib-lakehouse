"""
Configuration centralisée du projet Vélib Lakehouse.
Toutes les URLs, constantes et paramètres sont définis ici.
"""
import os

# --- API Vélib Open Data Paris ---
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

# --- Rétention des données (jours) ---
BRONZE_RETENTION_DAYS = 7
SILVER_RETENTION_DAYS = 30
GOLD_RETENTION_DAYS = 30

# --- MinIO / S3 ---
BUCKET = os.getenv("BUCKET", "velib-lakehouse")

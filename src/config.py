"""
Configuration centralisée du projet Vélib Lakehouse.
Toutes les URLs, constantes et paramètres sont définis ici.
"""
import os

# --- API Vélib Open Data Paris ---
VELIB_API_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
)

# --- API Vélib Open Data Paris (données de référence stations) ---
VELIB_REFERENCE_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
)

VELIB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
VELIB_PARAMS = {"limit": -1}

# --- MinIO / S3 ---
BUCKET = os.getenv("BUCKET", "velib-lakehouse")

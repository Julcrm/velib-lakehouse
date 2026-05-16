"""Unit tests for src.ingestion.producer."""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests


def test_fetch_json_success() -> None:
    """fetch_json returns parsed JSON when the request succeeds."""
    payload = [{"station_code": "12345", "bikes_available": 5}]
    mock_response = MagicMock()
    mock_response.json.return_value = payload
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response):
        from src.ingestion.producer import fetch_json

        result = fetch_json("http://fake-url")

    assert result == payload


def test_fetch_json_raises_on_error() -> None:
    """fetch_json propagates an HTTPError when the server returns 5xx."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with patch("requests.get", return_value=mock_response):
        from src.ingestion.producer import fetch_json

        with pytest.raises(requests.HTTPError):
            fetch_json("http://fake-url")


def test_coordinate_normalisation() -> None:
    """run() expands nested coordonnees_geo dicts into .lat and .lon columns."""
    records = [
        {"station_code": "abc", "coordonnees_geo": {"lat": 48.8566, "lon": 2.3522}},
        {"station_code": "def", "coordonnees_geo": {"lat": 48.8600, "lon": 2.3400}},
    ]
    captured: dict = {}

    def fake_write_parquet(df: pd.DataFrame, path: str, fs: object) -> str:
        captured["df"] = df
        return path

    with (
        patch("src.ingestion.producer.fetch_json", return_value=records),
        patch("src.ingestion.producer.write_parquet", side_effect=fake_write_parquet),
        patch("src.ingestion.producer._build_filesystem", return_value=MagicMock()),
    ):
        from src.ingestion.producer import run

        run()

    df = captured["df"]
    assert "coordonnees_geo.lat" in df.columns
    assert "coordonnees_geo.lon" in df.columns
    assert "coordonnees_geo" not in df.columns

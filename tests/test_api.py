"""Unit tests for src.serving.api."""
import pytest
from fastapi.testclient import TestClient

from src.serving.api import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_health(client: TestClient) -> None:
    """GET /health returns 200 and {"status": "ok"}."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_endpoints_require_api_key(client: TestClient) -> None:
    """GET /pipeline/status with an invalid API key returns 403."""
    response = client.get("/pipeline/status", headers={"X-API-Key": "wrong"})
    assert response.status_code == 403

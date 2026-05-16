# =============================================================================
# FastAPI Serving — Vélib Lakehouse API
# Exposes pipeline metrics and Vélib insights via REST endpoints on port 8000.
# Runtime: Python 3.11-slim, uv, fastapi, uvicorn, duckdb, httpx
# =============================================================================

FROM python:3.11-slim

WORKDIR /opt/dagster/app

ENV PYTHONPATH=/opt/dagster/app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "src.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
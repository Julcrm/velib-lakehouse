FROM python:3.11-slim

WORKDIR /opt/dagster/app

# Installe uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copie les fichiers de dépendances en premier pour le cache Docker
COPY pyproject.toml uv.lock ./

# Installe les dépendances sans les dev dependencies
RUN uv sync --frozen --no-dev

# Copie le code source
COPY ../../src/serving .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "src.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
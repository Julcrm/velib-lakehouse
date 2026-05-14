FROM python:3.11-slim

WORKDIR /opt/dagster/app

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

# Installe uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copie les fichiers de dépendances en premier pour le cache Docker
COPY pyproject.toml uv.lock ./

# Installe les dépendances sans les dev dependencies
RUN uv sync --frozen --no-dev

COPY . .

EXPOSE 4000

CMD ["uv", "run", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.dagster"]
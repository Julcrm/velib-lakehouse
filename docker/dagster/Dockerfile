FROM python:3.11-slim

WORKDIR /opt/dagster/app

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# dbt deps — télécharge dbt_utils pendant le build
COPY dbt/ /opt/dagster/app/dbt/
WORKDIR /opt/dagster/app/dbt
RUN uv run dbt deps --profiles-dir . --no-partial-parse 2>/dev/null || uv run dbt deps

WORKDIR /opt/dagster/app
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY . .

EXPOSE 4000

CMD ["uv", "run", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.dagster.definitions"]
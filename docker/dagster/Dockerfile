FROM python:3.11-slim

WORKDIR /opt/dagster/app

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY . .

RUN uv run dbt deps --project-dir /opt/dagster/app/dbt --profiles-dir /opt/dagster/app/dbt --no-partial-parse

EXPOSE 4000

CMD ["uv", "run", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.dagster.definitions"]
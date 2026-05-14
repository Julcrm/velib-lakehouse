FROM python:3.11-slim

WORKDIR /opt/dagster/app

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

RUN pip install --upgrade pip
RUN pip install --no-cache-dir dagster dagster-webserver requests pandas pyarrow s3fs fastapi uvicorn duckdb

COPY . .

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.dagster"]

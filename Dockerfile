
FROM python:3.11-slim

# On installe uv depuis l'image officielle
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Optimisation du cache
COPY pyproject.toml uv.lock ./
# Installation sans dev dependencies
RUN uv sync --frozen --no-dev

COPY . .

CMD ["uv", "run", "src/main.py"]

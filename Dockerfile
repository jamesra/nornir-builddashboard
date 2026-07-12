# Lightweight standalone image for the Nornir build dashboard.
# The dashboard only needs FastAPI + paho-mqtt; it does not depend on the rest
# of the Nornir monorepo, so this image stays small.
FROM python:3.14-slim-bookworm

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NORNIR_DASHBOARD_HOST=0.0.0.0 \
    NORNIR_DASHBOARD_PORT=8087 \
    NORNIR_DASHBOARD_DB=/data/nornir-dashboard.db

WORKDIR /app

# Install dependencies first for better layer caching.
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md
COPY nornir_dashboard /app/nornir_dashboard

RUN pip install --upgrade pip \
    && pip install .

# Persisted run/event history lives here (mount a volume to keep it).
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8087

CMD ["nornir-dashboard"]

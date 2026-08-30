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
COPY constraints.txt /app/constraints.txt
COPY README.md /app/README.md
COPY nornir_dashboard /app/nornir_dashboard

# Constraints pin the dependency tree so two builds of the same commit ship the
# same versions; pyproject.toml keeps permissive bounds for source installs.
RUN pip install --upgrade pip \
    && pip install -c /app/constraints.txt .

# Persisted run/event history lives here (mount a volume to keep it).
# Owned by the unprivileged runtime user so a named volume is writable; a bind
# mount from the host must be chowned to this uid (or use --user).
RUN mkdir -p /data \
    && groupadd --gid 10001 dashboard \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin dashboard \
    && chown -R 10001:10001 /data
VOLUME ["/data"]

EXPOSE 8087

# The service is network-exposed, so it does not run as root.
USER 10001:10001

# No curl in the slim image; use the interpreter that is already present.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import os,urllib.request,sys;\
port=os.environ.get('NORNIR_DASHBOARD_PORT','8087');\
sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{port}/api/health', timeout=4).status == 200 else 1)"]

CMD ["nornir-dashboard"]

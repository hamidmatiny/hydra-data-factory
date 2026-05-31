# ---------------------------------------------------------------------------
# Build stage — compile/install Python dependencies with build tooling
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --prefix=/install -r requirements.txt

# ---------------------------------------------------------------------------
# Runtime stage — slim image with non-root execution
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    INPUT_DIR=/data/telemetry \
    OUTPUT_DIR=/data/parquet \
    DLQ_DIR=/data/dead_letter \
    POLL_INTERVAL_SECONDS=15

LABEL org.opencontainers.image.title="hydra-data-factory" \
      org.opencontainers.image.description="Containerized AV telemetry ETL pipeline"

WORKDIR /app

RUN groupadd --system appuser \
    && useradd --system --gid appuser --create-home --home-dir /home/appuser appuser

COPY --from=builder /install /usr/local
COPY src/ ./src/
COPY config/ ./config/

RUN mkdir -p /data/telemetry /data/parquet /data/dead_letter /app/logs \
    && chown -R appuser:appuser /data /app /home/appuser

USER appuser

CMD ["python", "-m", "src.transform.run_etl_container"]

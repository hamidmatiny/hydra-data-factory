# Phase 3 Completion — Containerization, Data Contract Validation, and Local Cloud Pipeline Emulation

**Project:** hydra-data-factory  
**Phase:** 3 of N  
**Status:** Complete  

---

## Overview

Phase 3 hardens the Hydra Data Factory into a **Reproducible Data Pipeline** platform by introducing:

1. **Data Contracts and Schema Governance** via Pandera **Data Quality Gates**
2. **Containerized ETL Orchestration** with a multi-stage, non-root Dockerfile
3. **Local Cloud Pipeline Emulation** using Docker Compose to model an enterprise AWS ingestion → transform → lakehouse flow

This phase applies **Defensive Data Engineering** principles: contract violations never crash the pipeline — they are logged, counted, and routed to the Dead-Letter Queue (DLQ) before any Parquet partition is written.

---

## Architecture Overview

```
┌──────────────────── Docker Compose Stack ────────────────────┐
│                                                              │
│  ┌─────────────┐    shared volume     ┌──────────────────┐ │
│  │  simulator  │ ──▶ /data/telemetry ─▶│  etl-processor   │ │
│  │  (Phase 1)  │                       │  (Phase 2 + 3)   │ │
│  └─────────────┘                       └────────┬─────────┘ │
│                                                  │           │
│                     Pandera Contract Gate        │           │
│                     PyArrow Parquet Writer       ▼           │
│                                         /data/parquet        │
│                                         /data/dead_letter    │
└──────────────────────────────────────────────────────────────┘
         │ host bind mounts sync to ./output/ and ./logs/
         ▼
   Immediate local inspection on developer machine
```

### New Components

| Artifact | Purpose |
|----------|---------|
| `config/schema_contract.py` | Pandera `DataFrameSchema` contract + `apply_data_contract_gate()` |
| `Dockerfile` | Multi-stage build (`python:3.11-slim`), non-root `appuser`, default env paths |
| `docker-compose.yml` | `simulator` + `etl-processor` services with shared volumes |
| `src/transform/run_etl_container.py` | Polling container orchestrator with incremental file tracking |

### Processing Flow (with Contract Gate)

```
JSON batch → Pydantic triage → normalize/flatten → Pandera contract gate → PyArrow cast → Hive Parquet
                              ↓ failures              ↓ failures
                           DLQ (triage)            DLQ (contract)
```

---

## Schema Enforcement Summary

The Pandera contract (`TELEMETRY_DATA_CONTRACT`) guards the lakehouse immediately before Parquet materialization — after Pydantic record triage but before PyArrow typing.

### Contract Rules

| Column | Pandera Type | Validation | Rationale |
|--------|--------------|------------|-----------|
| `vehicle_id` | `str` | Regex `^TORC-AV-\d{3}$`, non-null | Prevents fleet ID drift and malformed identifiers |
| `timestamp` | `DateTime (UTC)` | Non-null | Guarantees partition key derivability (`year`, `month`) |
| `speed_mph` | `float` | Range `0.0`–`120.0` | Logical AV operating envelope (stricter than raw Pydantic max of 200) |
| `latitude` | `float` | Range `-90.0`–`90.0` | WGS-84 bounds |
| `longitude` | `float` | Range `-180.0`–`180.0` | WGS-84 bounds |

### Gate Behavior

1. The flat Pandas DataFrame is validated with `lazy=True` to collect all failures per batch.
2. **Passing rows** proceed to `pa.Table.from_pandas()` and Hive-partitioned Parquet writes.
3. **Failing rows** emit `WARNING` logs with column/check detail, append to the in-memory DLQ with `corruption_type=data_contract_violation`, and increment `contract_rejected_records` in operational stats.
4. If every row in a batch fails the contract, Parquet write is skipped for that batch — the pipeline continues on the next poll cycle.

### Example Contract Rejection Log

```
[WARNING] [schema_contract.py:NN]: Data contract gate rejection [pre_parquet_batch] index=3: speed_mph: in_range(0.0, 120.0)
[WARNING] [transformer.py:NN]: DLQ routing [data_contract_violation] from pre_parquet_batch: data_contract_violation: speed_mph: in_range(0.0, 120.0)
```

This prevents bad data drifting into `year=YYYY/month=MM/vehicle_id=VAL/` partitions — a core **Data Lakehouse Partitioning** hygiene practice.

---

## Containerized Execution Guide

### Prerequisites

- Docker Engine 24+ and Docker Compose v2
- Repository cloned locally

### 1. Build and run the full pipeline

From the repository root:

```bash
docker compose up --build
```

This command:

1. Builds the multi-stage `hydra-data-factory` image (shared by both services)
2. Starts `hydra-simulator` — loops `src.simulator.generator` every 30 seconds (+ 5s pause)
3. Starts `hydra-etl-processor` — polls `/data/telemetry` every 15 seconds for new JSON batches

### 2. Inspect host-synced outputs

All container paths bind-mount to the host `output/` tree:

```bash
# Raw JSON batches from simulator
ls -la output/telemetry/

# Hive-partitioned Parquet from ETL
find output/parquet -type f -name "*.parquet"

# DLQ audit files (triage + contract rejections)
ls -la output/dead_letter/

# Unified pipeline logs
tail -f logs/pipeline.log
```

Expected Parquet layout:

```
output/parquet/
└── year=2026/
    └── month=05/
        ├── vehicle_id=TORC-AV-001/
        │   └── part-0.parquet
        ├── vehicle_id=TORC-AV-002/
        │   └── part-0.parquet
        └── vehicle_id=TORC-AV-003/
            └── part-0.parquet
```

### 3. Run a one-shot ETL cycle (debugging)

```bash
docker compose run --rm \
  -e RUN_ONCE=true \
  -e POLL_INTERVAL_SECONDS=5 \
  etl-processor
```

### 4. Stop the stack

```bash
docker compose down
```

### Container Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_DIR` | `/data/telemetry` | Raw JSON batch input |
| `OUTPUT_DIR` | `/data/parquet` | Hive Parquet output base |
| `DLQ_DIR` | `/data/dead_letter` | DLQ audit JSON output |
| `POLL_INTERVAL_SECONDS` | `15` | ETL poll frequency |
| `RUN_ONCE` | `false` | Exit after one processing cycle |

---

## Dockerfile Design

| Stage | Base | Purpose |
|-------|------|---------|
| **builder** | `python:3.11-slim` | Installs `build-essential`, pip installs all requirements to `/install` |
| **runtime** | `python:3.11-slim` | Copies `/install` + `src/` + `config/` only; no build tools |

Security controls:

- Non-root `appuser` system account
- `PYTHONPATH=/app` for module resolution
- Writable `/data/*` and `/app/logs` owned by `appuser`
- Default `CMD` runs the container orchestrator

---

## End-to-End Local Execution (Non-Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Generate raw telemetry
python -m src.simulator.generator \
  --output-dir output/telemetry \
  --duration 60 \
  --rate 10 \
  --failure-rate 0.08

# Run ETL with Pandera contract gate
python -m src.transform.run_etl \
  --input-dir output/telemetry \
  --output-dir output/parquet \
  --dead-letter-dir output/dead_letter
```

---

## Deliverables Checklist

- [x] `config/schema_contract.py` — Pandera `DataFrameSchema` with regex, range, and nullability rules
- [x] Contract gate integrated into `TelemetryTransformer.build_arrow_table()`
- [x] Contract failures routed to DLQ with `data_contract_violation` corruption type
- [x] Multi-stage `Dockerfile` with non-root `appuser` and default env paths
- [x] `docker-compose.yml` with `simulator` + `etl-processor` and host volume mounts
- [x] `src/transform/run_etl_container.py` — polling incremental orchestrator
- [x] `pandera` added to `requirements.txt`
- [x] Updated `README.md` with **Data Quality Gates**, **Containerized ETL Orchestration**, **Reproducible Data Pipelines**, **Defensive Data Engineering**
- [x] This completion document

---

## Next Steps (Phase 4 Preview)

- Push container image to Amazon ECR
- **Infrastructure as Code (IaC)** — Terraform modules for ECS/Fargate task definitions
- S3 sync of Parquet partitions and DLQ objects via `boto3`
- AWS Glue Data Catalog registration aligned to Hive partitions
- CI/CD pipeline for automated `docker compose` integration tests

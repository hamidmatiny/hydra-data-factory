# Phase 2 Completion — Core Data Transformation Layer, Partitioned Parquet Writer, and GitHub Discoverability

**Project:** hydra-data-factory  
**Phase:** 2 of N  
**Status:** Complete  

---

## Overview

Phase 2 delivers the **Core Data Transformation Layer** for the Hydra Data Factory — a modular, high-performance **ETL pipeline** that ingests raw JSON telemetry batches, performs **Distributed Stream Triage** on corrupted records, enforces strict typing via **PyArrow** schema contracts, and materializes clean data as **Apache Parquet** under **Data Lakehouse Partitioning** (Hive-style `year/month/vehicle_id` directories).

This phase transforms the Phase 1 simulator output from append-only JSON batches into query-optimized **Columnar Storage Optimization** artifacts suitable for Athena, Spark, DuckDB, and future AWS Glue catalog integration.

---

## ETL Architecture Overview

The transformation layer lives under `src/transform/` and follows a linear, fault-tolerant pipeline design:

```
┌─────────────────────┐     ┌──────────────────────────┐     ┌─────────────────────────┐
│  output/telemetry/  │────▶│   TelemetryTransformer   │────▶│   output/parquet/       │
│  telemetry_*.json   │     │  ingest → triage → cast  │     │  year=…/month=…/*.pq    │
└─────────────────────┘     └────────────┬─────────────┘     └─────────────────────────┘
                                         │
                              corrupt / invalid
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │ output/dead_letter/  │
                              │   dlq_*.json         │
                              └──────────────────────┘
```

### Pipeline Stages

| Stage | Module | Description |
|-------|--------|-------------|
| **Ingestion** | `transformer.py` | Scans `input_dir` for `telemetry_*.json` batch files using `pathlib.Path.glob`. Each file contains a JSON array of ping objects. |
| **Triage** | `transformer.py` | Inspects every record against known simulator corruption modes (`drop_vehicle_id`, `malformed_gps`, `invalid_speed`, `null_timestamp`, `truncate_json`) plus full Pydantic schema validation. Failures emit `WARNING` logs and route to the DLQ — the pipeline never crashes on bad data. |
| **Normalization** | `transformer.py` | Flattens `gps_coordinates.latitude` / `gps_coordinates.longitude` to root columns. Parses timestamps to UTC and derives Hive partition keys `year` and `month`. |
| **Typing** | `transformer.py` | Builds a Pandas DataFrame, then casts to a strict PyArrow schema with `pa.timestamp('us', tz='UTC')` and `float64` geospatial columns. |
| **Materialization** | `transformer.py` | Writes via `pyarrow.dataset.write_dataset` with Hive partitioning on `year`, `month`, and `vehicle_id`. |
| **Orchestration** | `run_etl.py` | CLI wrapper with operational summary logging (inputs, Parquet rows, DLQ totals). |

### Fault-Tolerant Triage (DLQ)

The **Distributed Stream Triage** layer mirrors production dead-letter patterns:

- **In-memory DLQ** — `DeadLetterRecord` entries capture the raw payload, source filename, rejection reason, and corruption type.
- **Optional persistence** — When `--dead-letter-dir` is set, rejected records flush to `dlq_{timestamp}.json` for offline audit and replay.
- **Non-blocking** — A single corrupt ping in a batch of 100 does not fail the other 99.

---

## PyArrow Output Schema

| Column | PyArrow Type | Source |
|--------|--------------|--------|
| `timestamp` | `timestamp[us, tz=UTC]` | Parsed from ISO-8601 or epoch |
| `vehicle_id` | `string` | Root field + partition key |
| `trip_id` | `string` | UUID as string |
| `speed_mph` | `float64` | Root field |
| `latitude` | `float64` | Flattened from `gps_coordinates` |
| `longitude` | `float64` | Flattened from `gps_coordinates` |
| `sensor_status` | `string` | From `system_logs` |
| `brake_pressure` | `string` | From `system_logs` |
| `lidar_temp_c` | `float64` | From `system_logs` |
| `compute_load_pct` | `float64` | From `system_logs` |
| `hardware_version` | `string` | Root field |
| `year` | `string` | Derived partition column (`YYYY`) |
| `month` | `string` | Derived partition column (`MM`) |

Partition columns (`year`, `month`, `vehicle_id`) are encoded in the directory path by PyArrow's dataset engine and excluded from the embedded Parquet payload files.

---

## Storage Directory Tree Layout

After a successful ETL run, the **Data Lakehouse Partitioning** layout under `output/parquet/` looks like:

```
output/
├── telemetry/                              # Phase 1 raw JSON batches
│   ├── telemetry_TORC-AV-001_20260531T135255_470403Z.json
│   └── telemetry_TORC-AV-002_20260531T135256_477662Z.json
│
├── parquet/                                # Phase 2 Hive-partitioned Parquet
│   └── year=2026/
│       └── month=05/
│           ├── vehicle_id=TORC-AV-001/
│           │   └── part-0.parquet
│           ├── vehicle_id=TORC-AV-002/
│           │   └── part-0.parquet
│           └── vehicle_id=TORC-AV-003/
│               └── part-0.parquet
│
└── dead_letter/                            # Rejected records audit trail
    └── dlq_20260531T140012_123456Z.json
```

Each leaf directory conforms to the required pattern:

```
output_base_dir/year=YYYY/month=MM/vehicle_id=VAL/*.parquet
```

---

## End-to-End Local Execution Blueprint

All commands assume the repository root and an activated virtual environment.

### 0. Environment setup (first time only)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

> **Note:** Python 3.11 or 3.12 is recommended for prebuilt `pyarrow` wheels. Python 3.14 may require building from source.

### 1. Generate raw telemetry JSON batches (Phase 1 simulator)

```bash
python -m src.simulator.generator \
  --vehicle-ids TORC-AV-001 TORC-AV-002 TORC-AV-003 \
  --output-dir output/telemetry \
  --duration 60 \
  --rate 10 \
  --failure-rate 0.08 \
  --seed 42
```

This produces `telemetry_{vehicle_id}_{timestamp}.json` files with ~5% intentionally corrupted records for triage testing.

### 2. Run the ETL pipeline (Phase 2 transform)

```bash
python -m src.transform.run_etl \
  --input-dir output/telemetry \
  --output-dir output/parquet \
  --dead-letter-dir output/dead_letter
```

### 3. Verify outputs

```bash
# Inspect Hive partition tree
find output/parquet -type f -name "*.parquet"

# Read Parquet with PyArrow (quick sanity check)
python -c "
import pyarrow.parquet as pq
table = pq.read_table('output/parquet')
print(table.schema)
print(table.to_pandas().head())
"

# Review DLQ rejections
cat output/dead_letter/dlq_*.json | head -80

# Check operational logs
tail -30 logs/pipeline.log
```

### 4. Expected operational summary (stdout / logs)

```
============================================================
ETL OPERATIONAL SUMMARY
============================================================
Input JSON batch files scanned : 12
Total telemetry records ingested: 580
Records compiled to Parquet    : 534
Records rejected by triage layer : 46
Acceptance rate                : 92.07%
Parquet output base directory  : output/parquet
Dead-letter audit directory    : output/dead_letter
============================================================
```

---

## CLI Reference — `run_etl.py`

| Flag | Default | Description |
|------|---------|-------------|
| `--input-dir` | `output/telemetry` | Directory of `telemetry_*.json` batch files |
| `--output-dir` | `output/parquet` | Base path for Hive-partitioned Parquet |
| `--dead-letter-dir` | `output/dead_letter` | DLQ audit JSON output (empty string to skip) |

---

## Programmatic Usage

```python
from src.transform.transformer import TelemetryTransformer

transformer = TelemetryTransformer(
    input_dir="output/telemetry",
    dead_letter_dir="output/dead_letter",
)

stats = transformer.run(output_base_dir="output/parquet")

print(f"Valid: {stats.valid_records}, Rejected: {stats.rejected_records}")
```

---

## GitHub Discoverability & Search Optimization

This repository is structured and documented to rank well for high-intent **data engineering** and **AWS** search terms.

### Recommended GitHub Repository Topics

Add these in **GitHub → Settings → General → Topics**:

| Topic | Search Intent |
|-------|---------------|
| `aws-data-pipeline` | Cloud-native ingestion & orchestration |
| `etl-pipeline` | Extract-transform-load patterns |
| `pyarrow` | Columnar in-memory processing |
| `apache-parquet` | Columnar storage format |
| `telemetry-processing` | Time-series / IoT / AV data |
| `autonomous-vehicle` | Domain-specific AV fleet data |
| `data-lakehouse` | Partitioned analytical storage |
| `terraform-infrastructure` | Infrastructure as Code (IaC) |
| `python-data-engineering` | Python ETL tooling |
| `serverless-etl` | Lambda / event-driven patterns (future) |
| `data-quality` | DLQ triage & validation |
| `hive-partitioning` | Directory-based partition pruning |

### Keywords Woven Into Documentation

The README and phase completion docs intentionally index for:

- **Infrastructure as Code (IaC)** — Terraform module placeholder
- **Distributed Stream Triage** — DLQ routing without pipeline failure
- **Columnar Storage Optimization** — Parquet + PyArrow typed writes
- **Data Lakehouse Partitioning** — Hive `year/month/vehicle_id` layout

---

## Deliverables Checklist

- [x] `src/transform/transformer.py` — `TelemetryTransformer` with ingest, triage, PyArrow casting, Hive Parquet writes
- [x] `src/transform/run_etl.py` — CLI orchestration with operational summary
- [x] `src/transform/__init__.py` — Public API exports
- [x] Fault-tolerant DLQ for all simulator corruption modes
- [x] Flattened `latitude` / `longitude` root columns
- [x] `pa.timestamp('us', tz='UTC')` enforcement
- [x] Hive path layout: `year=YYYY/month=MM/vehicle_id=VAL/*.parquet`
- [x] Updated `README.md` with end-to-end commands and discoverability keywords
- [x] This completion document

---

## Next Steps (Phase 3 Preview)

- **Infrastructure as Code (IaC)** — Terraform modules for S3 landing zones, IAM, and Glue Data Catalog
- S3 upload of Parquet partitions via `boto3`
- AWS Lambda handler wrapping `TelemetryTransformer` for event-driven **serverless ETL**
- Glue/Athena external table DDL aligned to Hive partitions

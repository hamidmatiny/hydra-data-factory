# Phase 7 — Apache Airflow Orchestration

**Project:** hydra-data-factory  
**Commit:** `2861c81` — *Phase 7: Add DAGs and Docker Compose file*  
**Status:** Complete (verified from repository)

---

## Overview

Phase 7 wires the existing Hydra ETL pipeline into a scheduled Apache Airflow DAG, runnable locally via Docker Compose. The DAG chains telemetry generation, local ETL, output validation, and optional S3/Glue sync—the same processing stages previously invoked manually or through `docker-compose.yml`.

---

## What Was Built

**DAG:** `dags/hydra_pipeline_dag.py`

- `dag_id`: `hydra_av_telemetry_pipeline`
- Schedule: `@daily` (with `catchup=False`, `start_date=2025-01-01`)
- Retries: 2, retry delay 5 minutes

**Task graph** (as introduced in `2861c81`; MLflow added later in Phase 8):

| Task ID | Operator | Role |
|---------|----------|------|
| `generate_telemetry` | BashOperator | Runs `src.simulator.generator` (30 s, 10 pps, 8% corruption) |
| `run_etl` | BashOperator | Runs `src.transform.run_etl` with AWS env vars cleared (local Parquet + DLQ) |
| `validate_output` | PythonOperator | Computes rejection rate from DLQ audit files; fails above 20% threshold |
| `sync_to_s3` | PythonOperator | Uploads Parquet to S3 and syncs Glue when `DATA_LAKE_BUCKET` is set |

**Local stack:** `docker-compose.airflow.yml`

- Airflow **2.9.0**, **CeleryExecutor**
- Postgres metadata DB, Redis broker
- Project `src/`, `config/`, and `dags/` mounted into containers
- Host data dirs: `output/telemetry`, `output/parquet`, `output/dead_letter`
- UI: port **8080** (default credentials documented in README)

**Files added in commit `2861c81`:** `dags/__init__.py`, `dags/hydra_pipeline_dag.py`, `docker-compose.airflow.yml`, README section updates.

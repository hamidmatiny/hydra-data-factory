# Phase 8 — MLflow Experiment Tracking (Airflow Path)

**Project:** hydra-data-factory  
**Commit:** `537cee0` — *dag and mlflow docker compose file*  
**Status:** Complete (verified from repository)

---

## Overview

Phase 8 adds MLflow experiment tracking to the Airflow orchestration path. A dedicated MLflow server runs alongside the Airflow CeleryExecutor stack; a final DAG task logs run metrics pulled from upstream task XComs. MLflow is not integrated into `src/` or the Lambda/Step Functions path.

---

## What Was Built

**MLflow service** (`docker-compose.airflow.yml`)

- Custom image built from `mlflow/Dockerfile` (Python 3.11-slim, **mlflow==2.14.1**)
- Server command via `mlflow/entrypoint.sh`:
  - Backend store: `sqlite:////mlflow/data/mlflow.db` (persisted volume `mlflow-data`)
  - Artifact root: `s3://${DATA_LAKE_BUCKET}/mlflow-artifacts/` (requires `DATA_LAKE_BUCKET` in `.env`)
- Host port mapping: **5001:5000**
- Health check: `curl --fail http://localhost:5000/health`

**Airflow integration**

- `MLFLOW_TRACKING_URI: http://mlflow:5000` set in the shared Airflow environment
- `mlflow>=2.14.0` included in `_PIP_ADDITIONAL_REQUIREMENTS` for worker containers

**DAG task:** `log_run_to_mlflow` (`dags/hydra_pipeline_dag.py`)

- `PythonOperator` with `trigger_rule="all_done"` (runs even when upstream tasks fail)
- Sets experiment **`hydra_av_telemetry_pipeline`**
- Pulls XCom from `validate_output` and `sync_to_s3`
- Logs params (data interval, rejection threshold), metrics (`total_records`, `valid_records`, `rejected_records`, `rejection_rate`, `pipeline_duration_seconds`), and tags (`dag_run_id`, `s3_key`, `glue_table`)
- Failures are **non-fatal** — caught and logged as warnings

**Updated task chain:**

```
generate_telemetry → run_etl → validate_output → sync_to_s3 → log_run_to_mlflow
```

**Files changed in commit `537cee0`:** `dags/hydra_pipeline_dag.py`, `docker-compose.airflow.yml`, `mlflow/Dockerfile`, `mlflow/entrypoint.sh`.

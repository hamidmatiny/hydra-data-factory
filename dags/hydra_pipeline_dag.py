"""Airflow DAG — Hydra AV telemetry ingest, ETL, validation, and S3/Glue sync."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
DATA_ROOT = Path("/opt/airflow/data")
TELEMETRY_DIR = DATA_ROOT / "telemetry"
PARQUET_DIR = DATA_ROOT / "parquet"
DLQ_DIR = DATA_ROOT / "dead_letter"
PIPELINE_LOG = Path(PROJECT_ROOT) / "logs" / "pipeline.log"

REJECTION_RATE_THRESHOLD = 0.20
INGESTED_RECORDS_PATTERN = re.compile(r"Total telemetry records ingested:\s*(\d+)")

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _count_dlq_rejections(dlq_dir: Path) -> tuple[int, str | None]:
    """Return rejection count from the most recently modified DLQ audit file."""
    dlq_files = sorted(dlq_dir.glob("dlq_*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not dlq_files:
        return 0, None

    latest = dlq_files[0]
    with latest.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        return 0, latest.name

    return len(payload), latest.name


def _parse_total_processed(log_path: Path) -> int:
    """Extract the latest ingested-record count from the Hydra pipeline log."""
    if not log_path.is_file():
        return 0

    matches = INGESTED_RECORDS_PATTERN.findall(log_path.read_text(encoding="utf-8"))
    return int(matches[-1]) if matches else 0


def validate_output(**context: Any) -> None:
    """Report DLQ metrics and fail when rejection rate exceeds the SLA threshold."""
    task_logger = context["ti"].log

    total_rejected, dlq_file = _count_dlq_rejections(DLQ_DIR)
    total_processed = _parse_total_processed(PIPELINE_LOG)

    if total_processed == 0:
        rejection_rate = 0.0
    else:
        rejection_rate = total_rejected / total_processed

    task_logger.info("DLQ file inspected       : %s", dlq_file or "none")
    task_logger.info("total_rejected           : %d", total_rejected)
    task_logger.info("total_processed          : %d", total_processed)
    task_logger.info("rejection_rate           : %.4f", rejection_rate)

    if rejection_rate > REJECTION_RATE_THRESHOLD:
        raise AirflowFailException(
            f"Rejection rate {rejection_rate:.2%} exceeds threshold "
            f"{REJECTION_RATE_THRESHOLD:.0%} "
            f"(rejected={total_rejected}, processed={total_processed})."
        )


def sync_to_s3(**context: Any) -> None:
    """Upload validated local Parquet to S3 and sync the Glue catalog."""
    from dotenv import load_dotenv

    from src.transform.aws_sink import write_analytics_parquet_to_glue
    from src.utils.aws_config import get_aws_config

    task_logger = context["ti"].log
    load_dotenv(Path(PROJECT_ROOT) / ".env")

    bucket = os.environ.get("DATA_LAKE_BUCKET", "").strip()
    if not bucket:
        task_logger.info("S3 sink skipped — local mode")
        return

    aws_config = get_aws_config()
    if aws_config is None:
        task_logger.info("S3 sink skipped — local mode")
        return

    if not PARQUET_DIR.is_dir() or not any(PARQUET_DIR.rglob("*.parquet")):
        task_logger.warning("No Parquet files found under %s; nothing to sync.", PARQUET_DIR)
        return

    dataset = ds.dataset(str(PARQUET_DIR), format="parquet", partitioning="hive")
    frame = dataset.to_table().to_pandas()

    if frame.empty:
        task_logger.warning("Parquet dataset is empty; skipping S3 sync.")
        return

    if "device_type" not in frame.columns and "vehicle_id" in frame.columns:
        frame["device_type"] = frame["vehicle_id"].astype(str).str.rsplit("-", n=1).str[0]

    rows_written = write_analytics_parquet_to_glue(frame, config=aws_config)
    task_logger.info(
        "S3 sync complete: %d row(s) → s3://%s/analytics/telemetry/ (Glue: %s.telemetry)",
        rows_written,
        aws_config.bucket,
        aws_config.glue_database,
    )


default_args: dict[str, Any] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hydra_av_telemetry_pipeline",
    description="Hydra AV telemetry: generate → ETL → validate → S3/Glue sync",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hydra", "telemetry", "etl"],
) as dag:
    generate_telemetry = BashOperator(
        task_id="generate_telemetry",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.simulator.generator "
            "--output-dir /opt/airflow/data/telemetry "
            "--duration 30 --rate 10 --failure-rate 0.08"
        ),
        env={"PYTHONPATH": PROJECT_ROOT},
    )

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.transform.run_etl "
            "--input-dir /opt/airflow/data/telemetry "
            "--output-dir /opt/airflow/data/parquet "
            "--dead-letter-dir /opt/airflow/data/dead_letter"
        ),
        env={
            "PYTHONPATH": PROJECT_ROOT,
            "DATA_LAKE_BUCKET": "",
            "GLUE_DATABASE": "",
        },
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    sync_to_s3_task = PythonOperator(
        task_id="sync_to_s3",
        python_callable=sync_to_s3,
    )

    generate_telemetry >> run_etl >> validate_output_task >> sync_to_s3_task

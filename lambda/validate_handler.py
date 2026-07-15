"""Lambda handler — triage, Pandera validation, staging Parquet, and DLQ routing."""

from __future__ import annotations

import json
from typing import Any

import awswrangler as wr
import boto3
import pandas as pd

from config.schema_contract import apply_data_contract_gate
from common import require_env
from src.transform.transformer import TelemetryTransformer


def _process_batch(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Run Pydantic triage and Pandera contract gate on a JSON batch."""
    transformer = TelemetryTransformer(input_dir="/tmp")
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    source_label = "lambda_batch"

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            rejected_rows.append(
                {
                    "record": {"value": record},
                    "rejection_reason": f"record at index {index} is not an object",
                    "corruption_type": "invalid_record_type",
                }
            )
            continue

        rejection_reason, corruption_type = transformer._inspect_record(record)
        if rejection_reason:
            rejected_rows.append(
                {
                    "record": record,
                    "rejection_reason": rejection_reason,
                    "corruption_type": corruption_type,
                }
            )
            continue

        normalized = transformer._normalize_valid_record(record)
        valid_rows.append(normalized)

    if valid_rows:
        frame = pd.DataFrame(valid_rows)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        passing_frame, contract_failures = apply_data_contract_gate(frame, source_label=source_label)

        for failure in contract_failures:
            rejected_rows.append(
                {
                    "record": failure.record,
                    "rejection_reason": failure.rejection_reason,
                    "corruption_type": failure.corruption_type,
                }
            )
    else:
        passing_frame = pd.DataFrame()

    return passing_frame, rejected_rows


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Validate raw batch from S3; write staging Parquet and rejected JSON."""
    raw_key = require_env_key(event, "raw_key")
    execution_id = require_env_key(event, "execution_id")
    raw_bucket = require_env("RAW_BUCKET")
    staging_bucket = require_env("STAGING_BUCKET")

    s3_client = boto3.client("s3")
    payload = json.loads(
        s3_client.get_object(Bucket=raw_bucket, Key=raw_key)["Body"].read().decode("utf-8")
    )
    if not isinstance(payload, list):
        raise ValueError("Raw batch must be a JSON array.")

    total_count = len(payload)
    passing_frame, rejected_rows = _process_batch(payload)
    valid_count = len(passing_frame)
    rejected_count = len(rejected_rows)
    rejection_rate = (rejected_count / total_count) if total_count else 0.0

    staging_key = f"staging/{execution_id}/validated.parquet"
    staging_uri = f"s3://{staging_bucket}/{staging_key}"

    if valid_count > 0:
        wr.s3.to_parquet(df=passing_frame, path=staging_uri, index=False, compression="snappy")
    else:
        empty_frame = pd.DataFrame(
            columns=[
                "timestamp",
                "vehicle_id",
                "trip_id",
                "speed_mph",
                "latitude",
                "longitude",
                "device_type",
            ]
        )
        wr.s3.to_parquet(df=empty_frame, path=staging_uri, index=False, compression="snappy")

    rejected_key = f"dead_letter/{execution_id}/rejected.json"
    serializable_rejects = [
        {
            "rejection_reason": row["rejection_reason"],
            "corruption_type": row.get("corruption_type"),
            "record": row["record"],
        }
        for row in rejected_rows
    ]
    s3_client.put_object(
        Bucket=raw_bucket,
        Key=rejected_key,
        Body=json.dumps(serializable_rejects, default=str),
        ContentType="application/json",
    )

    return {
        "staging_key": staging_key,
        "rejection_rate": rejection_rate,
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "execution_id": execution_id,
        "total_count": total_count,
    }


def require_env_key(event: dict[str, Any], key: str) -> str:
    value = event.get(key)
    if not value:
        raise ValueError(f"Event key '{key}' is required.")
    return str(value)

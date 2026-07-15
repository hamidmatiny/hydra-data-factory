"""Lambda handler — promote staging Parquet to final prefix and sync Glue catalog."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import awswrangler as wr
import boto3
import pandas as pd

from common import require_env
from src.transform.aws_sink import write_analytics_parquet_to_glue
from src.utils.aws_config import AWSDataLakeConfig


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Copy validated Parquet to the final telemetry prefix and register Glue metadata."""
    staging_key = _require_key(event, "staging_key")
    execution_id = _require_key(event, "execution_id")

    staging_bucket = require_env("STAGING_BUCKET")
    validated_bucket = require_env("VALIDATED_BUCKET")
    glue_database = require_env("GLUE_DATABASE")
    glue_table = os.environ.get("GLUE_TABLE", "telemetry")

    os.environ["DATA_LAKE_BUCKET"] = validated_bucket
    os.environ["GLUE_DATABASE"] = glue_database

    staging_uri = f"s3://{staging_bucket}/{staging_key}"
    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    final_key = f"telemetry/dt={partition_date}/{execution_id}.parquet"
    final_uri = f"s3://{validated_bucket}/{final_key}"

    s3_client = boto3.client("s3")
    s3_client.copy_object(
        Bucket=validated_bucket,
        Key=final_key,
        CopySource={"Bucket": staging_bucket, "Key": staging_key},
    )

    frame = wr.s3.read_parquet(path=staging_uri)
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)

    partitions_added: list[str] = [f"dt={partition_date}"]

    if not frame.empty:
        if "device_type" not in frame.columns and "vehicle_id" in frame.columns:
            frame["device_type"] = frame["vehicle_id"].astype(str).str.rsplit("-", n=1).str[0]

        aws_config = AWSDataLakeConfig(bucket=validated_bucket, glue_database=glue_database)
        write_analytics_parquet_to_glue(frame, config=aws_config)

    glue_table_fqn = f"{glue_database}.{glue_table}"

    return {
        "final_key": final_key,
        "final_uri": final_uri,
        "glue_table": glue_table_fqn,
        "partitions_added": partitions_added,
        "execution_id": execution_id,
        "rows_synced": len(frame),
    }


def _require_key(event: dict[str, Any], key: str) -> str:
    value = event.get(key)
    if not value:
        raise ValueError(f"Event key '{key}' is required.")
    return str(value)

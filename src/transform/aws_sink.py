"""AWS S3 and Glue sink operations for the Hydra telemetry ETL pipeline."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import awswrangler as wr
import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.aws_config import (
    ANALYTICS_TELEMETRY_PREFIX,
    AWSDataLakeConfig,
    GLUE_TABLE_NAME,
    RAW_INVALID_PREFIX,
    get_aws_config,
)
from src.utils.logger import get_logger

if TYPE_CHECKING:
    from src.transform.transformer import DeadLetterRecord

logger = get_logger(__name__)


def _s3_client():
    return boto3.client("s3")


def upload_contract_violations_to_s3(
    records: list[DeadLetterRecord],
    *,
    config: AWSDataLakeConfig | None = None,
) -> int:
    """
    Export Pandera contract failures as raw JSON objects to the data lake raw zone.

    Target layout: ``s3://{bucket}/raw/invalid_telemetry/{timestamp}_{id}.json``
    """
    if not records:
        return 0

    aws_config = config or get_aws_config()
    if aws_config is None:
        logger.debug("AWS config absent; skipping S3 invalid telemetry upload.")
        return 0

    contract_records = [
        entry
        for entry in records
        if entry.corruption_type == "data_contract_violation"
    ]
    if not contract_records:
        return 0

    client = _s3_client()
    uploaded = 0

    for entry in contract_records:
        object_key = (
            f"{RAW_INVALID_PREFIX}"
            f"invalid_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')}_{uuid4().hex[:8]}.json"
        )
        payload = {
            "source_file": entry.source_file,
            "rejection_reason": entry.rejection_reason,
            "corruption_type": entry.corruption_type,
            "record": entry.record,
        }

        try:
            client.put_object(
                Bucket=aws_config.bucket,
                Key=object_key,
                Body=json.dumps(payload, default=str),
                ContentType="application/json",
            )
            uploaded += 1
            logger.info(
                "Uploaded contract violation to s3://%s/%s",
                aws_config.bucket,
                object_key,
            )
        except (BotoCoreError, ClientError) as exc:
            logger.error(
                "Failed to upload invalid telemetry to s3://%s/%s: %s",
                aws_config.bucket,
                object_key,
                exc,
            )

    return uploaded


def write_analytics_parquet_to_glue(
    frame: pd.DataFrame,
    *,
    config: AWSDataLakeConfig | None = None,
) -> int:
    """
    Persist validated telemetry to S3 and register/update the Glue table.

    Uses Hive-style partitioning on ``device_type`` under ``analytics/telemetry/``.
    """
    if frame.empty:
        return 0

    aws_config = config or get_aws_config()
    if aws_config is None:
        raise ValueError("AWS data lake configuration is not available.")

    if "device_type" not in frame.columns:
        raise ValueError("DataFrame must include a device_type column for partitioning.")

    output_path = f"s3://{aws_config.bucket}/{ANALYTICS_TELEMETRY_PREFIX}"

    try:
        wr.s3.to_parquet(
            df=frame,
            path=output_path,
            dataset=True,
            database=aws_config.glue_database,
            table=GLUE_TABLE_NAME,
            partition_cols=["device_type"],
            mode="append",
            compression="snappy",
        )
    except (BotoCoreError, ClientError) as exc:
        logger.error("Failed to write analytics Parquet to %s: %s", output_path, exc)
        raise

    logger.info(
        "Wrote %d row(s) to %s and synced Glue table %s.%s",
        len(frame),
        output_path,
        aws_config.glue_database,
        GLUE_TABLE_NAME,
    )
    return len(frame)

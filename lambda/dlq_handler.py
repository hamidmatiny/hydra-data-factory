"""Lambda handler — persist pipeline failure metadata to S3 and SQS."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import boto3

from common import require_env


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Write failure record to S3 dead-letter prefix and notify SQS."""
    execution_id = str(event.get("execution_id", "unknown"))
    error_payload = event.get("error", event)
    raw_bucket = require_env("RAW_BUCKET")
    dlq_queue_url = require_env("DLQ_QUEUE_URL")

    failure_record = {
        "execution_id": execution_id,
        "error": error_payload,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "state_input": event,
    }

    failure_key = f"dead_letter/failures/{execution_id}.json"
    body = json.dumps(failure_record, default=str)

    boto3.client("s3").put_object(
        Bucket=raw_bucket,
        Key=failure_key,
        Body=body,
        ContentType="application/json",
    )

    boto3.client("sqs").send_message(
        QueueUrl=dlq_queue_url,
        MessageBody=body,
    )

    return {
        "dlq_written": True,
        "failure_key": failure_key,
        "execution_id": execution_id,
    }

"""Lambda handler — generate a batch of mock AV telemetry and write to S3 raw zone."""

from __future__ import annotations

import json
from typing import Any

import boto3

from common import require_env, resolve_execution_id
from src.simulator.generator import VehicleTelemetrySimulator


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Generate one telemetry batch and persist to s3://{RAW_BUCKET}/raw/{execution_id}/batch.json."""
    execution_id = resolve_execution_id(event, context)
    batch_size = int(event.get("batch_size", 500))
    raw_bucket = require_env("RAW_BUCKET")

    simulator = VehicleTelemetrySimulator(
        vehicle_ids=["TORC-AV-001", "TORC-AV-002", "TORC-AV-003"],
        failure_rate=0.08,
    )

    batch: list[dict[str, Any]] = []
    for index in range(batch_size):
        vehicle_id = simulator.vehicle_ids[index % len(simulator.vehicle_ids)]
        batch.append(simulator.generate_ping(vehicle_id))

    raw_key = f"raw/{execution_id}/batch.json"
    boto3.client("s3").put_object(
        Bucket=raw_bucket,
        Key=raw_key,
        Body=json.dumps(batch, default=str),
        ContentType="application/json",
    )

    return {
        "raw_key": raw_key,
        "execution_id": execution_id,
        "batch_size": batch_size,
    }

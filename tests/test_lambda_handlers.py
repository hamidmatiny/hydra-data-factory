"""Moto-backed unit tests for Hydra Lambda handlers (Phase 9)."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import boto3
import pandas as pd
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
LAMBDA_DIR = ROOT / "lambda"


def _import_handler(module_name: str):
    """Load a handler module from lambda/ without using the reserved name 'lambda'."""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    if str(LAMBDA_DIR) not in sys.path:
        sys.path.insert(0, str(LAMBDA_DIR))

    module_path = LAMBDA_DIR / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load handler module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def aws_region() -> str:
    return "us-east-1"


@pytest.fixture
def mock_context() -> SimpleNamespace:
    return SimpleNamespace(aws_request_id="test-request-id-12345")


@pytest.fixture
def lakehouse_env(aws_region: str, monkeypatch: pytest.MonkeyPatch):
    """Create mocked S3/Glue/SQS and set handler environment variables."""
    with mock_aws():
        bucket = "hydra-data-lakehouse-test"
        database = "hydra_analytics_db"

        s3 = boto3.client("s3", region_name=aws_region)
        s3.create_bucket(Bucket=bucket)

        glue = boto3.client("glue", region_name=aws_region)
        glue.create_database(DatabaseInput={"Name": database})

        sqs = boto3.client("sqs", region_name=aws_region)
        queue_name = "hydra-data-factory-dlq"
        sqs.create_queue(QueueName=queue_name)
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

        monkeypatch.setenv("AWS_DEFAULT_REGION", aws_region)
        monkeypatch.setenv("RAW_BUCKET", bucket)
        monkeypatch.setenv("STAGING_BUCKET", bucket)
        monkeypatch.setenv("VALIDATED_BUCKET", bucket)
        monkeypatch.setenv("GLUE_DATABASE", database)
        monkeypatch.setenv("GLUE_TABLE", "telemetry")
        monkeypatch.setenv("DATA_LAKE_BUCKET", bucket)
        monkeypatch.setenv("DLQ_QUEUE_URL", queue_url)

        yield {
            "bucket": bucket,
            "database": database,
            "queue_url": queue_url,
            "region": aws_region,
        }


def _valid_record() -> dict:
    return {
        "timestamp": datetime(2026, 5, 31, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "vehicle_id": "TORC-AV-001",
        "trip_id": str(uuid4()),
        "speed_mph": 35.2,
        "gps_coordinates": {"latitude": 40.4406, "longitude": -79.9959},
        "system_logs": {
            "sensor_status": "OK",
            "brake_pressure": "nominal",
            "lidar_temp_c": 41.0,
            "compute_load_pct": 38.5,
        },
        "hardware_version": "HW-v3.2.1",
    }


def test_generate_handler_writes_raw_batch(lakehouse_env, mock_context):
    generate_handler = _import_handler("generate_handler")

    result = generate_handler.lambda_handler(
        {"execution_id": "exec-generate-001", "batch_size": 10},
        mock_context,
    )

    assert result["raw_key"] == "raw/exec-generate-001/batch.json"
    assert result["execution_id"] == "exec-generate-001"

    s3 = boto3.client("s3", region_name=lakehouse_env["region"])
    payload = json.loads(
        s3.get_object(Bucket=lakehouse_env["bucket"], Key=result["raw_key"])["Body"].read()
    )
    assert isinstance(payload, list)
    assert len(payload) == 10
    assert "vehicle_id" in payload[0]


def test_validate_handler_splits_batch_and_computes_rejection_rate(lakehouse_env, mock_context):
    validate_handler = _import_handler("validate_handler")
    execution_id = "exec-validate-001"
    raw_key = f"raw/{execution_id}/batch.json"

    valid_a = _valid_record()
    valid_b = _valid_record()
    valid_b["vehicle_id"] = "TORC-AV-002"

    corrupt = dict(_valid_record())
    corrupt.pop("vehicle_id")

    schema_violation = dict(_valid_record())
    schema_violation["vehicle_id"] = "TORC-AV-003"
    schema_violation["speed_mph"] = 999.0

    batch = [valid_a, corrupt, schema_violation, valid_b]
    s3 = boto3.client("s3", region_name=lakehouse_env["region"])
    s3.put_object(
        Bucket=lakehouse_env["bucket"],
        Key=raw_key,
        Body=json.dumps(batch),
        ContentType="application/json",
    )

    result = validate_handler.lambda_handler(
        {"raw_key": raw_key, "execution_id": execution_id},
        mock_context,
    )

    assert result["staging_key"] == f"staging/{execution_id}/validated.parquet"
    assert result["valid_count"] == 2
    assert result["rejected_count"] == 2
    assert result["rejection_rate"] == pytest.approx(0.5)

    rejected_key = f"dead_letter/{execution_id}/rejected.json"
    rejected_payload = json.loads(
        s3.get_object(Bucket=lakehouse_env["bucket"], Key=rejected_key)["Body"].read()
    )
    assert len(rejected_payload) == 2
    assert all("rejection_reason" in row for row in rejected_payload)

    staging_objects = s3.list_objects_v2(
        Bucket=lakehouse_env["bucket"],
        Prefix=f"staging/{execution_id}/",
    )
    assert staging_objects.get("KeyCount", 0) >= 1


def test_sync_handler_copies_parquet_and_registers_glue_partition(lakehouse_env, mock_context):
    sync_handler = _import_handler("sync_handler")
    validate_handler = _import_handler("validate_handler")

    execution_id = "exec-sync-001"
    raw_key = f"raw/{execution_id}/batch.json"
    batch = [_valid_record(), _valid_record()]
    batch[1]["vehicle_id"] = "TORC-AV-004"

    s3 = boto3.client("s3", region_name=lakehouse_env["region"])
    s3.put_object(
        Bucket=lakehouse_env["bucket"],
        Key=raw_key,
        Body=json.dumps(batch),
        ContentType="application/json",
    )

    validate_result = validate_handler.lambda_handler(
        {"raw_key": raw_key, "execution_id": execution_id},
        mock_context,
    )

    sync_result = sync_handler.lambda_handler(
        {
            "staging_key": validate_result["staging_key"],
            "execution_id": execution_id,
        },
        mock_context,
    )

    assert sync_result["final_key"].startswith("telemetry/dt=")
    assert sync_result["final_key"].endswith(f"{execution_id}.parquet")
    assert sync_result["glue_table"] == f"{lakehouse_env['database']}.telemetry"
    assert sync_result["partitions_added"]

    s3.head_object(Bucket=lakehouse_env["bucket"], Key=sync_result["final_key"])

    glue = boto3.client("glue", region_name=lakehouse_env["region"])
    table = glue.get_table(
        DatabaseName=lakehouse_env["database"],
        Name="telemetry",
    )
    assert table["Table"]["Name"] == "telemetry"

    analytics_objects = s3.list_objects_v2(
        Bucket=lakehouse_env["bucket"],
        Prefix="analytics/telemetry/",
    )
    assert analytics_objects.get("KeyCount", 0) >= 1

    partitions = glue.get_partitions(
        DatabaseName=lakehouse_env["database"],
        TableName="telemetry",
    )
    assert partitions.get("PartitionList") or table["Table"].get("PartitionKeys")


def test_dlq_handler_writes_s3_and_sqs(lakehouse_env, mock_context):
    dlq_handler = _import_handler("dlq_handler")
    execution_id = "exec-dlq-001"
    error_info = {"Error": "States.TaskFailed", "Cause": "Simulated failure"}

    result = dlq_handler.lambda_handler(
        {"execution_id": execution_id, "error": error_info},
        mock_context,
    )

    assert result["dlq_written"] is True

    failure_key = f"dead_letter/failures/{execution_id}.json"
    s3 = boto3.client("s3", region_name=lakehouse_env["region"])
    failure_payload = json.loads(
        s3.get_object(Bucket=lakehouse_env["bucket"], Key=failure_key)["Body"].read()
    )
    assert failure_payload["execution_id"] == execution_id
    assert failure_payload["error"] == error_info
    assert "failed_at" in failure_payload

    sqs = boto3.client("sqs", region_name=lakehouse_env["region"])
    messages = sqs.receive_message(
        QueueUrl=lakehouse_env["queue_url"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    assert "Messages" in messages
    body = json.loads(messages["Messages"][0]["Body"])
    assert body["execution_id"] == execution_id

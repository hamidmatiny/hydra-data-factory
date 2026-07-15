# Phase 9 — Step Functions + Lambda Orchestration

**Project:** hydra-data-factory  
**Account / region:** 903367786893 / us-east-1  
**Status:** Deployed and verified against live AWS infrastructure

---

## Overview

Phase 9 adds a serverless orchestration path for the Hydra AV telemetry pipeline: four container-image Lambda functions, chained by an AWS Step Functions state machine, writing into the same S3 data lakehouse and Glue catalog provisioned in earlier Terraform work. The handlers are thin entrypoints over the existing `src/` ETL code; they do not call MLflow.

This path was built and debugged manually against real AWS resources—not moto mocks or local simulation alone. A successful end-to-end execution was confirmed on 2026-07-14.

---

## What Was Built

Terraform apply completed with **17 resources added** (`Apply complete! Resources: 17 added`). The following infrastructure is live in account **903367786893**, region **us-east-1**:

**Container registry**

- ECR repository `hydra-data-factory-lambda`

**Lambda functions** (all `package_type = Image`, shared image URI with per-function `image_config.command` override, timeout **300 s**)

| Function | Memory |
|----------|--------|
| `hydra-generate-prod` | 1024 MB |
| `hydra-validate-prod` | 2048 MB |
| `hydra-sync-prod` | 1024 MB |
| `hydra-dlq-prod` | 1024 MB |

**Messaging and orchestration**

- SQS queue `hydra-data-factory-dlq` (14-day message retention)
- Step Functions state machine `hydra-data-factory-pipeline` (standard type, full execution logging enabled)

**Observability**

- Five CloudWatch log groups (one per Lambda function plus one for Step Functions), **14-day** retention

**IAM**

- `hydra-lambda-exec-prod` — S3, Glue, SQS send, and CloudWatch Logs scoped to the lakehouse bucket and analytics database
- `hydra-step-functions-exec-prod` — `lambda:InvokeFunction` on the four Hydra functions plus Step Functions logging permissions

**Application code** (repo)

- `lambda/generate_handler.py`, `validate_handler.py`, `sync_handler.py`, `dlq_handler.py` — import and reuse `src/` simulator, triage, Pandera gate, and Glue sync logic
- `lambda/Dockerfile` — based on `public.ecr.aws/lambda/python:3.11`; no fixed `CMD` (Terraform sets the handler per function)
- `terraform/statemachine/hydra_pipeline.asl.json.tpl` — generate → validate → rejection-rate gate (20%) → sync, with catch/retry and DLQ routing on failure

Data lands in the existing lakehouse bucket `hydra-data-lakehouse-prod-903367786893`, using prefixes `raw/`, `staging/`, `telemetry/`, and `dead_letter/` within that single bucket.

---

## Debugging Notes

Getting Phase 9 to a green execution required four separate fixes after the initial Terraform apply. Each failure was visible in CloudWatch or Step Functions execution history, not in unit tests alone.

### 1. Terraform apply — insufficient IAM on the local user

The first `terraform apply` failed with `AccessDeniedException` on `ecr:CreateRepository`, `logs:CreateLogGroup`, and `sqs:CreateQueue`. The local IAM user (`hydra-pipeline-local`) had only the S3/Glue/IAM permissions from Phases 5–6.

**Fix:** Attach `AmazonEC2ContainerRegistryFullAccess`, `CloudWatchLogsFullAccess`, `AmazonSQSFullAccess`, `AWSLambda_FullAccess`, and `AWSStepFunctionsFullAccess` to the user. Apply then succeeded.

### 2. ECR push — unsupported image manifest (BuildKit attestation)

After pushing the first image, Lambda rejected it:

> image manifest, config or layer media type… is not supported

Docker Desktop’s BuildKit attaches provenance/SBOM attestation manifests by default. Lambda’s container runtime does not accept those extra manifest types.

**Fix:** Build with attestation disabled:

```bash
docker build --provenance=false --sbom=false -f lambda/Dockerfile -t hydra-lambda .
```

### 3. Lambda runtime — wrong CPU architecture

Invocations then failed with `Runtime.InvalidEntrypoint` / `ProcessSpawnFailed`. The image had been built natively on Apple Silicon (**arm64**), while the Lambda functions default to **x86_64**.

**Fix:** Cross-build for the Lambda target platform:

```bash
docker build --platform linux/amd64 --provenance=false --sbom=false \
  -f lambda/Dockerfile -t hydra-lambda .
```

Tag, push to ECR, and update the functions to pick up the new digest.

### 4. Read-only filesystem — file-based logging at import time

With a valid amd64 image running, handlers failed immediately:

```
OSError: [Errno 30] Read-only file system: 'logs'
```

`src/utils/logger.py` called `setup_logger()` at import time via `get_logger()`. That path always created a relative `logs/` directory and attached a `FileHandler`. That works under Docker Compose and Airflow (writable project mounts) but not on Lambda, where only `/tmp` is writable and stdout is already captured by CloudWatch.

**Fix:** Detect Lambda via `AWS_LAMBDA_FUNCTION_NAME` and skip the file handler, logging to stdout only. An additional `OSError` guard remains so any other read-only environment degrades gracefully instead of crashing the handler cold start.

This change is in commit `dae9a9f` (`logger setup`).

---

## Verification

**Step Functions execution**

| Field | Value |
|-------|-------|
| Execution ARN | `arn:aws:states:us-east-1:903367786893:execution:hydra-data-factory-pipeline:1596af7d-f3a0-431a-a33f-f6a98676e3f3` |
| Status | `SUCCEEDED` |
| Duration | ~43 seconds (23:14:44 – 23:15:27 UTC-3, 2026-07-14) |
| Path | generate → validate → sync |

**S3 output (confirmed)**

```
s3://hydra-data-lakehouse-prod-903367786893/analytics/telemetry/device_type=TORC-AV/006a9825d0694ebb9d5492843d860cf5.snappy.parquet
```

File size: **20,535 bytes**, timestamp aligned with the execution window.

**Glue catalog (confirmed via `aws glue get-table`)**

The `telemetry` table’s `StorageDescriptor.Location` points at:

```
s3://hydra-data-lakehouse-prod-903367786893/analytics/telemetry/
```

— matching where the Lambda sync step wrote the Parquet object.

---

## Related Documentation

- Handler and Terraform source: `lambda/`, `terraform/ecr.tf`, `terraform/lambda.tf`, `terraform/step_functions.tf`, `terraform/iam_lambda.tf`, `terraform/sqs.tf`
- Unit tests (moto): `tests/test_lambda_handlers.py`
- Airflow orchestration (separate path, includes MLflow): see `docs/development-notes/phase-07-airflow-orchestration.md` and `phase-08-mlflow-experiment-tracking.md`

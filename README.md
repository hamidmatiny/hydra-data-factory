# Hydra Data Factory — Autonomous Vehicle Telemetry Lakehouse

End-to-end data engineering platform for autonomous-vehicle (AV) telemetry: mock fleet ingestion, fault-tolerant ETL, Pandera data contract validation, containerized orchestration, Terraform-managed AWS infrastructure, and Glue-synced Parquet lakehouse storage.

## Phases

| Phase | Status | Description | Documentation |
|-------|--------|-------------|---------------|
| 1 | Complete | Mock telemetry simulator, Pydantic schemas, centralized logging | [PHASE_1_COMPLETION.md](PHASE_1_COMPLETION.md) |
| 2 | Complete | PyArrow ETL, triage layer, Hive-partitioned local Parquet | [PHASE_2_COMPLETION.md](PHASE_2_COMPLETION.md) |
| 3 | Complete | Pandera data contracts, Dockerfile, Docker Compose emulation | [PHASE_3_COMPLETION.md](PHASE_3_COMPLETION.md) |
| 4 | Complete | Pytest validation suite for contract and triage logic | [PHASE_4_COMPLETION.md](PHASE_4_COMPLETION.md) |
| 5 | Complete | Terraform — S3 data lake, Glue catalog, IAM execution role | [PHASE_5_COMPLETION.md](PHASE_5_COMPLETION.md) |
| 6 | Complete | AWS cloud sink — boto3 DLQ routing, Wrangler Parquet + Glue sync | [PHASE_6_COMPLETION.md](PHASE_6_COMPLETION.md) |
| 9 | Complete | Step Functions + Lambda serverless orchestration (AWS-native alternative to Airflow) | See below |

See also [phase_completion.md](phase_completion.md) for the consolidated engineering log and production run metrics.

## What This Repository Solves

| Problem | Hydra Solution |
|---------|----------------|
| No realistic AV telemetry for pipeline development | Kinematic mock simulator with configurable corruption injection |
| Bad data crashing ETL jobs | Two-layer triage (Pydantic + Pandera) with Dead-Letter Queue isolation |
| Schema drift into the lakehouse | Pandera `DataFrameSchema` contract gate before any Parquet write |
| No production cloud footprint | Terraform-provisioned S3 + Glue + least-privilege IAM |
| Manual catalog maintenance | AWS Wrangler auto-syncs Glue table on every analytics write |
| Untested validation logic | Pytest suite covering happy path, range violations, and malformed types |

## What the Pipeline Does

1. Generates or ingests raw JSON telemetry batches from TORC-AV fleet devices
2. Triage-inspects each record for corruption modes (missing fields, malformed GPS, invalid types)
3. Normalizes valid rows into a flat columnar schema with derived `device_type` partition keys
4. Enforces a strict Pandera data contract gate before analytics materialization
5. Isolates rejected records to a local JSON Dead-Letter Queue (`output/dead_letter/dlq_*.json`)
6. Writes passing rows as Snappy Parquet to S3 via AWS Wrangler (when `.env` configured)
7. Auto-registers and updates the AWS Glue Data Catalog table `hydra_analytics_db.telemetry`

## Technology Stack

| Layer | Technologies |
|-------|-------------|
| Language | Python 3.11+ |
| Schema validation | Pydantic v2, Pandera |
| Data processing | Pandas, PyArrow, Apache Parquet |
| AWS integration | boto3, AWS Data Wrangler (`awswrangler`) |
| Infrastructure | Terraform (AWS provider ~> 5.0), Amazon S3, AWS Glue, IAM |
| Containerization | Docker (multi-stage), Docker Compose |
| Orchestration | Apache Airflow 2.9 (CeleryExecutor), AWS Step Functions + Lambda |
| Testing | pytest |
| Configuration | python-dotenv (`.env`) |
| Logging | Python `logging` — stdout + `logs/pipeline.log` |

## Application Architecture

End-to-end data flow from simulated fleet devices to queryable cloud lakehouse:

```mermaid
flowchart TB
    subgraph ingest [Phase 1 — Ingestion]
        Sim[VehicleTelemetrySimulator]
        JSON[telemetry_*.json batches]
        Sim --> JSON
    end

    subgraph transform [Phases 2–3 — Transform & Validate]
        ETL[TelemetryTransformer]
        Triage[Pydantic Triage Layer]
        Gate[Pandera Contract Gate]
        JSON --> ETL
        ETL --> Triage
        Triage --> Gate
    end

    subgraph reject [Rejection Paths]
        DLQ[Local DLQ\noutput/dead_letter/]
        S3Bad[S3 raw/invalid_telemetry/]
        Triage -->|corrupt record| DLQ
        Gate -->|contract violation| S3Bad
    end

    subgraph success [Phase 6 — Analytics Sink]
        WR[AWS Wrangler Parquet Engine]
        S3[s3://hydra-data-lakehouse-prod-.../analytics/telemetry/]
        Glue[AWS Glue\nhydra_analytics_db.telemetry]
        Gate -->|valid rows| WR
        WR --> S3
        WR --> Glue
    end

    subgraph infra [Phase 5 — AWS Infrastructure]
        TF[Terraform]
        TF -.-> S3
        TF -.-> Glue
    end

    subgraph local [Local Fallback]
        LocalPQ[output/parquet/\nyear=/month=/vehicle_id=]
        Gate -->|no .env| LocalPQ
    end
```

Processing sequence:

```
telemetry_*.json
  → Pydantic triage (corruption detection)
  → normalize / flatten GPS + system_logs
  → Pandera contract gate (range, regex, nullability)
  → [AWS enabled] awswrangler.s3.to_parquet + Glue sync
  → [local mode]  PyArrow Hive Parquet write
```

## Project Layout

```
hydra-data-factory/
├── config/
│   └── schema_contract.py         # Pandera DataFrameSchema contract
├── src/
│   ├── simulator/
│   │   ├── generator.py           # Mock AV telemetry stream (Phase 1)
│   │   └── schemas.py             # Pydantic ping schema
│   ├── transform/
│   │   ├── transformer.py         # Core ETL engine
│   │   ├── aws_sink.py            # S3 DLQ + Glue Parquet sink (Phase 6)
│   │   ├── run_etl.py             # Local CLI orchestrator
│   │   └── run_etl_container.py   # Docker polling orchestrator
│   └── utils/
│       ├── logger.py              # Unified logging
│       └── aws_config.py          # .env-driven AWS config (Phase 6)
├── lambda/                        # Step Functions Lambda handlers (Phase 9)
│   ├── generate_handler.py
│   ├── validate_handler.py
│   ├── sync_handler.py
│   ├── dlq_handler.py
│   ├── Dockerfile
│   └── requirements.txt
├── terraform/                     # S3, Glue, IAM (Phase 5) + Lambda/SFN (Phase 9)
│   ├── main.tf
│   ├── ecr.tf
│   ├── lambda.tf
│   ├── sqs.tf
│   ├── iam_lambda.tf
│   ├── step_functions.tf
│   ├── statemachine/hydra_pipeline.asl.json.tpl
│   ├── variables.tf
│   └── outputs.tf
├── tests/                         # Pytest suite (Phase 4 + Phase 9)
│   ├── conftest.py
│   ├── test_schema_contracts.py
│   └── test_lambda_handlers.py
├── dags/                          # Apache Airflow DAGs
│   └── hydra_pipeline_dag.py
├── Dockerfile                     # Multi-stage, non-root appuser
├── docker-compose.yml             # simulator + hydra-etl-processor
├── docker-compose.airflow.yml     # local Airflow CeleryExecutor stack
├── requirements.txt
├── PHASE_1_COMPLETION.md … PHASE_6_COMPLETION.md
├── phase_completion.md            # Consolidated engineering log
└── README.md
```

## Local Setup (without Docker)

```bash
cd hydra-data-factory
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Step 1 — Generate mock telemetry
python -m src.simulator.generator \
  --output-dir output/telemetry \
  --duration 60 \
  --rate 10 \
  --failure-rate 0.08

# Step 2 — Run ETL (local Parquet when .env absent)
python -m src.transform.run_etl \
  --input-dir output/telemetry \
  --output-dir output/parquet \
  --dead-letter-dir output/dead_letter

# Step 3 — Run tests
PYTHONPATH=. pytest tests/test_schema_contracts.py -v
```

## AWS Cloud Sink Setup

Create a `.env` file in the project root:

```
DATA_LAKE_BUCKET=hydra-data-lakehouse-prod-594397057785
GLUE_DATABASE=hydra_analytics_db
AWS_DEFAULT_REGION=us-east-1
```

When both `DATA_LAKE_BUCKET` and `GLUE_DATABASE` are set, `run_etl.py` automatically:

- Uploads Pandera contract violations to `s3://{bucket}/raw/invalid_telemetry/`
- Writes validated Parquet to `s3://{bucket}/analytics/telemetry/` (partitioned by `device_type`)
- Syncs schema metadata to Glue table `telemetry`

```bash
python -m src.transform.run_etl \
  --input-dir output/telemetry \
  --output-dir output/parquet \
  --dead-letter-dir output/dead_letter
```

## Terraform Deployment (Phase 5)

```bash
cd terraform
terraform init
terraform apply
```

Live infrastructure:

| Resource | Value |
|----------|-------|
| S3 bucket | `hydra-data-lakehouse-prod-594397057785` |
| Glue database | `hydra_analytics_db` |
| Raw prefix | `raw/` |
| Analytics prefix | `analytics/telemetry/` |
| Hive partition | `device_type` (e.g., `TORC-AV`) |

Verify:

```bash
aws s3 ls s3://hydra-data-lakehouse-prod-594397057785/analytics/telemetry/ --recursive | head
aws glue get-table --database-name hydra_analytics_db --name telemetry
```

## Docker Deployment

```bash
mkdir -p output/telemetry output/parquet output/dead_letter logs
docker compose up --build
```

| Service | Role |
|---------|------|
| `simulator` | Continuous JSON telemetry generation into `/data/telemetry` |
| `hydra-etl-processor` | Polling ETL with Pandera gate and optional AWS sink |

Host-mounted outputs:

```
output/telemetry/     # Raw JSON batches
output/parquet/       # Local Parquet fallback
output/dead_letter/   # DLQ audit JSON (dlq_*.json)
logs/                 # pipeline.log
```

Stop: `docker compose down`

## Apache Airflow Orchestration

Run the full Hydra pipeline on a daily schedule via a local Airflow CeleryExecutor stack (`apache/airflow:2.9.0`, Postgres metadata DB, Redis broker).

### Start Airflow

Initialize the database and admin user (first time only):

```bash
mkdir -p output/telemetry output/parquet output/dead_letter logs/airflow
docker compose -f docker-compose.airflow.yml up airflow-init
```

Start the Airflow services:

```bash
docker compose -f docker-compose.airflow.yml up
```

Access the UI at [http://localhost:8080](http://localhost:8080) — **user:** `airflow` / **pass:** `airflow`

### DAG: `hydra_av_telemetry_pipeline`

| Task | Operator | Action |
|------|----------|--------|
| `generate_telemetry` | BashOperator | Runs mock AV telemetry simulator (30 s, 10 pps, 8% corruption) |
| `run_etl` | BashOperator | Pydantic triage → Pandera gate → local Parquet + DLQ |
| `validate_output` | PythonOperator | Reports rejection metrics; fails if rejection rate > 20% |
| `sync_to_s3` | PythonOperator | Uploads Parquet to S3 + syncs Glue (skipped when `DATA_LAKE_BUCKET` unset) |

Schedule: `@daily` · Retries: 2 · Retry delay: 5 minutes

Trigger manually from the Airflow UI or unpause the DAG to run on schedule.

## Step Functions + Lambda Orchestration (Phase 9)

Serverless AWS-native orchestration alternative to the Airflow DAG. Four Lambda functions share one container image and are chained by an AWS Step Functions state machine. Any task failure or rejection rate above 20% routes through a DLQ handler that persists failure metadata to S3 and SQS.

```mermaid
flowchart LR
    SFN[Step Functions\nhydra-data-factory-pipeline]
    Gen[hydra_generate]
    Val[hydra_validate]
    Sync[hydra_sync]
    DLQ[hydra_dlq]
    S3[(S3 data lakehouse)]
    Glue[(Glue hydra_analytics_db.telemetry)]
    SQS[(SQS hydra-data-factory-dlq)]

    SFN --> Gen --> Val --> Sync
    Gen --> S3
    Val --> S3
    Sync --> S3
    Sync --> Glue
    SFN -->|failure or high rejection| DLQ
    DLQ --> S3
    DLQ --> SQS
```

| Lambda | Handler | Role |
|--------|---------|------|
| `hydra_generate` | `generate_handler.lambda_handler` | Mock AV telemetry batch → `raw/{execution_id}/batch.json` |
| `hydra_validate` | `validate_handler.lambda_handler` | Pydantic triage + Pandera gate → staging Parquet + rejected JSON |
| `hydra_sync` | `sync_handler.lambda_handler` | Copy to `telemetry/dt={date}/` + Glue catalog sync |
| `hydra_dlq` | `dlq_handler.lambda_handler` | Failure record → S3 `dead_letter/failures/` + SQS |

Both orchestrators coexist intentionally: **Airflow** demonstrates scheduled batch orchestration with local MLflow experiment tracking; **Step Functions + Lambda** demonstrates the serverless AWS-native pattern targeted by Wave HQ ML Engineer II requirements. They are independent — enable only one scheduler in production to avoid duplicate writes.

MLflow tracking is scoped to the Airflow path only. Lambda functions have no network route to the local Docker MLflow server and do not attempt to log runs.

### Build and push the Lambda image

Build from the **repository root** (the Dockerfile copies `src/` and `config/`):

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account_id>.dkr.ecr.us-east-1.amazonaws.com

docker build -f lambda/Dockerfile -t hydra-lambda .
docker tag hydra-lambda:latest <lambda_ecr_repo_url>:latest
docker push <lambda_ecr_repo_url>:latest
```

After pushing the image, apply Terraform (creates ECR repo, Lambdas, Step Functions, SQS, IAM):

```bash
cd terraform
terraform init
terraform apply
```

Optional daily schedule: set `enable_eventbridge_schedule = true` in Terraform variables (defaults to `false` so Airflow and EventBridge do not compete).

### Trigger a manual execution

```bash
aws stepfunctions start-execution \
  --state-machine-arn <step_function_arn> \
  --input '{}'
```

Terraform outputs `lambda_ecr_repo_url`, `step_function_arn`, and `dlq_queue_url` after apply.

## Run Tests

Local Lambda handler tests (moto mocks; Python 3.11 recommended — or run inside the Lambda image):

```bash
pip install -r requirements-test.txt
pytest tests/test_lambda_handlers.py -v
```

If `pyarrow` has no wheel for your local Python version, run tests in the built Lambda container:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace --entrypoint /bin/bash hydra-lambda \
  -c "pip install -q 'moto[s3,glue,sqs]' pytest && PYTHONPATH=/workspace:/workspace/lambda pytest tests/test_lambda_handlers.py -v"
```

Schema contract tests inside the container:

```bash
docker compose build
docker compose run --rm hydra-etl-processor pytest tests/test_schema_contracts.py -v
```

## Data Contract Schema

Fields enforced by Pandera (`config/schema_contract.py`) and registered in AWS Glue:

| Field | Type | Validation |
|-------|------|------------|
| `timestamp` | UTC timestamp | Required, non-null |
| `vehicle_id` | string | Regex `^TORC-AV-\d{3}$` |
| `trip_id` | string | UUID string |
| `speed_mph` | double | Range `0.0`–`120.0` |
| `latitude` | double | WGS-84: `-90.0`–`90.0` |
| `longitude` | double | WGS-84: `-180.0`–`180.0` |

Additional analytics columns: `sensor_status`, `brake_pressure`, `lidar_temp_c`, `compute_load_pct`, `hardware_version`, `device_type`.

## Operational Metrics & Performance

Latest verified production run:

| Metric | Value |
|--------|-------|
| JSON batch files scanned | 8,673 |
| Total records ingested | 42,972 |
| Records passed (analytics) | 39,450 |
| Records rejected (triage) | 3,522 |
| Overall acceptance rate | ~91.80% |
| Post-gate contract rejections | 0% |
| Glue rows synced | 39,450 |
| S3 target | `s3://hydra-data-lakehouse-prod-594397057785/analytics/telemetry/` |
| Glue catalog | `hydra_analytics_db.telemetry` |

## License

Portfolio / educational project.

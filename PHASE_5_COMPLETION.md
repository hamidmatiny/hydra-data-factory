# Phase 5 Completion — Infrastructure as Code (Terraform)

**Project:** hydra-data-factory  
**Phase:** 5 of 6  
**Status:** Complete  
**Commit:** `b54f21a` — *phase5 & 6*

---

## Overview

Phase 5 provisions the AWS cloud footprint required for the Hydra telemetry **data lakehouse** using modular **Terraform** (HCL). Resources include an encrypted S3 bucket, AWS Glue Data Catalog database, and a fine-grained IAM execution role for the containerized ETL processor.

---

## Objective

Stand up production-ready AWS infrastructure that supports:

- Raw telemetry landing and invalid-record quarantine under `raw/`
- Analytics Parquet storage under `analytics/`
- Glue catalog metadata for downstream Athena/Spark queries
- Least-privilege IAM for ECS task-based ETL execution

---

## Files Added (Committed to `origin/main`)

| File | Purpose |
|------|---------|
| `terraform/main.tf` | Provider, S3 bucket, versioning, encryption, public access block, Glue DB, IAM role + policy |
| `terraform/variables.tf` | Region, project name, environment, optional bucket override, Glue database name |
| `terraform/outputs.tf` | Bucket ARN, Glue database name, IAM role ARN, prefix constants |

---

## Resources Provisioned

| Resource | Terraform Name | Live Value |
|----------|----------------|------------|
| S3 data lake bucket | `aws_s3_bucket.data_lakehouse` | `hydra-data-lakehouse-prod-594397057785` |
| Bucket versioning | `aws_s3_bucket_versioning` | Enabled |
| Bucket encryption | `aws_s3_bucket_server_side_encryption_configuration` | AES256 + bucket key |
| Public access block | `aws_s3_bucket_public_access_block` | All blocks enabled |
| Glue database | `aws_glue_catalog_database.hydra_analytics` | `hydra_analytics_db` |
| ETL IAM role | `aws_iam_role.etl_processor` | `hydra-etl-processor-prod` |
| S3 IAM policy | `aws_iam_policy.etl_processor_s3` | Read `raw/*`, write `analytics/*` |

### S3 Prefix Layout

```
s3://hydra-data-lakehouse-prod-594397057785/
├── raw/                          # Raw ingestion zone
│   └── invalid_telemetry/        # Contract violation JSON (Phase 6)
└── analytics/
    └── telemetry/                # Hive-partitioned Parquet (Phase 6)
        └── device_type=TORC-AV/
```

### IAM Policy Scope

| Action | Resource | Condition |
|--------|----------|-----------|
| `s3:ListBucket` | Bucket ARN | Prefix `raw` or `raw/*` |
| `s3:GetObject`, `s3:GetObjectVersion` | `arn:.../raw/*` | — |
| `s3:PutObject`, `s3:AbortMultipartUpload` | `arn:.../analytics/*` | — |

Assume-role principal: `ecs-tasks.amazonaws.com` (ECS/Fargate-ready).

---

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `aws_region` | `us-east-1` | AWS deployment region |
| `project_name` | `hydra` | Resource naming prefix |
| `environment` | `prod` | Environment suffix |
| `bucket_name` | `null` (auto) | Override; otherwise `{project}-data-lakehouse-{env}-{account_id}` |
| `glue_database_name` | `hydra_analytics_db` | Glue catalog database |

---

## Outputs

| Output | Description |
|--------|-------------|
| `s3_bucket_name` | Resolved bucket name |
| `s3_bucket_arn` | Bucket ARN |
| `glue_database_name` | Glue database name |
| `glue_database_arn` | Glue database ARN |
| `etl_processor_role_arn` | IAM role for ETL container |
| `raw_prefix` | `raw/` |
| `analytics_prefix` | `analytics/` |

---

## Deploy

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

Verify:

```bash
terraform output s3_bucket_arn
terraform output glue_database_name

aws s3 ls s3://hydra-data-lakehouse-prod-594397057785/
aws glue get-database --name hydra_analytics_db
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Separate versioning + encryption resources | AWS provider v5 modular bucket configuration |
| Account ID in default bucket name | Global S3 name uniqueness without manual suffix guessing |
| Public access fully blocked | Production security baseline |
| Prefix-scoped IAM | Least privilege — ETL cannot write to arbitrary bucket paths |
| ECS task assume role | Aligns with containerized ETL processor from Phase 3 |

---

## Deliverables Checklist

- [x] `terraform/main.tf` — S3, Glue, IAM
- [x] `terraform/variables.tf` — Configurable region and naming
- [x] `terraform/outputs.tf` — Bucket ARN and Glue database exports
- [x] Live infrastructure verified in AWS account `594397057785`

---

## Next Steps (Phase 6 Preview)

- Python pipeline integration via `boto3` and `awswrangler`
- `.env`-driven cloud sink configuration
- Glue table auto-sync on Parquet writes

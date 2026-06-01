terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  bucket_name = coalesce(
    var.bucket_name,
    "${var.project_name}-data-lakehouse-${var.environment}-${data.aws_caller_identity.current.account_id}",
  )

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Component   = "data-lakehouse"
  }
}

resource "aws_s3_bucket" "data_lakehouse" {
  bucket = local.bucket_name

  tags = merge(local.common_tags, {
    Name = local.bucket_name
  })
}

resource "aws_s3_bucket_versioning" "data_lakehouse" {
  bucket = aws_s3_bucket.data_lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lakehouse" {
  bucket = aws_s3_bucket.data_lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lakehouse" {
  bucket = aws_s3_bucket.data_lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "hydra_analytics" {
  name        = var.glue_database_name
  description = "Relational metadata layer for Hydra Hive-partitioned Parquet telemetry."

  tags = local.common_tags
}

resource "aws_iam_role" "etl_processor" {
  name = "${var.project_name}-etl-processor-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-etl-processor-${var.environment}"
  })
}

resource "aws_iam_policy" "etl_processor_s3" {
  name        = "${var.project_name}-etl-processor-s3-${var.environment}"
  description = "Fine-grained S3 access for Hydra ETL: read raw/, write analytics/."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListRawPrefix"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
        ]
        Resource = aws_s3_bucket.data_lakehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "raw",
              "raw/*",
            ]
          }
        }
      },
      {
        Sid    = "ReadRawObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
        ]
        Resource = "${aws_s3_bucket.data_lakehouse.arn}/raw/*"
      },
      {
        Sid    = "WriteAnalyticsObjects"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload",
        ]
        Resource = "${aws_s3_bucket.data_lakehouse.arn}/analytics/*"
      },
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "etl_processor_s3" {
  role       = aws_iam_role.etl_processor.name
  policy_arn = aws_iam_policy.etl_processor_s3.arn
}

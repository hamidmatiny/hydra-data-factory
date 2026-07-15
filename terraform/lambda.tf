locals {
  lambda_bucket_env = {
    RAW_BUCKET       = aws_s3_bucket.data_lakehouse.bucket
    STAGING_BUCKET   = aws_s3_bucket.data_lakehouse.bucket
    VALIDATED_BUCKET = aws_s3_bucket.data_lakehouse.bucket
    GLUE_DATABASE    = aws_glue_catalog_database.hydra_analytics.name
    GLUE_TABLE       = var.glue_table_name
  }
}

resource "aws_cloudwatch_log_group" "hydra_generate" {
  name              = "/aws/lambda/${var.project_name}-generate-${var.environment}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "hydra_validate" {
  name              = "/aws/lambda/${var.project_name}-validate-${var.environment}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "hydra_sync" {
  name              = "/aws/lambda/${var.project_name}-sync-${var.environment}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "hydra_dlq" {
  name              = "/aws/lambda/${var.project_name}-dlq-${var.environment}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "hydra_generate" {
  function_name = "${var.project_name}-generate-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.hydra_lambda.repository_url}:latest"
  timeout       = 300
  memory_size   = 1024

  image_config {
    command = ["generate_handler.lambda_handler"]
  }

  environment {
    variables = local.lambda_bucket_env
  }

  depends_on = [aws_cloudwatch_log_group.hydra_generate]

  tags = local.common_tags
}

resource "aws_lambda_function" "hydra_validate" {
  function_name = "${var.project_name}-validate-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.hydra_lambda.repository_url}:latest"
  timeout       = 300
  memory_size   = 2048

  image_config {
    command = ["validate_handler.lambda_handler"]
  }

  environment {
    variables = local.lambda_bucket_env
  }

  depends_on = [aws_cloudwatch_log_group.hydra_validate]

  tags = local.common_tags
}

resource "aws_lambda_function" "hydra_sync" {
  function_name = "${var.project_name}-sync-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.hydra_lambda.repository_url}:latest"
  timeout       = 300
  memory_size   = 1024

  image_config {
    command = ["sync_handler.lambda_handler"]
  }

  environment {
    variables = merge(local.lambda_bucket_env, {
      DATA_LAKE_BUCKET = aws_s3_bucket.data_lakehouse.bucket
    })
  }

  depends_on = [aws_cloudwatch_log_group.hydra_sync]

  tags = local.common_tags
}

resource "aws_lambda_function" "hydra_dlq" {
  function_name = "${var.project_name}-dlq-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.hydra_lambda.repository_url}:latest"
  timeout       = 300
  memory_size   = 1024

  image_config {
    command = ["dlq_handler.lambda_handler"]
  }

  environment {
    variables = merge(local.lambda_bucket_env, {
      DLQ_QUEUE_URL = aws_sqs_queue.hydra_dlq.url
    })
  }

  depends_on = [aws_cloudwatch_log_group.hydra_dlq]

  tags = local.common_tags
}

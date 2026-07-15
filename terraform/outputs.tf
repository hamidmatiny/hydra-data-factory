output "s3_bucket_name" {
  description = "Name of the Hydra data lakehouse S3 bucket."
  value       = aws_s3_bucket.data_lakehouse.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the Hydra data lakehouse S3 bucket."
  value       = aws_s3_bucket.data_lakehouse.arn
}

output "glue_database_name" {
  description = "AWS Glue Data Catalog database name."
  value       = aws_glue_catalog_database.hydra_analytics.name
}

output "glue_database_arn" {
  description = "ARN of the AWS Glue Data Catalog database."
  value       = aws_glue_catalog_database.hydra_analytics.arn
}

output "etl_processor_role_arn" {
  description = "IAM execution role ARN for the Hydra ETL processor container."
  value       = aws_iam_role.etl_processor.arn
}

output "etl_processor_role_name" {
  description = "IAM execution role name for the Hydra ETL processor container."
  value       = aws_iam_role.etl_processor.name
}

output "raw_prefix" {
  description = "S3 prefix for raw telemetry JSON ingestion."
  value       = "raw/"
}

output "analytics_prefix" {
  description = "S3 prefix for Hive-partitioned Parquet analytics output."
  value       = "analytics/"
}

output "lambda_ecr_repo_url" {
  description = "ECR repository URL for the Hydra Lambda container image."
  value       = aws_ecr_repository.hydra_lambda.repository_url
}

output "step_function_arn" {
  description = "ARN of the Hydra Step Functions state machine."
  value       = aws_sfn_state_machine.hydra_pipeline.arn
}

output "dlq_queue_url" {
  description = "SQS queue URL for Hydra pipeline failure notifications."
  value       = aws_sqs_queue.hydra_dlq.url
}

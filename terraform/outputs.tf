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

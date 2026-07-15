variable "aws_region" {
  description = "AWS region for Hydra lakehouse resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project prefix used in resource naming."
  type        = string
  default     = "hydra"
}

variable "environment" {
  description = "Deployment environment (e.g., prod, staging)."
  type        = string
  default     = "prod"
}

variable "bucket_name" {
  description = "Optional explicit S3 bucket name. When null, a unique name is derived from project, environment, and account ID."
  type        = string
  default     = null
}

variable "glue_database_name" {
  description = "AWS Glue Data Catalog database name for analytics metadata."
  type        = string
  default     = "hydra_analytics_db"
}

variable "glue_table_name" {
  description = "AWS Glue table name for telemetry analytics."
  type        = string
  default     = "telemetry"
}

variable "enable_eventbridge_schedule" {
  description = "When true, EventBridge triggers the Step Functions pipeline daily at 06:00 UTC."
  type        = bool
  default     = false
}

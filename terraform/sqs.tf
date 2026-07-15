resource "aws_sqs_queue" "hydra_dlq" {
  name                      = "hydra-data-factory-dlq"
  message_retention_seconds = 1209600

  tags = local.common_tags
}

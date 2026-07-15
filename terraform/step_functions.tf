resource "aws_cloudwatch_log_group" "step_functions_logs" {
  name              = "/aws/states/${var.project_name}-pipeline-${var.environment}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_sfn_state_machine" "hydra_pipeline" {
  name     = "hydra-data-factory-pipeline"
  role_arn = aws_iam_role.step_functions_exec.arn

  definition = templatefile("${path.module}/statemachine/hydra_pipeline.asl.json.tpl", {
    generate_arn = aws_lambda_function.hydra_generate.arn
    validate_arn = aws_lambda_function.hydra_validate.arn
    sync_arn     = aws_lambda_function.hydra_sync.arn
    dlq_arn      = aws_lambda_function.hydra_dlq.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_event_rule" "hydra_pipeline_schedule" {
  count = var.enable_eventbridge_schedule ? 1 : 0

  name                = "${var.project_name}-pipeline-daily-${var.environment}"
  description         = "Optional daily trigger for Hydra Step Functions pipeline"
  schedule_expression = "cron(0 6 * * ? *)"

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "hydra_pipeline_schedule" {
  count = var.enable_eventbridge_schedule ? 1 : 0

  rule      = aws_cloudwatch_event_rule.hydra_pipeline_schedule[0].name
  target_id = "HydraPipelineStateMachine"
  arn       = aws_sfn_state_machine.hydra_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_step_functions[0].arn
}

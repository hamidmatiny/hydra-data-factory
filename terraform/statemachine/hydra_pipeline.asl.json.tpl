{
  "Comment": "Hydra AV telemetry pipeline — generate, validate, sync, DLQ on failure",
  "StartAt": "SeedExecutionContext",
  "States": {
    "SeedExecutionContext": {
      "Type": "Pass",
      "Parameters": {
        "execution_id.$": "$$.Execution.Name",
        "batch_size": 500
      },
      "Next": "GenerateTelemetry"
    },
    "GenerateTelemetry": {
      "Type": "Task",
      "Resource": "${generate_arn}",
      "TimeoutSeconds": 300,
      "Parameters": {
        "execution_id.$": "$.execution_id",
        "batch_size.$": "$.batch_size"
      },
      "ResultPath": "$.generate",
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "ValidateAndTriage"
    },
    "ValidateAndTriage": {
      "Type": "Task",
      "Resource": "${validate_arn}",
      "TimeoutSeconds": 300,
      "Parameters": {
        "raw_key.$": "$.generate.raw_key",
        "execution_id.$": "$.generate.execution_id"
      },
      "ResultPath": "$.validate",
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "CheckRejectionRate"
    },
    "CheckRejectionRate": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.validate.rejection_rate",
          "NumericGreaterThan": 0.2,
          "Next": "NotifyFailureRejection"
        }
      ],
      "Default": "SyncToS3AndGlue"
    },
    "SyncToS3AndGlue": {
      "Type": "Task",
      "Resource": "${sync_arn}",
      "TimeoutSeconds": 300,
      "Parameters": {
        "staging_key.$": "$.validate.staging_key",
        "execution_id.$": "$.validate.execution_id"
      },
      "ResultPath": "$.sync",
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "PipelineSucceeded"
    },
    "NotifyFailureRejection": {
      "Type": "Task",
      "Resource": "${dlq_arn}",
      "TimeoutSeconds": 300,
      "Parameters": {
        "execution_id.$": "$.validate.execution_id",
        "error": {
          "Error": "RejectionRateExceeded",
          "Cause": "Rejection rate exceeded 20% threshold",
          "rejection_rate.$": "$.validate.rejection_rate"
        },
        "validate.$": "$.validate"
      },
      "ResultPath": "$.dlq",
      "Next": "RejectionRateTooHigh"
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "${dlq_arn}",
      "TimeoutSeconds": 300,
      "Parameters": {
        "execution_id.$": "$.execution_id",
        "error.$": "$.error",
        "state.$": "$"
      },
      "ResultPath": "$.dlq",
      "Next": "PipelineFailed"
    },
    "PipelineSucceeded": {
      "Type": "Succeed"
    },
    "RejectionRateTooHigh": {
      "Type": "Fail",
      "Error": "RejectionRateExceeded",
      "Cause": "Rejection rate exceeded 20% threshold"
    },
    "PipelineFailed": {
      "Type": "Fail",
      "Error": "PipelineFailed",
      "Cause": "Hydra pipeline execution failed; DLQ notification recorded"
    }
  }
}

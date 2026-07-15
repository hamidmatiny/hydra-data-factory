#!/bin/sh
set -e
: "${DATA_LAKE_BUCKET:?DATA_LAKE_BUCKET must be set}"
exec mlflow server \
  --backend-store-uri sqlite:////mlflow/data/mlflow.db \
  --default-artifact-root "s3://${DATA_LAKE_BUCKET}/mlflow-artifacts/" \
  --host 0.0.0.0 --port 5000

"""AWS data lake configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from dotenv import load_dotenv

load_dotenv()

ENV_DATA_LAKE_BUCKET: Final[str] = "DATA_LAKE_BUCKET"
ENV_GLUE_DATABASE: Final[str] = "GLUE_DATABASE"

RAW_INVALID_PREFIX: Final[str] = "raw/invalid_telemetry/"
ANALYTICS_TELEMETRY_PREFIX: Final[str] = "analytics/telemetry/"
GLUE_TABLE_NAME: Final[str] = "telemetry"


@dataclass(frozen=True)
class AWSDataLakeConfig:
    """Resolved AWS lakehouse targets for the Hydra ETL pipeline."""

    bucket: str
    glue_database: str

    @property
    def invalid_telemetry_uri(self) -> str:
        return f"s3://{self.bucket}/{RAW_INVALID_PREFIX}"

    @property
    def analytics_telemetry_uri(self) -> str:
        return f"s3://{self.bucket}/{ANALYTICS_TELEMETRY_PREFIX}"


def get_aws_config() -> AWSDataLakeConfig | None:
    """Return AWS config when both required environment variables are set."""
    bucket = os.environ.get(ENV_DATA_LAKE_BUCKET, "").strip()
    glue_database = os.environ.get(ENV_GLUE_DATABASE, "").strip()

    if not bucket or not glue_database:
        return None

    return AWSDataLakeConfig(bucket=bucket, glue_database=glue_database)


def aws_enabled() -> bool:
    """True when the pipeline should sink data to AWS instead of local-only paths."""
    return get_aws_config() is not None

"""Pandera data contract and triage validation tests for the Hydra ETL pipeline."""

from __future__ import annotations

import pandas as pd
import pandera as pa
import pytest

from config.schema_contract import TELEMETRY_DATA_CONTRACT, apply_data_contract_gate
from src.transform.transformer import TelemetryTransformer


class TestTelemetryDataContract:
    """Semantic validation rules enforced before Hive Parquet materialization."""

    def test_contract_passes_valid_data(self, valid_telemetry_frame: pd.DataFrame) -> None:
        """Compliant telemetry tables pass the Pandera contract without error."""
        validated = TELEMETRY_DATA_CONTRACT.validate(valid_telemetry_frame, lazy=True)

        assert len(validated) == len(valid_telemetry_frame)
        assert validated["vehicle_id"].tolist() == ["TORC-AV-001", "TORC-AV-002"]
        assert validated["speed_mph"].between(0.0, 120.0).all()

        passing, dlq_records = apply_data_contract_gate(
            valid_telemetry_frame,
            source_label="test_happy_path",
        )
        assert len(passing) == 2
        assert dlq_records == []

    def test_contract_rejects_negative_speed(self, valid_telemetry_frame: pd.DataFrame) -> None:
        """Logical speed violations trigger SchemaErrors with an in_range failure."""
        invalid_frame = valid_telemetry_frame.copy()
        invalid_frame.loc[0, "speed_mph"] = -5.0

        with pytest.raises(pa.errors.SchemaErrors) as exc_info:
            TELEMETRY_DATA_CONTRACT.validate(invalid_frame, lazy=True)

        failure_cases = exc_info.value.failure_cases
        assert "speed_mph" in failure_cases["column"].values
        assert any(
            "in_range" in str(check)
            for check in failure_cases["check"].dropna().tolist()
        )

        passing, dlq_records = apply_data_contract_gate(
            invalid_frame,
            source_label="test_negative_speed",
        )
        assert len(passing) == 1
        assert len(dlq_records) == 1
        assert dlq_records[0].corruption_type == "data_contract_violation"
        assert "speed_mph" in dlq_records[0].rejection_reason


class TestTriageLayer:
    """Pre-contract ingestion triage for simulator corruption modes."""

    def test_triage_handles_malformed_types(self, valid_raw_telemetry_record: dict) -> None:
        """String payloads in numeric fields are rejected before the contract gate."""
        corrupted_record = valid_raw_telemetry_record.copy()
        corrupted_record["speed_mph"] = "NOT_A_NUMBER"

        transformer = TelemetryTransformer(input_dir="output/telemetry")
        rejection_reason, corruption_type = transformer._inspect_record(corrupted_record)

        assert corruption_type == "invalid_speed"
        assert "NOT_A_NUMBER" in rejection_reason

        with pytest.raises(pa.errors.SchemaErrors):
            TELEMETRY_DATA_CONTRACT.validate(
                pd.DataFrame(
                    [
                        {
                            "vehicle_id": corrupted_record["vehicle_id"],
                            "timestamp": pd.Timestamp("2026-05-31T12:00:00", tz="UTC"),
                            "speed_mph": corrupted_record["speed_mph"],
                            "latitude": 40.4406,
                            "longitude": -79.9959,
                        }
                    ]
                ),
                lazy=True,
            )

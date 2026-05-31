"""Shared pytest fixtures for Hydra telemetry contract tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest


@pytest.fixture
def valid_telemetry_frame() -> pd.DataFrame:
    """Compliant flat telemetry DataFrame matching the Pandera contract."""
    return pd.DataFrame(
        [
            {
                "vehicle_id": "TORC-AV-001",
                "timestamp": pd.Timestamp("2026-05-31T12:00:00", tz="UTC"),
                "speed_mph": 42.5,
                "latitude": 40.4406,
                "longitude": -79.9959,
            },
            {
                "vehicle_id": "TORC-AV-002",
                "timestamp": pd.Timestamp("2026-05-31T12:00:01", tz="UTC"),
                "speed_mph": 18.0,
                "latitude": 40.4410,
                "longitude": -79.9940,
            },
        ]
    )


@pytest.fixture
def valid_raw_telemetry_record() -> dict:
    """Compliant nested JSON ping as emitted by the simulator."""
    return {
        "timestamp": datetime(2026, 5, 31, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "vehicle_id": "TORC-AV-004",
        "trip_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "speed_mph": 35.2,
        "gps_coordinates": {"latitude": 40.4406, "longitude": -79.9959},
        "system_logs": {
            "sensor_status": "OK",
            "brake_pressure": "nominal",
            "lidar_temp_c": 41.0,
            "compute_load_pct": 38.5,
        },
        "hardware_version": "HW-v3.2.1",
    }

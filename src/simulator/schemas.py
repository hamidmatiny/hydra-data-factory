"""Pydantic schema definitions for autonomous-vehicle telemetry payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class GpsCoordinates(BaseModel):
    """Geographic position of the vehicle at the time of the ping."""

    model_config = ConfigDict(frozen=True)

    latitude: float = Field(..., ge=-90.0, le=90.0, description="Latitude in decimal degrees.")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Longitude in decimal degrees.")


class SystemLogs(BaseModel):
    """Mock onboard subsystem status indicators."""

    model_config = ConfigDict(frozen=True)

    sensor_status: str = Field(..., description="Aggregate health of perception sensors.")
    brake_pressure: str = Field(..., description="Hydraulic brake system pressure state.")
    lidar_temp_c: float = Field(..., description="Primary LiDAR unit temperature in Celsius.")
    compute_load_pct: float = Field(..., ge=0.0, le=100.0, description="Edge compute utilization.")


class VehicleTelemetry(BaseModel):
    """
    Canonical schema for a single autonomous-vehicle telemetry ping.

    Represents one time-sliced observation emitted by the onboard data
    recorder during an active trip.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )

    timestamp: datetime | int = Field(
        ...,
        description="Observation time as ISO-8601 datetime or Unix epoch seconds.",
    )
    vehicle_id: str = Field(..., min_length=1, description="Fleet identifier, e.g. TORC-AV-004.")
    trip_id: UUID = Field(..., description="UUID of the active driving session.")
    speed_mph: float = Field(..., ge=0.0, le=200.0, description="Ground speed in miles per hour.")
    gps_coordinates: GpsCoordinates
    system_logs: SystemLogs | dict[str, Any] | list[Any]
    hardware_version: str = Field(..., min_length=1, description="Onboard compute stack revision.")

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: datetime | int | str | float) -> datetime | int:
        """Accept ISO strings and coerce them to aware UTC datetimes."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        if isinstance(value, float):
            return int(value)
        return value

    def to_payload(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return self.model_dump(mode="json")

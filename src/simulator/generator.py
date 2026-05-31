"""Mock vehicle telemetry generator with realistic kinematic state evolution."""

from __future__ import annotations

import argparse
import json
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.simulator.schemas import GpsCoordinates, SystemLogs, VehicleTelemetry
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Approximate downtown Pittsburgh — plausible AV test corridor anchor.
_DEFAULT_LATITUDE: float = 40.4406
_DEFAULT_LONGITUDE: float = -79.9959
_METERS_PER_DEGREE_LAT: float = 111_320.0


@dataclass
class VehicleState:
    """Tracks kinematic and session context for a single vehicle."""

    trip_id: UUID = field(default_factory=uuid4)
    latitude: float = _DEFAULT_LATITUDE
    longitude: float = _DEFAULT_LONGITUDE
    speed_mph: float = 0.0
    heading_deg: float = field(default_factory=lambda: random.uniform(0.0, 360.0))
    last_updated: float = field(default_factory=time.time)


class VehicleTelemetrySimulator:
    """
    Produces schema-valid (or intentionally corrupt) telemetry pings.

    Speed and GPS evolve using a simple kinematic model so consecutive pings
    from the same vehicle appear temporally coherent rather than i.i.d. noise.
    """

    _HARDWARE_VERSIONS: tuple[str, ...] = (
        "HW-v3.2.1",
        "HW-v3.3.0",
        "HW-v4.0.0-beta",
    )

    def __init__(
        self,
        vehicle_ids: list[str],
        failure_rate: float = 0.0,
        *,
        seed: int | None = None,
    ) -> None:
        if not vehicle_ids:
            raise ValueError("vehicle_ids must contain at least one identifier.")
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be between 0.0 and 1.0 inclusive.")

        self.vehicle_ids = list(vehicle_ids)
        self.failure_rate = failure_rate
        self._rng = random.Random(seed)
        self._states: dict[str, VehicleState] = {
            vehicle_id: VehicleState(
                latitude=_DEFAULT_LATITUDE + self._rng.uniform(-0.02, 0.02),
                longitude=_DEFAULT_LONGITUDE + self._rng.uniform(-0.02, 0.02),
                speed_mph=self._rng.uniform(0.0, 35.0),
                heading_deg=self._rng.uniform(0.0, 360.0),
            )
            for vehicle_id in vehicle_ids
        }

        logger.info(
            "Simulator initialized for %d vehicle(s) with failure_rate=%.2f",
            len(self.vehicle_ids),
            self.failure_rate,
        )

    def generate_ping(self, vehicle_id: str) -> dict[str, Any]:
        """
        Generate one telemetry payload for ``vehicle_id``.

        Returns a dictionary matching :class:`VehicleTelemetry`. When
        ``failure_rate`` triggers, the payload is deliberately corrupted
        for downstream triage testing.
        """
        if vehicle_id not in self._states:
            raise KeyError(f"Unknown vehicle_id: {vehicle_id!r}")

        state = self._states[vehicle_id]
        now = time.time()
        elapsed = max(now - state.last_updated, 0.001)
        state.last_updated = now

        self._evolve_kinematics(state, elapsed)

        telemetry = VehicleTelemetry(
            timestamp=datetime.fromtimestamp(now, tz=timezone.utc),
            vehicle_id=vehicle_id,
            trip_id=state.trip_id,
            speed_mph=round(state.speed_mph, 2),
            gps_coordinates=GpsCoordinates(
                latitude=round(state.latitude, 7),
                longitude=round(state.longitude, 7),
            ),
            system_logs=self._generate_system_logs(state.speed_mph),
            hardware_version=self._rng.choice(self._HARDWARE_VERSIONS),
        )

        payload = telemetry.to_payload()

        if self._rng.random() < self.failure_rate:
            payload = self._corrupt_payload(payload, vehicle_id)

        return payload

    def stream_to_local_json(
        self,
        output_dir: str,
        duration_seconds: int,
        pings_per_second: int,
    ) -> int:
        """
        Continuously generate pings and persist them in batched JSON files.

        Each batch file is named ``telemetry_{vehicle_id}_{timestamp}.json``.

        Returns the total number of pings written.
        """
        if duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive.")
        if pings_per_second <= 0:
            raise ValueError("pings_per_second must be positive.")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        interval = 1.0 / pings_per_second
        deadline = time.monotonic() + duration_seconds
        total_written = 0
        batch_size = max(pings_per_second, 1)

        logger.info(
            "Starting local stream: dir=%s duration=%ds rate=%d pps",
            output_path.resolve(),
            duration_seconds,
            pings_per_second,
        )

        batch: list[dict[str, Any]] = []
        batch_vehicle_id = self._rng.choice(self.vehicle_ids)

        while time.monotonic() < deadline:
            loop_start = time.monotonic()
            vehicle_id = self._rng.choice(self.vehicle_ids)
            ping = self.generate_ping(vehicle_id)
            batch.append(ping)
            total_written += 1

            if len(batch) >= batch_size:
                self._flush_batch(output_path, batch_vehicle_id, batch)
                batch = []
                batch_vehicle_id = self._rng.choice(self.vehicle_ids)

            elapsed = time.monotonic() - loop_start
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

        if batch:
            self._flush_batch(output_path, batch_vehicle_id, batch)

        logger.info(
            "Stream complete: wrote %d ping(s) to %s",
            total_written,
            output_path.resolve(),
        )
        return total_written

    def _evolve_kinematics(self, state: VehicleState, elapsed_seconds: float) -> None:
        """Apply bounded random walk to speed and derive GPS displacement."""
        target_speed = state.speed_mph + self._rng.uniform(-8.0, 8.0)
        target_speed = max(0.0, min(target_speed, 75.0))
        state.speed_mph += (target_speed - state.speed_mph) * min(elapsed_seconds * 0.5, 1.0)

        if state.speed_mph < 2.0 and self._rng.random() < 0.15:
            state.heading_deg = (state.heading_deg + self._rng.uniform(-45.0, 45.0)) % 360.0

        speed_mps = state.speed_mph * 0.44704
        distance_m = speed_mps * elapsed_seconds
        heading_rad = math.radians(state.heading_deg)

        delta_lat = (distance_m * math.cos(heading_rad)) / _METERS_PER_DEGREE_LAT
        delta_lon = (distance_m * math.sin(heading_rad)) / (
            _METERS_PER_DEGREE_LAT * math.cos(math.radians(state.latitude))
        )

        state.latitude += delta_lat
        state.longitude += delta_lon

    def _generate_system_logs(self, speed_mph: float) -> SystemLogs:
        """Derive subsystem metrics loosely coupled to vehicle speed."""
        compute_load = min(95.0, 25.0 + speed_mph * 0.6 + self._rng.uniform(-5.0, 10.0))
        lidar_temp = 38.0 + speed_mph * 0.08 + self._rng.uniform(-1.5, 1.5)

        sensor_status = "OK"
        if self._rng.random() < 0.03:
            sensor_status = "DEGRADED"
        if speed_mph > 65.0 and self._rng.random() < 0.05:
            sensor_status = "WARN"

        brake_pressure = "nominal"
        if speed_mph > 50.0:
            brake_pressure = "elevated"
        if speed_mph < 1.0:
            brake_pressure = "idle"

        return SystemLogs(
            sensor_status=sensor_status,
            brake_pressure=brake_pressure,
            lidar_temp_c=round(lidar_temp, 2),
            compute_load_pct=round(compute_load, 2),
        )

    def _corrupt_payload(self, payload: dict[str, Any], vehicle_id: str) -> dict[str, Any]:
        """Introduce a random schema violation for pipeline resilience testing."""
        corruption_strategies: tuple[str, ...] = (
            "drop_vehicle_id",
            "invalid_speed",
            "malformed_gps",
            "null_timestamp",
            "truncate_json",
        )
        strategy = self._rng.choice(corruption_strategies)
        corrupted = dict(payload)

        if strategy == "drop_vehicle_id":
            corrupted.pop("vehicle_id", None)
            logger.warning(
                "Simulated corruption [%s]: dropped vehicle_id for %s",
                strategy,
                vehicle_id,
            )
        elif strategy == "invalid_speed":
            corrupted["speed_mph"] = "NOT_A_NUMBER"
            logger.warning(
                "Simulated corruption [%s]: invalid speed_mph for %s",
                strategy,
                vehicle_id,
            )
        elif strategy == "malformed_gps":
            corrupted["gps_coordinates"] = {"lat": corrupted["gps_coordinates"]["latitude"]}
            logger.warning(
                "Simulated corruption [%s]: malformed gps_coordinates for %s",
                strategy,
                vehicle_id,
            )
        elif strategy == "null_timestamp":
            corrupted["timestamp"] = None
            logger.warning(
                "Simulated corruption [%s]: null timestamp for %s",
                strategy,
                vehicle_id,
            )
        elif strategy == "truncate_json":
            corrupted.pop("hardware_version", None)
            corrupted.pop("system_logs", None)
            logger.warning(
                "Simulated corruption [%s]: removed required fields for %s",
                strategy,
                vehicle_id,
            )

        return corrupted

    @staticmethod
    def _flush_batch(
        output_path: Path,
        vehicle_id: str,
        batch: list[dict[str, Any]],
    ) -> None:
        """Write a batch of pings to a timestamped JSON file."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        safe_vehicle_id = vehicle_id.replace("/", "-").replace(" ", "_")
        filename = f"telemetry_{safe_vehicle_id}_{timestamp}.json"
        file_path = output_path / filename

        with file_path.open("w", encoding="utf-8") as handle:
            json.dump(batch, handle, indent=2, default=str)

        logger.info("Wrote batch of %d ping(s) to %s", len(batch), file_path.resolve())


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream mock AV telemetry to local JSON batch files.",
    )
    parser.add_argument(
        "--vehicle-ids",
        nargs="+",
        default=["TORC-AV-001", "TORC-AV-002", "TORC-AV-003"],
        help="Fleet identifiers to simulate.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/telemetry",
        help="Directory for JSON batch files.",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Streaming duration in seconds.",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=5,
        help="Telemetry pings generated per second.",
    )
    parser.add_argument(
        "--failure-rate",
        type=float,
        default=0.05,
        help="Probability [0.0-1.0] of emitting a corrupt ping.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible output.",
    )
    return parser


def main() -> None:
    """CLI entrypoint for local telemetry streaming."""
    args = _build_arg_parser().parse_args()

    simulator = VehicleTelemetrySimulator(
        vehicle_ids=args.vehicle_ids,
        failure_rate=args.failure_rate,
        seed=args.seed,
    )
    simulator.stream_to_local_json(
        output_dir=args.output_dir,
        duration_seconds=args.duration,
        pings_per_second=args.rate,
    )


if __name__ == "__main__":
    main()

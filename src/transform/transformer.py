"""PyArrow-backed telemetry ETL with distributed stream triage and DLQ routing."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pydantic import ValidationError

from src.simulator.schemas import VehicleTelemetry
from src.utils.logger import get_logger

logger = get_logger(__name__)

INPUT_GLOB: Final[str] = "telemetry_*.json"

TELEMETRY_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("vehicle_id", pa.string()),
        ("trip_id", pa.string()),
        ("speed_mph", pa.float64()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("sensor_status", pa.string()),
        ("brake_pressure", pa.string()),
        ("lidar_temp_c", pa.float64()),
        ("compute_load_pct", pa.float64()),
        ("hardware_version", pa.string()),
        ("year", pa.string()),
        ("month", pa.string()),
    ]
)

PARTITION_COLUMNS: Final[tuple[str, ...]] = ("year", "month", "vehicle_id")


@dataclass
class DeadLetterRecord:
    """A rejected telemetry record retained for audit and replay."""

    record: dict[str, Any]
    source_file: str
    rejection_reason: str
    corruption_type: str | None = None


@dataclass
class TransformStats:
    """Operational counters emitted after an ETL run."""

    input_files: int = 0
    input_records: int = 0
    valid_records: int = 0
    rejected_records: int = 0
    parquet_rows_written: int = 0

    @property
    def acceptance_rate(self) -> float:
        if self.input_records == 0:
            return 0.0
        return self.valid_records / self.input_records


@dataclass
class TelemetryTransformer:
    """
    Fault-tolerant ETL engine for raw AV telemetry JSON batches.

    Performs **Distributed Stream Triage** on ingested records, routes
    malformed payloads to a Dead-Letter Queue (DLQ), and materializes clean
    rows as **Columnar Storage Optimization** Parquet under Hive-style
    **Data Lakehouse Partitioning**.
    """

    input_dir: str
    dead_letter_dir: str | None = None
    stats: TransformStats = field(default_factory=TransformStats)
    dead_letter_queue: list[DeadLetterRecord] = field(default_factory=list)
    _valid_rows: list[dict[str, Any]] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        self._input_path = Path(self.input_dir)
        self._dead_letter_path = (
            Path(self.dead_letter_dir) if self.dead_letter_dir else None
        )

    def run(self, output_base_dir: str) -> TransformStats:
        """
        Execute the full ingest → triage → Parquet materialization pipeline.

        Returns operational statistics for logging and observability.
        """
        logger.info("Starting ETL run: input_dir=%s", self._input_path.resolve())

        self.ingest_and_triage()

        if self._valid_rows:
            table = self.build_arrow_table()
            self.write_partitioned_parquet(table, output_base_dir)
        else:
            logger.warning("No valid records found; skipping Parquet write.")

        if self.dead_letter_queue:
            self.persist_dead_letter_queue()

        logger.info(
            "ETL run finished: files=%d records=%d valid=%d rejected=%d parquet_rows=%d",
            self.stats.input_files,
            self.stats.input_records,
            self.stats.valid_records,
            self.stats.rejected_records,
            self.stats.parquet_rows_written,
        )
        return self.stats

    def ingest_and_triage(self) -> None:
        """Scan input JSON batches and partition records into valid vs DLQ paths."""
        if not self._input_path.is_dir():
            raise FileNotFoundError(f"Input directory does not exist: {self._input_path}")

        json_files = sorted(self._input_path.glob(INPUT_GLOB))
        self.stats.input_files = len(json_files)

        if not json_files:
            logger.warning(
                "No telemetry batch files matching %r in %s",
                INPUT_GLOB,
                self._input_path.resolve(),
            )
            return

        logger.info("Discovered %d JSON batch file(s) for ingestion.", len(json_files))

        for file_path in json_files:
            self._process_file(file_path)

    def _process_file(self, file_path: Path) -> None:
        """Load one batch file and triage each embedded telemetry object."""
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse JSON file %s: %s", file_path.name, exc)
            self.dead_letter_queue.append(
                DeadLetterRecord(
                    record={"raw_file": file_path.name},
                    source_file=file_path.name,
                    rejection_reason=f"json_decode_error: {exc}",
                    corruption_type="invalid_json_file",
                )
            )
            self.stats.rejected_records += 1
            return

        records: list[Any]
        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict):
            records = [payload]
        else:
            logger.warning(
                "Unexpected JSON root type in %s: %s",
                file_path.name,
                type(payload).__name__,
            )
            self.dead_letter_queue.append(
                DeadLetterRecord(
                    record={"raw_file": file_path.name, "root_type": type(payload).__name__},
                    source_file=file_path.name,
                    rejection_reason="unexpected_json_root_type",
                    corruption_type="invalid_json_structure",
                )
            )
            self.stats.rejected_records += 1
            return

        for index, record in enumerate(records):
            self.stats.input_records += 1
            if not isinstance(record, dict):
                self._reject_record(
                    record={"value": record},
                    source_file=file_path.name,
                    rejection_reason=f"record at index {index} is not an object",
                    corruption_type="invalid_record_type",
                )
                continue

            rejection_reason, corruption_type = self._inspect_record(record)
            if rejection_reason:
                self._reject_record(
                    record=record,
                    source_file=file_path.name,
                    rejection_reason=rejection_reason,
                    corruption_type=corruption_type,
                )
                continue

            normalized = self._normalize_valid_record(record)
            self._valid_rows.append(normalized)
            self.stats.valid_records += 1

    def _inspect_record(self, record: dict[str, Any]) -> tuple[str, str | None]:
        """
        Perform pre-validation triage aligned with simulator corruption modes.

        Returns ``("", None)`` when the record passes inspection.
        """
        if "vehicle_id" not in record or not record.get("vehicle_id"):
            return "missing or empty vehicle_id", "drop_vehicle_id"

        if record.get("timestamp") is None:
            return "null or missing timestamp", "null_timestamp"

        if "hardware_version" not in record or "system_logs" not in record:
            return "missing required fields (hardware_version and/or system_logs)", "truncate_json"

        gps = record.get("gps_coordinates")
        if not isinstance(gps, dict):
            return "gps_coordinates is not an object", "malformed_gps"

        if "latitude" not in gps or "longitude" not in gps:
            return "gps_coordinates missing latitude/longitude keys", "malformed_gps"

        speed = record.get("speed_mph")
        if isinstance(speed, str) or speed is None:
            return f"invalid speed_mph value: {speed!r}", "invalid_speed"
        if not isinstance(speed, (int, float)):
            return f"invalid speed_mph type: {type(speed).__name__}", "invalid_speed"

        try:
            VehicleTelemetry.model_validate(record)
        except ValidationError as exc:
            first_error = exc.errors()[0]
            field_path = ".".join(str(part) for part in first_error.get("loc", ()))
            message = first_error.get("msg", "validation failed")
            return f"{field_path}: {message}", "schema_validation"

        return "", None

    def _reject_record(
        self,
        *,
        record: dict[str, Any],
        source_file: str,
        rejection_reason: str,
        corruption_type: str | None,
    ) -> None:
        """Route a failed record to the in-memory DLQ and emit a WARNING."""
        logger.warning(
            "DLQ routing [%s] from %s: %s",
            corruption_type or "unknown",
            source_file,
            rejection_reason,
        )
        self.dead_letter_queue.append(
            DeadLetterRecord(
                record=record,
                source_file=source_file,
                rejection_reason=rejection_reason,
                corruption_type=corruption_type,
            )
        )
        self.stats.rejected_records += 1

    @staticmethod
    def _normalize_valid_record(record: dict[str, Any]) -> dict[str, Any]:
        """Flatten nested GPS and system_logs into a columnar-friendly row."""
        validated = VehicleTelemetry.model_validate(record)
        payload = validated.model_dump(mode="python")

        timestamp = payload["timestamp"]
        if isinstance(timestamp, int):
            timestamp_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        else:
            timestamp_dt = timestamp.astimezone(timezone.utc)

        gps = payload["gps_coordinates"]
        if hasattr(gps, "model_dump"):
            gps = gps.model_dump()

        system_logs = payload["system_logs"]
        if hasattr(system_logs, "model_dump"):
            system_logs = system_logs.model_dump()

        if isinstance(system_logs, dict):
            sensor_status = str(system_logs.get("sensor_status", "UNKNOWN"))
            brake_pressure = str(system_logs.get("brake_pressure", "UNKNOWN"))
            lidar_temp_c = float(system_logs.get("lidar_temp_c", 0.0))
            compute_load_pct = float(system_logs.get("compute_load_pct", 0.0))
        else:
            sensor_status = "UNKNOWN"
            brake_pressure = "UNKNOWN"
            lidar_temp_c = 0.0
            compute_load_pct = 0.0

        return {
            "timestamp": timestamp_dt,
            "vehicle_id": payload["vehicle_id"],
            "trip_id": str(payload["trip_id"]),
            "speed_mph": float(payload["speed_mph"]),
            "latitude": float(gps["latitude"]),
            "longitude": float(gps["longitude"]),
            "sensor_status": sensor_status,
            "brake_pressure": brake_pressure,
            "lidar_temp_c": lidar_temp_c,
            "compute_load_pct": compute_load_pct,
            "hardware_version": payload["hardware_version"],
            "year": f"{timestamp_dt.year:04d}",
            "month": f"{timestamp_dt.month:02d}",
        }

    def build_arrow_table(self) -> pa.Table:
        """
        Convert validated rows into a strictly typed PyArrow Table via Pandas.

        GPS coordinates are flattened to root-level ``latitude`` and
        ``longitude`` columns; timestamps use microsecond UTC resolution.
        """
        if not self._valid_rows:
            raise ValueError("Cannot build Arrow table without valid records.")

        frame = pd.DataFrame(self._valid_rows)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

        table = pa.Table.from_pandas(frame, schema=TELEMETRY_ARROW_SCHEMA, preserve_index=False)
        logger.info("Built PyArrow table with %d row(s) and %d column(s).", table.num_rows, table.num_columns)
        return table

    def write_partitioned_parquet(self, table: pa.Table, output_base_dir: str) -> None:
        """
        Persist the table using Hive-style **Data Lakehouse Partitioning**.

        Directory layout::

            output_base_dir/year=YYYY/month=MM/vehicle_id=VAL/*.parquet
        """
        output_path = Path(output_base_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        partitioning = ds.partitioning(
            pa.schema(
                [
                    ("year", pa.string()),
                    ("month", pa.string()),
                    ("vehicle_id", pa.string()),
                ]
            ),
            flavor="hive",
        )

        ds.write_dataset(
            data=table,
            base_dir=str(output_path),
            format="parquet",
            partitioning=partitioning,
            existing_data_behavior="overwrite_or_ignore",
            basename_template="part-{i}.parquet",
        )

        self.stats.parquet_rows_written = table.num_rows
        logger.info(
            "Wrote %d row(s) to Hive-partitioned Parquet under %s",
            table.num_rows,
            output_path.resolve(),
        )

    def persist_dead_letter_queue(self) -> None:
        """Flush in-memory DLQ records to JSON for offline audit."""
        if not self.dead_letter_queue:
            return

        if self._dead_letter_path is None:
            logger.info(
                "Retained %d DLQ record(s) in memory; no dead_letter_dir configured.",
                len(self.dead_letter_queue),
            )
            return

        self._dead_letter_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        dlq_file = self._dead_letter_path / f"dlq_{timestamp}.json"

        serializable = [
            {
                "source_file": entry.source_file,
                "rejection_reason": entry.rejection_reason,
                "corruption_type": entry.corruption_type,
                "record": entry.record,
            }
            for entry in self.dead_letter_queue
        ]

        with dlq_file.open("w", encoding="utf-8") as handle:
            json.dump(serializable, handle, indent=2, default=str)

        logger.info(
            "Persisted %d DLQ record(s) to %s",
            len(self.dead_letter_queue),
            dlq_file.resolve(),
        )

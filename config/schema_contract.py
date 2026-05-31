"""Pandera data contract definitions for telemetry lakehouse governance."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

from src.utils.logger import get_logger

if TYPE_CHECKING:
    from src.transform.transformer import DeadLetterRecord

logger = get_logger(__name__)

VEHICLE_ID_PATTERN: str = r"^TORC-AV-\d{3}$"
SPEED_MIN_MPH: float = 0.0
SPEED_MAX_MPH: float = 120.0
LATITUDE_MIN: float = -90.0
LATITUDE_MAX: float = 90.0
LONGITUDE_MIN: float = -180.0
LONGITUDE_MAX: float = 180.0

TELEMETRY_DATA_CONTRACT: DataFrameSchema = DataFrameSchema(
    {
        "vehicle_id": Column(
            str,
            checks=Check.str_matches(VEHICLE_ID_PATTERN),
            nullable=False,
            description="Fleet identifier matching TORC-AV-NNN pattern.",
        ),
        "timestamp": Column(
            pd.Timestamp,
            nullable=False,
            coerce=True,
            description="UTC observation timestamp; null values are forbidden.",
        ),
        "speed_mph": Column(
            float,
            checks=Check.in_range(SPEED_MIN_MPH, SPEED_MAX_MPH),
            nullable=False,
            description="Ground speed within logical AV operating envelope.",
        ),
        "latitude": Column(
            float,
            checks=Check.in_range(LATITUDE_MIN, LATITUDE_MAX),
            nullable=False,
            description="Decimal latitude within WGS-84 bounds.",
        ),
        "longitude": Column(
            float,
            checks=Check.in_range(LONGITUDE_MIN, LONGITUDE_MAX),
            nullable=False,
            description="Decimal longitude within WGS-84 bounds.",
        ),
    },
    strict=False,
    coerce=True,
    name="telemetry_flat_contract",
)


def apply_data_contract_gate(
    frame: pd.DataFrame,
    *,
    source_label: str = "etl_batch",
) -> tuple[pd.DataFrame, list[DeadLetterRecord]]:
    """
    Enforce **Data Quality Gates** via Pandera before Parquet materialization.

    Rows that fail the contract are returned as DLQ candidates; passing rows
    proceed to the lakehouse write path.
    """
    from src.transform.transformer import DeadLetterRecord

    if frame.empty:
        return frame, []

    working = frame.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True)

    try:
        validated = TELEMETRY_DATA_CONTRACT.validate(working, lazy=True)
        logger.info(
            "Data contract gate passed for %d row(s) [%s].",
            len(validated),
            source_label,
        )
        return validated, []
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases
        failed_indices: set[int] = set()

        if "index" in failure_cases.columns:
            failed_indices = {
                int(value)
                for value in failure_cases["index"].dropna().tolist()
                if pd.notna(value)
            }

        if not failed_indices:
            failed_indices = set(working.index.tolist())

        passing = working.drop(index=list(failed_indices), errors="ignore")
        dlq_records: list[DeadLetterRecord] = []

        for idx in sorted(failed_indices):
            if idx not in working.index:
                continue

            row = working.loc[idx]
            row_failures = failure_cases[failure_cases["index"] == idx]

            if row_failures.empty:
                reason = "data contract validation failed"
            else:
                details = [
                    f"{failure_row.get('column', 'unknown')}: {failure_row.get('check', 'check_failed')}"
                    for _, failure_row in row_failures.iterrows()
                ]
                reason = "; ".join(details)

            logger.warning(
                "Data contract gate rejection [%s] index=%s: %s",
                source_label,
                idx,
                reason,
            )

            dlq_records.append(
                DeadLetterRecord(
                    record=row.to_dict(),
                    source_file=source_label,
                    rejection_reason=f"data_contract_violation: {reason}",
                    corruption_type="data_contract_violation",
                )
            )

        logger.warning(
            "Data contract gate rejected %d row(s); %d row(s) cleared for Parquet write.",
            len(dlq_records),
            len(passing),
        )
        return passing, dlq_records

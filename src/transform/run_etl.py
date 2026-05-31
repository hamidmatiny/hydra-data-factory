"""Local orchestration CLI for the Hydra telemetry ETL pipeline."""

from __future__ import annotations

import argparse
import sys

from src.transform.transformer import TelemetryTransformer, TransformStats
from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_INPUT_DIR = "output/telemetry"
DEFAULT_OUTPUT_DIR = "output/parquet"
DEFAULT_DLQ_DIR = "output/dead_letter"


def _log_operational_summary(
    *,
    stats: TransformStats,
    output_dir: str,
    dead_letter_dir: str | None,
) -> None:
    """Emit a standardized operational summary block."""
    acceptance_pct = stats.acceptance_rate * 100.0
    logger.info("=" * 60)
    logger.info("ETL OPERATIONAL SUMMARY")
    logger.info("=" * 60)
    logger.info("Input JSON batch files scanned : %d", stats.input_files)
    logger.info("Total telemetry records ingested: %d", stats.input_records)
    logger.info("Records compiled to Parquet    : %d", stats.parquet_rows_written)
    logger.info("Records rejected by triage layer  : %d", stats.rejected_records)
    logger.info("Contract gate rejections       : %d", stats.contract_rejected_records)
    logger.info("Acceptance rate                : %.2f%%", acceptance_pct)
    logger.info("Parquet output base directory  : %s", output_dir)
    if dead_letter_dir:
        logger.info("Dead-letter audit directory    : %s", dead_letter_dir)
    logger.info("=" * 60)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Extract, validate, and load raw AV telemetry JSON batches into "
            "Hive-partitioned Parquet with fault-tolerant DLQ triage."
        ),
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing telemetry_*.json batch files (default: {DEFAULT_INPUT_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Base directory for Hive-partitioned Parquet output (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--dead-letter-dir",
        default=DEFAULT_DLQ_DIR,
        help=(
            f"Directory for rejected records audit JSON (default: {DEFAULT_DLQ_DIR}). "
            "Pass an empty string to skip DLQ persistence."
        ),
    )
    return parser


def main() -> int:
    """Run the telemetry ETL pipeline and emit an operational summary."""
    args = _build_arg_parser().parse_args()
    dead_letter_dir = args.dead_letter_dir or None

    transformer = TelemetryTransformer(
        input_dir=args.input_dir,
        dead_letter_dir=dead_letter_dir,
    )

    try:
        stats = transformer.run(output_base_dir=args.output_dir)
    except FileNotFoundError as exc:
        logger.error("ETL aborted: %s", exc)
        return 1
    except Exception as exc:
        logger.exception("ETL failed with unexpected error: %s", exc)
        return 1

    _log_operational_summary(
        stats=stats,
        output_dir=args.output_dir,
        dead_letter_dir=dead_letter_dir,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())

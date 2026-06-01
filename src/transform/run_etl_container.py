"""Container-aware ETL orchestration for local cloud pipeline emulation."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from src.transform.run_etl import _log_operational_summary
from src.transform.transformer import TelemetryTransformer
from src.utils.aws_config import aws_enabled, get_aws_config
from src.utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

ENV_INPUT_DIR = "INPUT_DIR"
ENV_OUTPUT_DIR = "OUTPUT_DIR"
ENV_DLQ_DIR = "DLQ_DIR"
ENV_POLL_INTERVAL = "POLL_INTERVAL_SECONDS"
ENV_RUN_ONCE = "RUN_ONCE"

DEFAULT_INPUT_DIR = "/data/telemetry"
DEFAULT_OUTPUT_DIR = "/data/parquet"
DEFAULT_DLQ_DIR = "/data/dead_letter"
DEFAULT_POLL_INTERVAL_SECONDS = 15
INPUT_GLOB = "telemetry_*.json"


def _env_path(name: str, default: str) -> Path:
    return Path(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return int(raw)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _discover_unprocessed_files(input_dir: Path, processed: set[str]) -> list[Path]:
    candidates = sorted(input_dir.glob(INPUT_GLOB))
    return [path for path in candidates if path.name not in processed]


def _process_new_files(
    *,
    input_dir: Path,
    output_dir: Path,
    dlq_dir: Path | None,
    processed: set[str],
) -> None:
    new_files = _discover_unprocessed_files(input_dir, processed)
    if not new_files:
        logger.debug("No new telemetry batch files detected in %s.", input_dir)
        return

    logger.info("Processing %d new telemetry batch file(s).", len(new_files))

    transformer = TelemetryTransformer(
        input_dir=str(input_dir),
        dead_letter_dir=str(dlq_dir) if dlq_dir else None,
    )
    transformer.ingest_files(new_files)

    if transformer._valid_rows:
        try:
            validated_frame = transformer.get_validated_dataframe()
            transformer.write_analytics_output(validated_frame, str(output_dir))
        except ValueError as exc:
            logger.warning("Analytics write skipped after contract gate: %s", exc)

    if transformer.dead_letter_queue:
        transformer.persist_dead_letter_queue()

    for file_path in new_files:
        processed.add(file_path.name)

    _log_operational_summary(
        stats=transformer.stats,
        output_dir=str(output_dir),
        dead_letter_dir=str(dlq_dir) if dlq_dir else None,
    )


def run_container_pipeline() -> int:
    """
    Poll the shared telemetry volume and run **Containerized ETL Orchestration**.

    Designed for Docker Compose co-scheduling with the simulator service.
    """
    input_dir = _env_path(ENV_INPUT_DIR, DEFAULT_INPUT_DIR)
    output_dir = _env_path(ENV_OUTPUT_DIR, DEFAULT_OUTPUT_DIR)
    dlq_dir = _env_path(ENV_DLQ_DIR, DEFAULT_DLQ_DIR)
    poll_interval = _env_int(ENV_POLL_INTERVAL, DEFAULT_POLL_INTERVAL_SECONDS)
    run_once = _env_bool(ENV_RUN_ONCE, default=False)

    for directory in (input_dir, output_dir, dlq_dir):
        directory.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("HYDRA CONTAINER ETL ORCHESTRATOR STARTUP")
    logger.info("=" * 60)
    logger.info("Input directory           : %s", input_dir.resolve())
    logger.info("Parquet output directory  : %s", output_dir.resolve())
    logger.info("Dead-letter directory     : %s", dlq_dir.resolve())
    logger.info("Poll interval (seconds)   : %d", poll_interval)
    logger.info("Run once mode             : %s", run_once)
    if aws_enabled():
        aws_config = get_aws_config()
        logger.info("AWS data lake bucket      : %s", aws_config.bucket if aws_config else "")
        logger.info("Glue database             : %s", aws_config.glue_database if aws_config else "")
    logger.info("=" * 60)

    processed_files: set[str] = set()

    try:
        while True:
            if not input_dir.is_dir():
                logger.warning(
                    "Input directory %s not yet available; retrying in %ds.",
                    input_dir,
                    poll_interval,
                )
            else:
                _process_new_files(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    dlq_dir=dlq_dir,
                    processed=processed_files,
                )

            if run_once:
                logger.info("RUN_ONCE enabled; exiting container orchestrator.")
                break

            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Container orchestrator interrupted; shutting down.")
        return 0
    except Exception as exc:
        logger.exception("Container orchestrator failed: %s", exc)
        return 1

    return 0


def main() -> int:
    """CLI entrypoint for containerized ETL execution."""
    return run_container_pipeline()


if __name__ == "__main__":
    sys.exit(main())

# Phase 1 Completion — Project Skeleton, Mock Telemetry Simulator, and Schema Definition

**Project:** hydra-data-factory  
**Phase:** 1 of N  
**Status:** Complete  

---

## Overview

Phase 1 establishes the foundational codebase for a production-grade, serverless AV telemetry ingestion and ETL pipeline. This phase delivers a typed project skeleton, a centralized logging utility, a rigid Pydantic telemetry schema, and a kinematic mock data simulator capable of streaming batched JSON files locally—with optional injected corruptions for downstream pipeline triage testing.

---

## Architecture Highlights

### Simulator Component

The simulator lives under `src/simulator/` and is composed of two primary modules:

| Module | Responsibility |
|--------|----------------|
| `schemas.py` | Defines immutable, validated Pydantic models for GPS coordinates, system logs, and the top-level `VehicleTelemetry` ping. |
| `generator.py` | Implements `VehicleTelemetrySimulator`, which maintains per-vehicle kinematic state and emits temporally coherent telemetry. |

**Design decisions:**

- **Stateful kinematic model** — Each vehicle maintains latitude, longitude, speed, heading, and an active `trip_id`. Consecutive pings evolve position using heading-based displacement rather than independent random coordinates, producing realistic trajectories.
- **Controlled failure injection** — The `failure_rate` constructor parameter randomly corrupts payloads (missing fields, invalid types, truncated records) so future ETL stages can exercise dead-letter and quarantine paths.
- **Batch persistence** — `stream_to_local_json()` accumulates pings and flushes them to JSON batch files at a configurable rate, mimicking how edge recorders or Kinesis Firehose would land objects in object storage.
- **Centralized logging** — All simulator activity (initialization, batch writes, corruption events) flows through `src/utils/logger.py`, writing to both stdout and `logs/pipeline.log`.

```
┌─────────────────────────────────────────────────────────────┐
│                  VehicleTelemetrySimulator                  │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐  │
│  │ VehicleState│──▶│ generate_ping│──▶│ VehicleTelemetry │  │
│  │  (per ID)   │   │  + corruption│   │    (Pydantic)    │  │
│  └─────────────┘   └──────────────┘   └────────┬─────────┘  │
│                                                 │           │
│                     stream_to_local_json()      ▼           │
│              ┌──────────────────────────────────────────┐   │
│              │  output/telemetry/telemetry_{id}_{ts}.json │
│              └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    logs/pipeline.log + stdout
```

### Supporting Utilities

| Module | Responsibility |
|--------|----------------|
| `src/utils/logger.py` | Configures the `hydra` root logger with identical `StreamHandler` and `FileHandler` formatters. |
| `terraform/` | Reserved for Phase 2+ AWS infrastructure (currently empty). |

---

## Data Schema Definition

The canonical schema is defined in `src/simulator/schemas.py` as Pydantic `BaseModel` classes. A single telemetry ping (`VehicleTelemetry`) represents one observation from an onboard data recorder.

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `timestamp` | `datetime` (ISO 8601) or `int` (Unix epoch) | Yes | Observation time in UTC. ISO strings with or without `Z` suffix are accepted and normalized. |
| `vehicle_id` | `string` | Yes | Fleet identifier (e.g., `TORC-AV-004`). Minimum length 1. |
| `trip_id` | `UUID` (string in JSON) | Yes | UUID of the active driving session; stable for all pings from the same vehicle until reset. |
| `speed_mph` | `float` | Yes | Ground speed in miles per hour. Validated range: 0.0–200.0. |
| `gps_coordinates` | `object` | Yes | Nested object with `latitude` and `longitude` (see below). |
| `system_logs` | `object` or `dict`/`list` | Yes | Onboard subsystem status. Structured as `SystemLogs` in valid payloads; flexible type allows future array-based log streams. |
| `hardware_version` | `string` | Yes | Onboard compute stack revision (e.g., `HW-v3.2.1`). |

### Nested: `gps_coordinates`

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `latitude` | `float` | −90.0 to 90.0 | Decimal degrees north/south. |
| `longitude` | `float` | −180.0 to 180.0 | Decimal degrees east/west. |

### Nested: `system_logs` (structured form)

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `sensor_status` | `string` | — | Aggregate perception sensor health (`OK`, `DEGRADED`, `WARN`). |
| `brake_pressure` | `string` | — | Brake hydraulic state (`idle`, `nominal`, `elevated`). |
| `lidar_temp_c` | `float` | — | Primary LiDAR temperature in Celsius. |
| `compute_load_pct` | `float` | 0.0–100.0 | Edge compute utilization percentage. |

### Example Valid Payload

```json
{
  "timestamp": "2026-05-31T18:42:01.123456+00:00",
  "vehicle_id": "TORC-AV-001",
  "trip_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "speed_mph": 32.47,
  "gps_coordinates": {
    "latitude": 40.4418234,
    "longitude": -79.9945123
  },
  "system_logs": {
    "sensor_status": "OK",
    "brake_pressure": "nominal",
    "lidar_temp_c": 41.23,
    "compute_load_pct": 44.48
  },
  "hardware_version": "HW-v3.3.0"
}
```

---

## Project Structure

```
hydra-data-factory/
├── .gitignore
├── README.md
├── PHASE_1_COMPLETION.md          ← this document
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── simulator/
│   │   ├── __init__.py
│   │   ├── generator.py           ← VehicleTelemetrySimulator + CLI
│   │   └── schemas.py             ← Pydantic telemetry models
│   └── utils/
│       ├── __init__.py
│       └── logger.py              ← Centralized logging
├── terraform/                     ← empty (future IaC)
├── logs/                          ← created at runtime (gitignored)
└── output/                        ← simulator output (gitignored)
```

---

## How to Run

All commands assume you are at the repository root (`hydra-data-factory/`).

### 1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Run the telemetry simulator (default settings)

```bash
python -m src.simulator.generator
```

This streams for **30 seconds** at **5 pings/second** from three default vehicles into `output/telemetry/`, with a **5% corruption rate**.

### 4. Run with custom parameters

```bash
python -m src.simulator.generator \
  --vehicle-ids TORC-AV-004 TORC-AV-005 \
  --output-dir output/telemetry \
  --duration 60 \
  --rate 10 \
  --failure-rate 0.02 \
  --seed 42
```

| Flag | Default | Description |
|------|---------|-------------|
| `--vehicle-ids` | `TORC-AV-001` … `003` | One or more fleet identifiers |
| `--output-dir` | `output/telemetry` | Directory for JSON batch files |
| `--duration` | `30` | Streaming duration in seconds |
| `--rate` | `5` | Pings generated per second |
| `--failure-rate` | `0.05` | Probability of corrupt payload (0.0–1.0) |
| `--seed` | *(none)* | Optional RNG seed for reproducibility |

### 5. Verify output

```bash
# List generated batch files
ls -la output/telemetry/

# Inspect a sample batch
cat output/telemetry/telemetry_TORC-AV-001_*.json | head -50

# Review pipeline logs
tail -20 logs/pipeline.log
```

### 6. Programmatic usage

```python
from src.simulator.generator import VehicleTelemetrySimulator

simulator = VehicleTelemetrySimulator(
    vehicle_ids=["TORC-AV-004"],
    failure_rate=0.0,
)

ping = simulator.generate_ping("TORC-AV-004")
print(ping)

simulator.stream_to_local_json(
    output_dir="output/telemetry",
    duration_seconds=10,
    pings_per_second=5,
)
```

---

## Dependencies (Phase 1 + Forward-Looking)

| Package | Phase 1 Usage | Future Phases |
|---------|---------------|---------------|
| `pydantic` | Schema validation | API contracts, config models |
| `pandas` | — | DataFrame transforms in ETL |
| `pyarrow` | — | Parquet serialization for S3/Glue |
| `boto3` | — | S3, Lambda, Kinesis, Glue interactions |
| `pytest` | — | Unit and integration tests |
| `python-dotenv` | — | Local environment configuration |

---

## Deliverables Checklist

- [x] Project directory skeleton with `src/`, `terraform/`, and package `__init__.py` files
- [x] Centralized logger (`stdout` + `logs/pipeline.log`) with standardized format
- [x] Pydantic telemetry schema with nested GPS and system log models
- [x] `VehicleTelemetrySimulator` with realistic kinematic state evolution
- [x] `stream_to_local_json()` batch writer with production-grade filenames
- [x] Configurable corruption injection via `failure_rate`
- [x] CLI entrypoint via `python -m src.simulator.generator`
- [x] `requirements.txt` with forward-looking engineering packages
- [x] `.gitignore`, `README.md`, and this completion document

---

## Next Steps (Phase 2 Preview)

- Terraform modules for S3 landing bucket, IAM roles, and event notifications
- Upload simulator output to S3 via `boto3`
- Lambda trigger skeleton for ingestion validation

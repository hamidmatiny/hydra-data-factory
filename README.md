# Hydra Data Factory

Production-grade, serverless **AWS data pipeline** simulating large-scale **autonomous-vehicle** **telemetry-processing** at fleet scale. This repository demonstrates **Reproducible Data Pipelines** through **Containerized ETL Orchestration**, **Data Contracts and Schema Governance** (Pandera), **Data Quality Gates**, **Defensive Data Engineering**, and **Data Lakehouse Partitioning** — with future **Terraform Infrastructure as Code (IaC)** modules for cloud deployment.

## Phase 3 — Current Scope

- **Pandera data contracts** (`config/schema_contract.py`) — **Schema Governance** before Parquet writes
- **Multi-stage Dockerfile** — slim, non-root production container
- **Docker Compose local cloud emulation** — simulator + ETL processor services
- **Container orchestrator** (`run_etl_container.py`) — polling incremental ETL
- PyArrow ETL with DLQ triage (Phase 2) and mock telemetry simulator (Phase 1)

## Containerized Quick Start (Recommended)

```bash
docker compose up --build
```

This launches:

| Service | Role |
|---------|------|
| `simulator` | Continuously generates raw JSON telemetry into `output/telemetry/` |
| `etl-processor` | Polls, validates via **Data Quality Gates**, writes Hive Parquet to `output/parquet/` |

Inspect host-synced artifacts immediately:

```bash
ls output/telemetry/
find output/parquet -name "*.parquet"
cat output/dead_letter/dlq_*.json
tail -f logs/pipeline.log
```

Stop the stack: `docker compose down`

## Local Python Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m src.simulator.generator \
  --output-dir output/telemetry \
  --duration 30 \
  --rate 5 \
  --failure-rate 0.05

python -m src.transform.run_etl \
  --input-dir output/telemetry \
  --output-dir output/parquet \
  --dead-letter-dir output/dead_letter
```

## Data Contracts and Schema Governance

Before any row reaches the Parquet lakehouse, the ETL pipeline enforces a Pandera `DataFrameSchema` contract (`config/schema_contract.py`):

| Field | Rule |
|-------|------|
| `vehicle_id` | Required string matching `^TORC-AV-\d{3}$` |
| `timestamp` | Required UTC datetime, non-null |
| `speed_mph` | Float in `0.0`–`120.0` |
| `latitude` / `longitude` | Valid WGS-84 coordinate bounds |

Contract violations trigger **Defensive Data Engineering** — rows route to the DLQ without crashing the pipeline.

## Documentation

| Phase | Document |
|-------|----------|
| Phase 1 | [PHASE_1_COMPLETION.md](./PHASE_1_COMPLETION.md) — Simulator & schema |
| Phase 2 | [PHASE_2_COMPLETION.md](./PHASE_2_COMPLETION.md) — ETL & Parquet layout |
| Phase 3 | [PHASE_3_COMPLETION.md](./PHASE_3_COMPLETION.md) — Docker & Pandera contracts |

## Project Layout

```
hydra-data-factory/
├── config/
│   └── schema_contract.py   # Pandera data contract definitions
├── src/
│   ├── simulator/           # Mock AV telemetry generator
│   ├── transform/           # PyArrow ETL + container orchestrator
│   └── utils/               # Centralized logging
├── Dockerfile               # Multi-stage production container
├── docker-compose.yml       # Local cloud pipeline emulation
├── terraform/               # Infrastructure as Code (IaC) — future phases
├── logs/                    # Runtime log output (gitignored)
└── output/                  # Telemetry JSON + Parquet + DLQ (gitignored)
```

## Recommended GitHub Topics

`aws-data-pipeline`, `etl-pipeline`, `docker-compose`, `pandera`, `data-contracts`, `pyarrow`, `apache-parquet`, `telemetry-processing`, `autonomous-vehicle`, `data-lakehouse`, `data-quality`, `containerized-etl`, `terraform-infrastructure`, `python-data-engineering`

## License

Portfolio / educational project.

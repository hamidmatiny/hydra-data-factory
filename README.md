# Hydra Data Factory

Production-grade, serverless **AWS data pipeline** simulating large-scale **autonomous-vehicle** **telemetry-processing** at fleet scale. This repository demonstrates end-to-end ingestion, **Distributed Stream Triage**, **Columnar Storage Optimization** with **Apache Parquet**, and **Data Lakehouse Partitioning** — with future **Terraform Infrastructure as Code (IaC)** modules for cloud deployment.

## Phase 2 — Current Scope

- **ETL pipeline** (`src/transform/`) — PyArrow + Pandas transformation layer
- **Fault-tolerant DLQ triage** — isolates corrupted simulator records without crashing
- **Hive-partitioned Parquet writer** — `year=YYYY/month=MM/vehicle_id=VAL/` layout
- Mock telemetry simulator (Phase 1) and centralized logging

## End-to-End Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Step 1 — Generate raw JSON telemetry batches
python -m src.simulator.generator \
  --output-dir output/telemetry \
  --duration 30 \
  --rate 5 \
  --failure-rate 0.05

# Step 2 — Extract, clean, and write Hive-partitioned Parquet
python -m src.transform.run_etl \
  --input-dir output/telemetry \
  --output-dir output/parquet \
  --dead-letter-dir output/dead_letter
```

Raw JSON lands in `output/telemetry/`. Clean **Apache Parquet** files materialize under `output/parquet/`. Rejected records are auditable in `output/dead_letter/`. Logs stream to stdout and `logs/pipeline.log`.

## Documentation

| Phase | Document |
|-------|----------|
| Phase 1 | [PHASE_1_COMPLETION.md](./PHASE_1_COMPLETION.md) — Simulator & schema |
| Phase 2 | [PHASE_2_COMPLETION.md](./PHASE_2_COMPLETION.md) — ETL & Parquet layout |

## Project Layout

```
hydra-data-factory/
├── src/
│   ├── simulator/     # Mock AV telemetry generator (Phase 1)
│   ├── transform/     # PyArrow ETL + Hive Parquet writer (Phase 2)
│   └── utils/         # Centralized logging
├── terraform/         # Infrastructure as Code (IaC) — future phases
├── logs/              # Runtime log output (gitignored)
└── output/            # Simulator JSON + Parquet + DLQ (gitignored)
```

## Recommended GitHub Topics

`aws-data-pipeline`, `etl-pipeline`, `pyarrow`, `apache-parquet`, `telemetry-processing`, `autonomous-vehicle`, `data-lakehouse`, `terraform-infrastructure`, `python-data-engineering`, `serverless-etl`

## License

Portfolio / educational project.

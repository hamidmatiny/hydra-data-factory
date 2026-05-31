# Hydra Data Factory

Production-grade, serverless data ingestion and ETL pipeline simulating large-scale autonomous vehicle (AV) telemetry processing on AWS.

## Phase 1 — Current Scope

- Project skeleton and package layout
- Pydantic telemetry schema definitions
- Mock telemetry simulator with realistic kinematic state evolution
- Centralized logging utility

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m src.simulator.generator \
  --output-dir output/telemetry \
  --duration 30 \
  --rate 5 \
  --failure-rate 0.05
```

Generated batch files appear under `output/telemetry/`. Pipeline logs are written to `logs/pipeline.log` and stdout.

See [PHASE_1_COMPLETION.md](./PHASE_1_COMPLETION.md) for full architecture and schema documentation.

## Project Layout

```
hydra-data-factory/
├── src/
│   ├── simulator/     # Telemetry schema + mock generator
│   └── utils/         # Shared logging utilities
├── terraform/       # Infrastructure (future phases)
├── logs/              # Runtime log output (gitignored)
└── output/            # Simulator JSON batches (gitignored)
```

## License

Portfolio / educational project.

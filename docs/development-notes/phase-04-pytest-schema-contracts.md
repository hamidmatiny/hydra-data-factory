# Phase 4 Completion — Automated Testing Framework

**Project:** hydra-data-factory  
**Phase:** 4 of 6  
**Status:** Complete  
**Commit:** `99abd24` — *phase4*

---

## Overview

Phase 4 introduces a production-grade **pytest** validation suite that locks down the Pandera data contract, triage layer behavior, and schema enforcement rules introduced in Phases 2–3. Tests run locally or inside the `hydra-etl-processor` Docker container against the same code paths used in production ETL runs.

---

## Objective

Validate that:

1. Compliant telemetry DataFrames pass the Pandera contract without error
2. Logical violations (e.g., negative `speed_mph`) trigger `SchemaErrors` with identifiable failure reasons
3. Malformed type injections (e.g., `"NOT_A_NUMBER"`) are caught at the triage layer before the contract gate

---

## Files Added (Committed to `origin/main`)

| File | Purpose |
|------|---------|
| `tests/conftest.py` | Shared pytest fixtures — `valid_telemetry_frame`, `valid_raw_telemetry_record` |
| `tests/test_schema_contracts.py` | Contract and triage test cases |
| `Dockerfile` | Updated to `COPY tests/` into the runtime image |
| `docker-compose.yml` | Service renamed to `hydra-etl-processor` for test execution |

---

## Test Coverage

| Test | Class | Validates |
|------|-------|-----------|
| `test_contract_passes_valid_data` | `TestTelemetryDataContract` | Happy-path Pandera validation + empty DLQ from `apply_data_contract_gate()` |
| `test_contract_rejects_negative_speed` | `TestTelemetryDataContract` | `speed_mph = -5.0` → `SchemaErrors`, `in_range` check, DLQ routing with `data_contract_violation` |
| `test_triage_handles_malformed_types` | `TestTriageLayer` | `"NOT_A_NUMBER"` speed → `invalid_speed` corruption type at `_inspect_record()` |

### Fixtures

**`valid_telemetry_frame`** — Flat Pandas DataFrame with two TORC-AV rows matching `TELEMETRY_DATA_CONTRACT` bounds.

**`valid_raw_telemetry_record`** — Nested JSON ping matching the Phase 1 Pydantic simulator schema (GPS, system_logs, hardware_version).

---

## Dependencies

Uses existing project packages — no new runtime dependencies beyond `pytest` (already in `requirements.txt`):

- `pandera` — schema validation under test
- `pandas` — fixture DataFrames
- `config.schema_contract` — `TELEMETRY_DATA_CONTRACT`, `apply_data_contract_gate()`
- `src.transform.transformer` — `TelemetryTransformer._inspect_record()`

---

## How to Run

Local:

```bash
PYTHONPATH=. pytest tests/test_schema_contracts.py -v
```

Inside Docker:

```bash
docker compose build
docker compose run --rm hydra-etl-processor pytest tests/test_schema_contracts.py -v
```

Expected output: **3 passed**

---

## Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Module import paths in tests | `PYTHONPATH=.` locally; `PYTHONPATH=/app` in Docker |
| Pandera `in_range(inclusive=...)` API change | Removed unsupported `inclusive` kwarg for pandera 0.22 compatibility |
| Testing triage without full ETL run | Direct `_inspect_record()` calls on `TelemetryTransformer` with corrupted dict payloads |

---

## Results

- **3/3** pytest cases passing
- Contract gate and triage layer behavior verified independently of AWS or local Parquet I/O
- Test suite executable inside the production container image

---

## Deliverables Checklist

- [x] `tests/conftest.py` with reusable fixtures
- [x] `tests/test_schema_contracts.py` with three required test cases
- [x] Dockerfile updated to include `tests/` directory
- [x] Docker Compose service aligned for in-container test execution

---

## Next Steps (Phase 5 Preview)

- Terraform provisioning for S3 data lake, Glue catalog, and IAM execution role

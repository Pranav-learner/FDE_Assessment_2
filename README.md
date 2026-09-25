# NYC Taxi Operations Intelligence Pipeline

An end-to-end, trustworthy, production-grade data engineering pipeline that ingests raw NYC Taxi and Limousine Commission (TLC) trip data, executes strict data quality gates, transforms and models operational entities, computes business and data-quality KPIs, and publishes partitioned outputs atomically with complete execution manifest tracking.

---

## 1. Business Problem & Objective

Taxi operations leadership requires an authoritative, repeatable, and explainable understanding of:
- **Trip Activity & Volume**: Total monthly demand patterns across NYC taxi operations.
- **Operational Efficiency**: Trip duration and distance distributions to assess network throughput.
- **Data Quality & Integrity**: Objective measurement of timestamp anomalies and location reference validity.

Raw TLC operational datasets are messy and unmodeled: they contain out-of-period timestamps, negative durations (chronology anomalies), and unmapped location IDs. The objective of this project is to construct a **one-command, idempotent, and resilient pipeline** that guarantees data integrity before publication.

---

## 2. Key Stakeholders

- **Operations Leadership**: Relies on executive headline KPIs (Trip Volume, Median Duration, Average Distance) for monthly performance reviews.
- **Operations & Fleet Planning**: Utilizes location-level dimensional trip models (`fact_trip`, `dim_zone`) to analyze spatial demand and route efficiency.
- **Data Engineering & Quality Assurance**: Governs data contracts, validation gate enforcement, schema stability, and idempotency guarantees.

---

## 3. Business & Data Quality KPIs

| Metric | Target Dimension | Type | July 2026 Result | Presentation Format |
|---|---|---|---|---|
| **Trip Volume** | Operational Activity | Business KPI | `3,530,063` | `3,530,063` trips |
| **Average Trip Duration** | Network Efficiency | Business KPI | `17.29 minutes` | `17.29 min` |
| **Median Trip Duration** | Robust Efficiency | Business KPI | `14.17 minutes` | `14.17 min` |
| **Average Trip Distance** | Trip Breadth | Business KPI | `5.55 miles` | `5.55 mi` |
| **Invalid Trip Duration Rate** | Timestamp Quality | Data Quality KPI | `0.0000283%` (1 / 3,530,063) | `0.0000283%` |
| **Invalid/Unmatched Location Rate** | Reference Integrity | Data Quality KPI | `0.00%` (0 / 3,530,063) | `0.00%` |

---

## 4. Source Data Systems

1. **Trip Activity Records**: NYC TLC Yellow Taxi Monthly Trip Records (Parquet format)
   - Source size: `3,530,109` records
   - Reporting period filter: Pickup timestamp in July 2026 (`2026-07-01 00:00:00` to `2026-08-01 00:00:00`)
   - 46 records belong outside July (preserved in raw, excluded from July reporting model)
2. **Zone Reference Table**: NYC TLC Taxi Zone Lookup (CSV format)
   - `265` unique borough, zone, and service zone mappings

---

## 5. Pipeline Architecture

```text
       CLI: python run_pipeline.py --run-date 2026-07-31
                             │
                             ▼
                    PIPELINE CONFIG
          (Derives reporting period: 2026-07)
                             │
                             ▼
                      RAW INGESTION
          (Trips: 3,530,109 | Zones: 265 rows)
                             │
                             ▼
                     VALIDATION GATES
             (Schema, nulls, keys, FK integrity)
                             │
              ├── Critical Failure ──► STOP (Log ERROR, Exit 1)
              │
              ▼
                   TRANSFORMATION ENGINE
         (Filter to 2026-07: 3,530,063 rows, compute duration,
          flag invalid chronology, validate distances)
                             │
                             ▼
                     RELATIONAL MODEL
          (fact_trip: 3,530,063 rows | dim_zone: 265 rows)
                             │
                             ▼
                      METRICS ENGINE
              (Calculate 4 Business + 2 DQ KPIs)
                             │
                             ▼
                    PRE-PUBLISH SANITY
               (Row counts, schema, null keys)
                             │
              ├── Critical Failure ──► STOP (No publication, Exit 1)
              │
              ▼
                 ATOMIC PUBLISH ENGINE
         (Write to staging -> atomic POSIX replace)
                             │
                             ▼
                   RUN MANIFEST & LOGS
       (Write JSON manifest, output terminal summary, Exit 0)
```

---

## 6. Setup & Installation

The project requires Python 3.10+ (tested on Python 3.12 and 3.14).

```bash
# Clone the repository
git clone https://github.com/your-username/nyc-taxi-fde-pipeline.git
cd nyc-taxi-fde-pipeline

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## 7. Execution: One-Command Runner

Execute the entire end-to-end pipeline with a single command specifying the target run date:

```bash
python run_pipeline.py --run-date 2026-07-31
```

### Run-Date Semantics
- `--run-date YYYY-MM-DD` determines the monthly reporting period.
- For example, `--run-date 2026-07-31` resolves to reporting period **July 2026** (`2026-07-01 00:00:00` to `2026-08-01 00:00:00`).
- It does **not** filter to a single day; it governs the entire calendar month.
- Malformed dates (e.g. `2026-13-01`, `not-a-date`, `2026/07/31`) are strictly rejected with exit code `2`.

---

## 8. Expected Outputs & Artifact Locations

Upon successful completion, artifacts are atomically published to:

| Artifact | File Path | Format | Record Count | Description |
|---|---|---|---:|---|
| **Fact Trips** | `data/processed/fact_trip/fact_trip_2026-07.parquet` | Parquet (Snappy) | 3,530,063 | Modeled trip records with surrogate keys & flags |
| **Dimension Zones** | `data/processed/dim_zone/dim_zone_2026-07.parquet` | Parquet (Snappy) | 265 | Conformed geographic reference dimensions |
| **KPI Metrics** | `data/processed/metrics/metrics_2026-07.csv` | CSV | 6 | All 6 computed KPIs with denominators and descriptions |
| **Run Manifest** | `data/processed/manifest_2026-07.json` | JSON | 1 | Complete run metadata, source/output row counts, fingerprints, durations |
| **Execution Log** | `logs/pipeline.log` | Text Log | Append | Full structured trace of pipeline execution |

---

## 9. Reliability & Production Behaviors

### 1. Atomic Staging & Publication
Outputs are never written directly into production destination paths. Each artifact is written to isolated temporary staging files (`.tmp_*`), checked for completeness, and committed via atomic POSIX directory replacement (`os.replace`). Readers never observe partial, corrupted, or mid-write datasets.

### 2. Strict Idempotency
Rerunning `python run_pipeline.py --run-date 2026-07-31` multiple times produces **identical logical results**:
- Fact trip row count remains exactly `3,530,063` (no duplicates, no double appends).
- Data files are overwritten cleanly via atomic swap.
- SHA-256 logical fingerprints of data columns remain identical across executions.

### 3. Fail-Stop Validation Gates
If incoming raw data suffers critical corruption (e.g. missing required columns, null location IDs, negative trip distances, unmapped foreign keys):
- Execution aborts immediately at the validation stage.
- Processing is halted before transformation, modeling, or publication.
- No partial or corrupted files reach `data/processed/`.
- The CLI terminates with exit code `1` and prints an explicit failure diagnostic.

### 4. Bounded Retries with Exponential Backoff
The pipeline includes a production-grade retry module (`src/retry.py`) with exponential backoff and jitter (`retry_with_backoff`) for transient operational failures (such as rate limits and timeouts). Local file ingestion does not fabricate artificial errors.

### 5. Transparent Anomaly Preservation
Rather than silently dropping the 1 trip record with a negative trip duration (dropoff before pickup), the pipeline preserves the record in `fact_trip` with `is_valid_chronology = False`. Downstream metric engines explicitly exclude invalid chronology trips when calculating duration averages, ensuring full auditability.

---

## 10. Automated Test Suite

The test suite covers unit, integration, validation, retry, idempotency, failure, and CLI runner scenarios:

```bash
pytest tests/ -v
```

**Test Coverage Summary (122 Tests Passed)**:
- `test_config.py`: Configuration immutability and directory creation.
- `test_ingest.py`: Parquet/CSV file-based ingestion and schema contracts.
- `test_validate.py`: Schema validation, null checks, key uniqueness, and reference integrity.
- `test_retry.py`: Exponential backoff, jitter, retry limits, and transient error simulation.
- `test_transform.py`: Timestamp filtering, duration derivation, and anomaly flagging.
- `test_model.py`: Relational surrogate key generation and dimensional integrity.
- `test_metrics.py`: Accurate calculation of 4 business KPIs + 2 data quality KPIs.
- `test_output.py`: Atomic staging, POSIX rename, and JSON run manifest generation.
- `test_idempotency.py`: Rerun stability, row count preservation, and SHA-256 fingerprint invariance.
- `test_controlled_failure.py`: Critical validation halts, fail-stop enforcement, and zero bad outputs.
- `test_pipeline_runner.py`: End-to-end CLI arguments, date parsing, exit codes, and output validation.

---

## 11. Documentation

Detailed architectural and engineering documentation is available in `docs/`:
- `docs/final_pipeline_evidence.md`: Comprehensive final evidence report with complete findings.
- `docs/pipeline_architecture.md`: Architectural blueprints and system flow diagrams.
- `docs/metrics_definition.md`: Mathematical definitions and business logic for all KPIs.
- `docs/idempotency.md`: Mathematical proof of idempotent partition processing.
- `docs/controlled_failure.md`: Verification of fail-stop behavior during critical anomalies.
- `docs/retry_and_failure_handling.md`: Transient failure handling and simulation protocols.
- `docs/source_map.md` & `docs/retrieval_manifest.md`: Source lineage and ingestion verification.
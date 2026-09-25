# Step 8I — Output Persistence and Logging Architecture

This document defines the output persistence, data validation gates, atomic writing mechanisms, execution logging, and manifest metadata architecture for the **NYC Taxi Operations Intelligence Pipeline**.

---

## 1. Pipeline Architecture & Flow

The pipeline executes a guarded, observable workflow ensuring that invalid or corrupt data is never published to processed data directories:

```
    fact_trip ────────┐
                      │
    dim_zone ─────────┼──> OUTPUT VALIDATION
                      │
    metrics ──────────┘
                              │
                              ▼
                       ATOMIC WRITE
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                fact_trip  dim_zone  metrics
                    │         │         │
                    └─────────┼─────────┘
                              ▼
                          MANIFEST
```

---

## 2. Output Artifacts, Formats, and Locations

All output paths are dynamically sourced from [`src/config.py`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/src/config.py) and rooted in `data/processed/`. Raw directories (`data/raw/trips/`, `data/raw/zones/`) remain immutable and untouched.

| Artifact | Logical Table | Format | Target Destination Path | July 2026 Population |
| :--- | :--- | :--- | :--- | :--- |
| **Trip Fact Table** | `fact_trip` | Apache Parquet (Snappy) | `data/processed/fact_trip/fact_trip_2026-07.parquet` | 3,530,063 rows |
| **Zone Dimension** | `dim_zone` | Apache Parquet (Snappy) | `data/processed/dim_zone/dim_zone_2026-07.parquet` | 265 rows |
| **Headline KPIs** | `metrics` | Comma-Separated Values (CSV) | `data/processed/metrics/metrics_2026-07.csv` | 6 headline KPI rows |
| **Run Manifest** | `manifest` | JSON | `data/processed/manifest_2026-07.json` | 1 execution record |

### Schema Specifications

#### `fact_trip` Parquet Schema
- `trip_id` (int64, primary key, non-null, unique)
- `pickup_datetime` (timestamp[us])
- `dropoff_datetime` (timestamp[us])
- `pickup_location_id` (int64, foreign key -> `dim_zone.LocationID`)
- `dropoff_location_id` (int64, foreign key -> `dim_zone.LocationID`)
- `trip_distance` (float64)
- `trip_duration_minutes` (float64)
- `is_valid_duration` (bool)
- `is_valid_location` (bool)
- `is_valid_distance` (bool)
- `is_valid_trip` (bool)

#### `dim_zone` Parquet Schema
- `LocationID` (int64, primary key, 1–265)
- `Borough` (string)
- `Zone` (string)
- `service_zone` (string)

#### `metrics` CSV Schema
- `Metric Name` (string): Human-readable KPI label.
- `Type` (string): Business KPI or Data Quality KPI.
- `Value` (float/int): Exact unrounded numerical value.
- `Display Value` (string): Formatted presentation string with units.
- `Unit` (string): Measurement unit (`trips`, `minutes`, `miles`, `percent`).
- `Numerator` (optional float): Component count or sum.
- `Denominator` (optional float): Component base count.
- `Population` (string): Scope of the calculation.
- `Definition` (string): Formal business logic definition.

---

## 3. Output Sanity Checks (Pre-Save Gate)

Before writing any output to disk, [`validate_outputs()`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/src/output.py) performs rigorous pre-save sanity verification:

1. **Non-Empty Gate**: `fact_trip`, `dim_zone`, and `metrics` must each contain at least 1 record.
2. **Schema Integrity**: All required columns must be present in `fact_trip` (11 columns) and `dim_zone` (4 columns).
3. **Population Integrity**:
   - `fact_trip` must match the expected reporting period row count (3,530,063 for July 2026).
   - `dim_zone` must match 265 rows.
4. **Primary Key Uniqueness**: `fact_trip["trip_id"]` must be strictly unique (0 duplicates allowed).
5. **Foreign Key Integrity**: Every `pickup_location_id` and `dropoff_location_id` in `fact_trip` must resolve to a valid `LocationID` in `dim_zone`.
6. **Headline Metric Completeness**: The metrics output must contain all 6 headline KPIs:
   - `Trip Volume`
   - `Average Trip Duration`
   - `Median Trip Duration`
   - `Average Trip Distance`
   - `Invalid Trip Duration Rate`
   - `Invalid/Unmatched Location Rate`

If any condition fails, an `OutputValidationError` is raised, logged at `ERROR` level, and the save sequence is aborted immediately.

---

## 4. Atomic Writing Approach

Writing directly to the target destination risks leaving partial, truncated, or corrupted files on disk if an I/O failure, disk full condition, or process interruption occurs.

To ensure transactional durability, [`atomic_write_file()`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/src/output.py) employs a temporary-write-and-replace strategy:

1. A hidden temporary file is created within the **same directory** as the destination file (e.g. `.fact_trip_2026-07.parquet.tmp.<uuid>`). Writing to the same filesystem prevents cross-device link failures.
2. Data serialization (Parquet, CSV, or JSON) is completed and flushed into the temporary file.
3. `os.replace(temp_path, target_path)` performs an atomic filesystem inode swap.
4. If an exception occurs at any point during writing:
   - The temporary file is cleaned up via `unlink()`.
   - The final destination file is never touched or modified.
   - An `OutputWriteError` is raised.

---

## 5. Logging Behavior & Levels

Execution observability is implemented in [`src/logger.py`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/src/logger.py) using Python's standard `logging` module.

```
    PIPELINE
       │
       ├── INFO → progress
       ├── WARNING → non-fatal quality issue
       └── ERROR → failed stage
```

### Log Levels
- **`INFO`**: Normal pipeline progression (pipeline startup, record ingestion counts, transformation milestones, output persistence, completion).
- **`WARNING`**: Non-fatal data quality anomalies (e.g. the 1 invalid chronology record flagged in July 2026 TLC data).
- **`ERROR`**: Fatal pipeline failures (validation gate breaches, missing files, schema mismatches, atomic write errors).

### Standard Log Destinations
- Console standard output (`sys.stdout`).
- Persistent execution log: `logs/pipeline.log`.

---

## 6. Execution Manifest

The manifest serves as a persistent, machine-readable proof artifact generated at run conclusion.

Example `data/processed/manifest_2026-07.json`:
```json
{
  "reporting_period": "2026-07",
  "generated_at": "2026-09-25T10:36:00.841350+00:00",
  "fact_trip_path": "/home/pranav/Documents/nyc-taxi-fde-pipeline/data/processed/fact_trip/fact_trip_2026-07.parquet",
  "fact_trip_rows": 3530063,
  "dim_zone_path": "/home/pranav/Documents/nyc-taxi-fde-pipeline/data/processed/dim_zone/dim_zone_2026-07.parquet",
  "dim_zone_rows": 265,
  "metrics_path": "/home/pranav/Documents/nyc-taxi-fde-pipeline/data/processed/metrics/metrics_2026-07.csv",
  "metric_count": 6,
  "pipeline_status": "success"
}
```

---

## 7. Example Successful Run (July 2026)

```text
[2026-09-25 16:05:54] [INFO] [pipeline_runner]: Pipeline started
[2026-09-25 16:05:54] [INFO] [pipeline_runner]: Reporting period: 2026-07
[2026-09-25 16:05:54] [INFO] [pipeline_runner]: Trip input path: data/raw/trips/yellow_tripdata_2026-07.parquet
[2026-09-25 16:05:54] [INFO] [pipeline_runner]: Zone input path: data/raw/zones/taxi_zone_lookup.csv
[2026-09-25 16:05:54] [INFO] [pipeline_runner]: Raw trip rows: 3,530,109
[2026-09-25 16:05:54] [INFO] [pipeline_runner]: Raw zone rows: 265
[2026-09-25 16:05:57] [INFO] [pipeline_runner]: Transformed reporting-period trip rows: 3,530,063
[2026-09-25 16:05:57] [INFO] [pipeline_runner]: dim_zone rows: 265
[2026-09-25 16:05:58] [INFO] [pipeline_runner]: fact_trip rows: 3,530,063
[2026-09-25 16:05:58] [INFO] [pipeline_runner]: Metrics generated: 6
[2026-09-25 16:05:58] [INFO] [pipeline_runner]: Output validation passed (fact_trip: 3,530,063 rows, dim_zone: 265 rows, metrics: 6 KPIs).
[2026-09-25 16:06:00] [INFO] [pipeline_runner]: Saved fact_trip (3,530,063 rows) -> data/processed/fact_trip/fact_trip_2026-07.parquet
[2026-09-25 16:06:00] [INFO] [pipeline_runner]: Saved dim_zone (265 rows) -> data/processed/dim_zone/dim_zone_2026-07.parquet
[2026-09-25 16:06:00] [INFO] [pipeline_runner]: Saved metrics (6 KPIs) -> data/processed/metrics/metrics_2026-07.csv
[2026-09-25 16:06:00] [INFO] [pipeline_runner]: Saved manifest -> data/processed/manifest_2026-07.json
[2026-09-25 16:06:00] [INFO] [pipeline_runner]: Pipeline outputs successfully published.
```

---

## 8. Failure Behavior & Guardrails

- **Pre-Save Failure**: If a validation gate fails (e.g. duplicate `trip_id` or missing headline metric), an `OutputValidationError` is raised, logged at `ERROR`, and zero files are written.
- **Write Failure**: If an I/O error occurs mid-write, the temporary `.tmp` file is deleted immediately, leaving no corrupt partial artifacts.
- **Immutability of Raw Data**: No pipeline stage modifies `data/raw/`.

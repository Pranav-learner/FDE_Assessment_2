# Step 8K — Controlled Failure Demonstration & Safety Architecture

This document formalizes the controlled failure behavior, validation failure classification, error containment, output protection guarantees, and the critical distinction between transient retryable faults and permanent validation failures for the **NYC Taxi Operations Intelligence Pipeline**.

---

## 1. Controlled Failure Flow

When an ingested input dataset violates critical structural, schema, or referential integrity contracts, the pipeline halts immediately at the validation gate prior to downstream transformation, modeling, or output publication:

```
    Missing required column
             │
             ▼
         Validation
             │
             X
             │
             ▼
          FAIL
             │
       ┌─────┴─────┐
       ▼           ▼
    ERROR LOG   NO OUTPUT
                   │
                   ▼
             OLD VALID OUTPUT
                PRESERVED
```

---

## 2. Failure Scenario: Missing Required Column (`trip_distance`)

### Description
In this controlled demonstration, a copy of the input dataset is modified to omit the mandatory column `trip_distance`:
- Expected schema contract requires: `("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "trip_distance")`.
- Observed input schema: `trip_distance` is completely absent.

### Why This Failure Is Critical
- Downstream transformation logic calculates `is_valid_distance` (`trip_distance >= 0`).
- Downstream modeling derives business metrics including `Average Trip Distance`.
- Missing required columns indicate an upstream data contract breach, schema change, or truncated ingestion file that invalidates all downstream analytical models.

---

## 3. Why This Failure Is Non-Retryable (8F vs 8K Distinction)

A core data engineering reliability principle is distinguishing between **transient operational faults** and **permanent structural failures**:

| Dimension | Transient Failure (Step 8F) | Permanent Validation Failure (Step 8K) |
| :--- | :--- | :--- |
| **Examples** | HTTP 500, HTTP 429 rate limit, network timeout, connection reset | Missing required column (`trip_distance`), schema mismatch, invalid authentication, negative trip distance |
| **Cause** | Ephemeral infrastructure or remote server state | Upstream data corruption, contract breach, bad input format |
| **Behavior** | **Bounded Retry with Exponential Backoff** | **Immediate Halt (Zero Retries)** |
| **Pipeline Action** | Retries up to `max_attempts` (default 3); recovers if server stabilizes | Raises `ValidationError` immediately; logs `ERROR`; halts execution |
| **Output Protection** | Writes only upon eventual retrieval success | Publishes **no** output; prevents partial or corrupt artifacts |

Retrying a schema error or missing column is futile; repeating the same query against a broken file will yield identical failure while wasting compute and masking structural defects.

---

## 4. Expected vs. Actual Observed Pipeline Behavior

| Pipeline Stage | Expected Behavior | Actual Observed Result |
| :--- | :--- | :--- |
| **Ingestion** | Ingests the raw input table as-is without filtering | Loaded 2 raw trips missing `trip_distance` |
| **Validation Gate** | Detects missing `trip_distance` column via Gate 1 | Gate failed; added to `result.errors` |
| **Halt Point** | Raises `ValidationError` and halts pipeline | Raised `ValidationError` immediately |
| **Transformation** | **Must NOT run** | Verified `transform_trip_data` was **not called** (mock assertion passed) |
| **Modeling** | **Must NOT run** | Verified `build_fact_trip` and `build_dim_zone` **not called** |
| **Metrics Calculation** | **Must NOT run** | Verified `calculate_metrics` **not called** |
| **Output Publication** | **Must NOT publish** | Verified zero files written to output directories |
| **Manifest** | **Must NOT publish success manifest** | Verified no manifest created |
| **Error Logging** | Emits `ERROR`-level log entry with details | Recorded `[ERROR] [nyc_taxi_pipeline]: Pipeline failed during validation: ... trip_distance` |
| **Existing Output Protection**| Previous valid outputs remain untouched | Verified previous `fact_trip_2026-07.parquet` unchanged |

---

## 5. Output Protection & Atomic Guarantees

1. **Clean Directory**: When executed against an empty target directory, a failed run creates no `fact_trip`, `dim_zone`, `metrics`, or `manifest` files.
2. **Existing Outputs Protected**: When valid outputs already exist from a previous run:
   - A subsequent failed run aborts during validation **before** any output writing begins.
   - The original `fact_trip_2026-07.parquet` file remains bitwise identical (`st_mtime_ns` and SHA-256 fingerprint unchanged).
   - Downstream reporting consumers are shielded from partial tables or broken schemas.
3. **No Leftover Temporary Files**: Atomic staging paths (`.tmp.<uuid>`) are cleaned up immediately if any error is encountered.

---

## 6. Verification Test Summary

The test suite in [`tests/test_controlled_failure.py`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/tests/test_controlled_failure.py) validates all 11 failure scenarios:

1. `test_missing_required_column_fails_pipeline`: Execution raises `ValidationError`.
2. `test_validation_error_message_identifies_missing_column`: Error explicitly names `trip_distance`.
3. `test_transformation_does_not_execute_on_validation_failure`: `transform_trip_data` spy call count is 0.
4. `test_model_does_not_execute_on_validation_failure`: `build_fact_trip` and `build_dim_zone` call counts are 0.
5. `test_metrics_do_not_execute_on_validation_failure`: `calculate_metrics` call count is 0.
6. `test_no_processed_output_published_on_failure`: Target processed directories remain empty.
7. `test_previous_valid_output_preserved_after_controlled_failure`: Existing 3.53M row table is preserved.
8. `test_no_success_manifest_published_on_failure`: No manifest written with `pipeline_status: success`.
9. `test_error_log_recorded_on_validation_failure`: Log file contains `[ERROR]` and `trip_distance`.
10. `test_critical_validation_failure_is_not_retried`: `is_retryable_error` returns `False`; call count is 1.
11. `test_negative_trip_distance_halts_pipeline`: Critical data quality breach stops pipeline before transformation.

# Retry & Failure Handling Architecture

## 1. Overview & Core Reliability Principles

Production data pipelines must handle two distinct failure modes with fundamentally different strategies:
1. **Transient Operational Faults**: Ephemeral network blips, HTTP rate limits, and server timeouts that resolve when retried.
2. **Permanent Structural Failures**: Missing columns, malformed schemas, broken foreign keys, and bad CLI arguments where retrying is futile and wastes resources.

```text
                                INCOMING FAULT
                                      │
                   Is fault transient or permanent?
                                      │
                 ┌────────────────────┴────────────────────┐
                 ▼                                         ▼
         TRANSIENT FAULT                           PERMANENT FAILURE
     (HTTP 429/500, Timeout)                   (Missing column, bad date)
                 │                                         │
                 ▼                                         ▼
         Bounded Retry with                         Fail-Stop Gate
     Exponential Backoff & Jitter                          │
                 │                                         ├─ Log ERROR trace
                 ├── Exhausted (3 attempts)                ├─ Abort immediately
                 │           │                             ├─ Do NOT transform/model
                 ▼           ▼                             ├─ Do NOT publish output
              Success      Halt                            └─ Preserve existing files
```

---

## 2. Failure Classification Matrix

| Category | Examples | Root Cause | Policy | Action |
|---|---|---|---|---|
| **Transient Fault** | HTTP 500/502/503, HTTP 429 Rate Limit, Socket Timeout | Remote infrastructure / congestion | **Bounded Retry with Backoff** | Retry up to 3 attempts with exponential delay; recover if transient state clears. |
| **Authentication Failure** | HTTP 401 Unauthorized, HTTP 403 Forbidden | Expired token, invalid credentials | **Immediate Fail-Stop** | Never retry; raise `AuthenticationError` immediately. |
| **Schema Contract Breach** | Missing `trip_distance` column, unexpected data type | Upstream schema drift or broken source | **Immediate Fail-Stop** | Halt before transformation; exit code 1; publish 0 output files. |
| **Data Quality Gate Breach** | Negative `trip_distance`, null location IDs | Ingestion data corruption | **Immediate Fail-Stop** | Halt before transformation; exit code 1; preserve existing outputs. |
| **CLI / User Argument Error** | Date format `2026-13-01` or `invalid-date` | User command input error | **Fail Fast** | Exit code 2; print usage instructions without initializing pipeline. |

---

## 3. Transient Failure Handling: Exponential Backoff & Jitter

The retry engine (`src/retry.py`) provides deterministic and production-safe retries for transient operations via `retry_with_backoff` and `retry_call`.

### Backoff Delay Formula
The delay before attempt $k$ (where $k \ge 1$) is calculated as:

$$\text{delay}_k = \min(\text{max\_delay}, \text{base\_delay} \times 2^{k-1}) + \text{jitter}$$

- **Base Delay**: Defaults to 1.0 second.
- **Max Delay**: Defaults to 60.0 seconds.
- **Jitter**: Uniform random noise between 0 and 0.5 seconds to eliminate thundering herd problems in concurrent worker topologies.
- **Retry-After Header**: If a remote server returns a `Retry-After` header (common with HTTP 429), the backoff engine honors that duration.

### Retry Limits & Exhaustion
- **Bounded Attempts**: Default maximum attempts is 3 (`max_attempts = 3`).
- **Exhaustion Behavior**: If all configured attempts fail, the engine raises `RetryExhaustedError`, encapsulating total attempts performed and the underlying exception. Infinite retry loops are strictly prevented.

---

## 4. Controlled Failure: Validation Gates & Output Protection

When a permanent failure or critical data quality breach is detected by `src/validate.py`:

### Safety Guarantees:
1. **Zero Execution of Downstream Stages**:
   - `transform_trip_data()` is never invoked.
   - `build_fact_trip()` and `build_dim_zone()` are never invoked.
   - `calculate_metrics()` is never invoked.
   - `publish_outputs()` is never invoked.
2. **Output Target Directory Protection**:
   - Zero files are written to `data/processed/`.
   - Any previously published valid datasets (e.g. earlier successful runs for July 2026) remain 100% untouched and uncorrupted.
   - Staging files (`.tmp_*`) are unlinked immediately.
3. **Structured Terminal & Log Diagnostics**:
   - Terminal prints a clean `PIPELINE FAILED` diagnostic specifying the exact failed stage and error explanation.
   - `logs/pipeline.log` records a detailed `[ERROR]` entry with the full stack trace.
   - CLI process exits with non-zero exit code (`1` for pipeline failures, `2` for CLI argument syntax errors).

---

## 5. Verification Test Coverage

The retry and failure handling subsystem is covered by 21 automated tests:
- **`tests/test_retry.py` (10 tests)**:
  - HTTP 500, 429, and network timeout retry recovery
  - Immediate rejection of HTTP 401/403 authentication failures
  - Immediate rejection of schema and validation errors
  - Maximum attempt exhaustion and infinite loop prevention
  - Sleep simulation and fast-path unit testing
- **`tests/test_controlled_failure.py` (11 tests)**:
  - Missing required column (`trip_distance`) detection
  - Validation error message accuracy
  - Spy verification that transform, model, and metrics do not run
  - Target directory cleanliness on failure
  - Protection of existing valid outputs upon rerun failure
  - Absence of success manifest on failure
  - Negative trip distance detection and gate enforcement

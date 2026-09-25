# Step 8J — Idempotency and Safe Rerun Architecture

This document formalizes the idempotency architecture, partition semantics, atomic replacement strategy, logical content fingerprinting, and failure-safe rerun guarantees for the **NYC Taxi Operations Intelligence Pipeline**.

---

## 1. Core Principle: What Idempotency Means

In data engineering, an operation is **idempotent** if running it multiple times produces the exact same state as running it once, without duplicate records, data corruption, or side effects:

```
    SAME INPUT
         +
    SAME REPORTING PERIOD
         +
    SAME PIPELINE LOGIC
         ↓
    SAME LOGICAL OUTPUT
```

### Idempotency Is NOT "Do Nothing"
Idempotency is explicitly **not** achieved by detecting existing outputs and exiting early. The pipeline re-executes all computational stages:
1. **Ingest**: Ingests raw source records.
2. **Validate**: Enforces validation gates on schema, nulls, and referential constraints.
3. **Transform**: Filters reporting boundaries and computes duration and quality flags.
4. **Model**: Builds relational entities (`fact_trip`, `dim_zone`).
5. **Metrics**: Computes the 6 headline KPIs and supporting breakdowns.
6. **Pre-Save Sanity**: Verifies output schemas, row counts, uniqueness, and references.
7. **Atomic Replace**: Safely swaps output files into the logical partition.

This proves determinism from first principles: same inputs under identical logic yield identical outputs.

---

## 2. Logical Run Key vs Execution Instance

The pipeline explicitly distinguishes between two concepts:

| Concept | Definition | Example | Role |
| :--- | :--- | :--- | :--- |
| **Logical Run Key** | The logical data partition defined by the reporting period. | `2026-07` | Determines target filenames and output partitions. |
| **Execution Instance** | Metadata identifying when a specific execution occurred. | `2026-09-25T16:05:00Z` | Recorded in execution logs and manifest `generated_at`. |

Multiple runs sharing the same logical run key target the same partition (`data/processed/fact_trip/fact_trip_2026-07.parquet`), replacing it rather than generating suffixed files (`*_1.parquet`, `*_2.parquet`) or timestamped duplicates.

---

## 3. Output Partitioning Strategy

Every processed output is strictly partitioned by the logical run key:

| Logical Dataset | Target Partition Path | Partition Strategy |
| :--- | :--- | :--- |
| `fact_trip` | `data/processed/fact_trip/fact_trip_{YYYY}-{MM}.parquet` | Overwritten atomically |
| `dim_zone` | `data/processed/dim_zone/dim_zone_{YYYY}-{MM}.parquet` | Overwritten atomically |
| `metrics` | `data/processed/metrics/metrics_{YYYY}-{MM}.csv` | Overwritten atomically |
| `manifest` | `data/processed/manifest_{YYYY}-{MM}.json` | Overwritten atomically |

Different reporting periods (e.g. `2026-07` and `2026-08`) write to distinct partition paths and coexist side-by-side without interference.

---

## 4. Replacement vs Append Semantics

Append modes (`mode="a"` in pandas, append mutations on Parquet) are strictly avoided:
- Appending records across reruns duplicates primary keys (`trip_id`), corrupting aggregations, doubling trip volumes, and distorting KPI calculations.
- Atomic replacement guarantees that a rerun completely reconstructs the target partition from validated models.

```
    Run 1
      ↓
    fact_trip_2026-07
      │
      │ rerun same period
      ▼
    Run 2
      ↓
    atomic replacement
      ↓
    fact_trip_2026-07
      ↓
    still 3,530,063 rows
```

---

## 5. Atomic Replacement Mechanism

Atomic replacement is enforced via [`atomic_write_file()`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/src/output.py):

1. **Staging**: Data is written into a hidden temporary file within the same filesystem directory:
   `data/processed/fact_trip/.fact_trip_2026-07.parquet.tmp.<uuid>`
2. **Commit**: Upon complete and successful serialization, `os.replace` performs an atomic filesystem inode swap.
3. **Failure Isolation**: If an error occurs prior to swap:
   - The temporary file is immediately unlinked.
   - The pre-existing valid output file remains completely untouched.
   - A partially-written or corrupt file is never exposed to downstream consumers.

---

## 6. Deterministic Logical Fingerprints

Parquet files contain writer metadata, compression dictionary details, and timestamps that cause binary hashes of the raw files to vary even when logical data is identical.

To prove logical idempotency, the pipeline computes SHA-256 fingerprints across logical representations:
- **`fact_trip` Fingerprint**: Rows sorted by primary key `trip_id`, projecting all 11 required columns, hashed with [`pandas.util.hash_pandas_object`](file:///home/pranav/Documents/nyc-taxi-fde-pipeline/src/output.py).
- **`dim_zone` Fingerprint**: Rows sorted by `LocationID`, projecting all 4 required columns, hashed.
- **`metrics` Fingerprint**: Rows sorted by `Metric Name`, projecting all 9 metric definition and value columns, hashed.

---

## 7. Real Data Rerun Proof (July 2026)

Executing the complete end-to-end pipeline twice on the July 2026 TLC dataset confirms total idempotency:

| Verification Metric | Run 1 | Run 2 | Status |
| :--- | :--- | :--- | :--- |
| **`fact_trip` Row Count** | 3,530,063 | 3,530,063 | **PASS** |
| **`dim_zone` Row Count** | 265 | 265 | **PASS** |
| **Unique `trip_id` Count** | 3,530,063 | 3,530,063 | **PASS** |
| **Unique `LocationID` Count** | 265 | 265 | **PASS** |
| **`fact_trip` Fingerprint** | `67fcb5767539c7c743552fc51fa8a3b80e2348ee46b68ca2c0a8721211de01a8` | `67fcb5767539c7c743552fc51fa8a3b80e2348ee46b68ca2c0a8721211de01a8` | **IDENTICAL** |
| **`dim_zone` Fingerprint** | `1878a6fce857ea83acb8816a56240a89dd2869aabbf23a1ba6f1e42b75d745d6` | `1878a6fce857ea83acb8816a56240a89dd2869aabbf23a1ba6f1e42b75d745d6` | **IDENTICAL** |
| **`metrics` Fingerprint** | `374337a2a6628a8b4a5a65b6fffca4d0f2d32ff54e61911e4dfadd203ea8a614` | `374337a2a6628a8b4a5a65b6fffca4d0f2d32ff54e61911e4dfadd203ea8a614` | **IDENTICAL** |
| **Trip Volume** | 3,530,063 | 3,530,063 | **PASS** |
| **Average Trip Duration** | 17.29 minutes | 17.29 minutes | **PASS** |
| **Median Trip Duration** | 14.17 minutes | 14.17 minutes | **PASS** |
| **Average Trip Distance** | 5.55 miles | 5.55 miles | **PASS** |
| **Invalid Duration Rate** | 0.0000283% | 0.0000283% | **PASS** |
| **Invalid Location Rate** | 0.00% | 0.00% | **PASS** |
| **Output File Duplication** | 0 extra files | 0 extra files | **PASS** |

---

## 8. Failure-Safe Rerun Guarantees

If a failure occurs during a rerun (e.g. disk quota exhaustion, process interruption, or memory limit breach):
1. The exception halts the execution before the atomic replace call.
2. The staging `.tmp` file is unlinked.
3. The previously published, valid partition remains intact and readable.
4. Downstream consumers are never presented with empty or corrupted tables.

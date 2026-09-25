# Step 8H — Metrics Definition & Traceability

## 1. Executive Summary

This document defines the 4 Business KPIs and 2 Data Quality KPIs calculated from `fact_trip` for the July 2026 reporting period.

---

## 2. Headline Metrics Definition Table

| Metric | Definition | Population | Numerator | Denominator | Unit | Current Value |
|---|---|---|---|---|---|---:|
| **Trip Volume** | Total number of July 2026 trip records in `fact_trip` | All July 2026 trips in `fact_trip` | `COUNT(trip_id)` | None | trips | **3,530,063** |
| **Average Trip Duration** | Mean duration in minutes among trips with valid chronology | Trips where `is_valid_duration == True` | `SUM(trip_duration_minutes)` | 3,530,062 | minutes | **17.29** |
| **Median Trip Duration** | Outlier-resistant median duration in minutes among valid trips | Trips where `is_valid_duration == True` | Deterministic pandas median | 3,530,062 | minutes | **14.17** |
| **Average Trip Distance** | Mean distance in miles among trips with valid non-negative distance | Trips where `is_valid_distance == True` | `SUM(trip_distance)` | 3,530,063 | miles | **5.55** |
| **Invalid Trip Duration Rate** | Percentage of trips with inverted timestamps (`dropoff < pickup`) | Trips with required timestamps (all July trips) | 1 | 3,530,063 | percent | **0.0000283%** |
| **Invalid/Unmatched Location Rate** | Percentage of trips with unmapped pickup or dropoff location IDs | All July 2026 trips | 0 | 3,530,063 | percent | **0.00%** |

---

## 3. Supporting Analytical Breakdown (Non-Headline)

### Trip Volume by Pickup Zone
Calculated by joining `fact_trip.pickup_location_id -> dim_zone.LocationID` and aggregating trip counts:

| Rank | Zone | Borough | July 2026 Trip Volume |
|---:|---|---|---:|
| 1 | **Midtown Center** | Manhattan | 196,165 |
| 2 | **JFK Airport** | Queens | 179,271 |
| 3 | **Upper East Side South** | Manhattan | 162,118 |
| 4 | **Upper East Side North** | Manhattan | 152,709 |
| 5 | **Penn Station/Madison Sq West** | Manhattan | 136,878 |

---

## 4. Metric Traceability Matrix

Every metric is fully traceable from raw input through transformation to the physical columns of `fact_trip`:

```text
Raw Source (Parquet/CSV)
       │
       ▼
Transform Layer (Filter & Derive Flags)
       │
       ▼
fact_trip Columns
       │
       ▼
Metric Calculations
```

| Metric | Required `fact_trip` Columns | Transformation Filter / Logic |
|---|---|---|
| **Trip Volume** | `trip_id` | Full population count |
| **Average Trip Duration** | `trip_duration_minutes`, `is_valid_duration` | Filter `is_valid_duration == True`, then mean |
| **Median Trip Duration** | `trip_duration_minutes`, `is_valid_duration` | Filter `is_valid_duration == True`, then median |
| **Average Trip Distance** | `trip_distance`, `is_valid_distance` | Filter `is_valid_distance == True`, then mean |
| **Invalid Trip Duration Rate** | `is_valid_duration` | Count `is_valid_duration == False` / total rows |
| **Invalid Location Rate** | `is_valid_location` | Count `is_valid_location == False` / total rows |

---

## 5. Knowns, Assumptions, Unknowns, and Limitations

### Known
- **3,530,063** July trip records in `fact_trip`.
- **3,530,062** trips with valid duration.
- Exactly **1** invalid chronology record preserved (`pickup = 22:24:27`, `dropoff = 22:24:17`).
- **0** unmatched location records (100% referential integrity against `dim_zone`).
- Exact unrounded metrics:
  - Average Duration: `17.2933...` minutes
  - Median Duration: `14.1666...` minutes
  - Average Distance: `5.5539...` miles
  - Invalid Duration Rate: `0.0000283281...%`
  - Invalid Location Rate: `0.0%`

### Assumptions
- **Reporting period boundary**: Strictly pickup-based (`pickup_datetime >= 2026-07-01 00:00:00` and `< 2026-08-01 00:00:00`). Dropoffs after midnight on August 1 remain valid July trips.
- **Reference integrity**: Any `LocationID` present in `dim_zone` is considered a valid location.
- **Distance validity**: Non-negative distance (`>= 0`) is valid; zero distance is intentionally valid.

### Unknown
- Underlying real-world causes for individual extreme durations or distances (e.g. traffic congestion, driver navigation errors, GPS drift).
- Weather conditions, passenger counts, or driver identities (untracked in core model).

### Limitations
- The analysis applies strictly to the July 2026 reporting period.
- `dim_zone` provides relational referential integrity, not physical geographic coordinate verification.
- Extreme but technically valid duration/distance values are not artificially clipped or excluded.

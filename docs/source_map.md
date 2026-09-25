# Source Map & Lineage Specification

## 1. Purpose

This source map connects each operational business and data quality question to the information required to answer it, the source system that provides that information, the physical storage paths, and the ingestion lineage.

The pipeline ingests a primary operational trip activity dataset and a reference dataset for translating taxi location identifiers into conformed spatial entities.

---

## 2. Ingested Source Systems Overview

| Source System | Provider | Raw Storage Path | Format | Record Count | Grain | Natural / Primary Key |
|---|---|---|---|---:|---|---|
| **NYC TLC Yellow Taxi Trip Records** | NYC Taxi & Limousine Commission | `data/raw/trips/yellow_tripdata_2026-07.parquet` | Apache Parquet (Snappy) | 3,530,109 rows | One record per taxi trip | Inherent composite (`pickup_datetime`, `PULocationID`, `DOLocationID`) |
| **NYC TLC Taxi Zone Lookup** | NYC Taxi & Limousine Commission | `data/raw/zones/taxi_zone_lookup.csv` | CSV | 265 rows | One record per taxi zone | `LocationID` (Unique integers 1–265) |

---

## 3. Business Question → Required Information → Source Traceability

| Business / Quality Question | Required Information | Source Dataset | Key Source Fields | Downstream Destination |
|---|---|---|---|---|
| **How much taxi activity occurred?** | Trip records within reporting period | TLC Trip Records | `tpep_pickup_datetime` | `fact_trip.trip_id`, Metric: `Trip Volume` |
| **How long do trips typically take?** | Pickup and dropoff timestamps | TLC Trip Records | `tpep_pickup_datetime`, `tpep_dropoff_datetime` | `fact_trip.trip_duration_minutes`, Metrics: `Average Trip Duration`, `Median Trip Duration` |
| **What distance is typically covered per trip?** | Validated trip distance | TLC Trip Records | `trip_distance` | `fact_trip.trip_distance`, Metric: `Average Trip Distance` |
| **Which pickup locations have highest activity?** | Location IDs, zone names, and boroughs | TLC Trips + Zone Lookup | `PULocationID` joined to `LocationID`, `Zone`, `Borough` | `fact_trip.pickup_location_id`, `dim_zone.Zone` |
| **What proportion of trips have invalid duration?** | Chronological ordering of timestamps | TLC Trip Records | `tpep_pickup_datetime`, `tpep_dropoff_datetime` | `fact_trip.is_valid_duration`, Metric: `Invalid Trip Duration Rate` |
| **What proportion of trips have unmapped locations?** | Referential integrity of location IDs | TLC Trips + Zone Lookup | `PULocationID`, `DOLocationID` checked against `LocationID` | `fact_trip.is_valid_location`, Metric: `Invalid/Unmatched Location Rate` |

---

## 4. Entity Relationships & Join Semantics

The Yellow Taxi Trip Records dataset represents the transactional fact data. The Taxi Zone Lookup represents conformed reference dimension data.

```text
       NYC TLC TRIP RECORDS (3,530,109 raw rows)
       ├── PULocationID ────────────────┐
       └── DOLocationID ──────────┐     │
                                  ▼     ▼
                        TAXI ZONE LOOKUP (265 rows)
                        └── LocationID (PK)
                              ├── Borough
                              ├── Zone
                              └── service_zone
```

### Join Mechanics:
- Both `PULocationID` and `DOLocationID` in `fact_trip` enforce foreign key referential integrity against `dim_zone.LocationID`.
- In the July 2026 dataset, 100% of location identifiers map to valid entries in the zone lookup (`Invalid/Unmatched Location Rate = 0.00%`).

---

## 5. Reporting Period Filter Criteria

- **Target Period**: Calendar month of **July 2026**.
- **Filter Field**: `tpep_pickup_datetime`.
- **Inclusion Window**: `2026-07-01 00:00:00 <= tpep_pickup_datetime < 2026-08-01 00:00:00`.
- **Boundary Semantics**:
  - Ingestion loads the entire raw dataset (`3,530,109` records) into memory/storage without alteration.
  - Transformation applies the pickup boundary, producing exactly `3,530,063` reporting trips.
  - 46 records with pickup timestamps outside July (December 2008: 1, June 2026: 7, August 2026: 38) are preserved in `data/raw/` and excluded from `fact_trip`.
  - Trips initiated on July 31 with dropoff in August remain valid July trips.
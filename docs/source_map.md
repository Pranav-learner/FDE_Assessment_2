# Source Map & Lineage Specification

## 1. Purpose

This source map establishes Class 4 source reasoning for the **NYC Taxi Operations Intelligence Pipeline**. It connects each operational business and data quality question to the exact information required to answer it, the authoritative source asset, the data grain, the external provider and ownership boundary, and known source gaps and limitations.

---

## 2. Ingested Primary Source Systems

The pipeline ingests two raw, immutable source assets provided by the City of New York:

| Dataset | Local Raw Storage Path | Storage Format | Source Record Count | Grain | Provider / External Ownership |
|---|---|---|---:|---|---|
| **NYC TLC Yellow Taxi Trip Records** | `data/raw/trips/yellow_tripdata_2026-07.parquet` | Apache Parquet (Snappy) | 3,530,109 rows | **One row = one taxi trip record** | NYC Taxi and Limousine Commission (TLC) *(Note: Specific internal agency team ownership is not published or available in the raw dataset)* |
| **NYC Taxi Zone Lookup** | `data/raw/zones/taxi_zone_lookup.csv` | CSV | 265 rows | **One row = one taxi zone reference record** | NYC Taxi and Limousine Commission (TLC) *(Note: Reference taxonomy maintained by TLC)* |

*Note: Raw assets in `data/raw/` are strictly immutable and preserved in their original state.*

---

## 3. Business Question to Source Mapping Matrix

The following table explicitly links each analytical business and data quality question to its required source information, underlying data grain, provider, and operational gaps:

| Business Question | Required Information | Source | Grain | Provider / Ownership | Gaps / Limitations |
|---|---|---|---|---|---|
| **How many taxi trips occurred during the reporting period?** | Trip records with pickup timestamps within July 2026 (`tpep_pickup_datetime`) | NYC TLC Yellow Taxi Trip Records (`yellow_tripdata_2026-07.parquet`) | One row = one taxi trip record | NYC TLC *(Internal operational ownership not available from dataset)* | Reflects historical July 2026 reporting partition rather than a real-time operational stream; cancelled or unfulfilled hail attempts are not captured. |
| **What was the typical trip duration?** | Pickup timestamp (`tpep_pickup_datetime`) and dropoff timestamp (`tpep_dropoff_datetime`) | NYC TLC Yellow Taxi Trip Records (`yellow_tripdata_2026-07.parquet`) | One row = one taxi trip record | NYC TLC *(Internal operational ownership not available from dataset)* | Traffic congestion, weather delays, and route detours are unrecorded; 1 trip exhibits corrupted inverted chronology (`dropoff < pickup`). |
| **What was the average trip distance?** | Trip travel distance in miles (`trip_distance`) | NYC TLC Yellow Taxi Trip Records (`yellow_tripdata_2026-07.parquet`) | One row = one taxi trip record | NYC TLC *(Internal operational ownership not available from dataset)* | Distance is meter-reported odometer/GPS distance; no turn-by-turn waypoint telemetry or physical route tracks are provided. |
| **Are the location references complete and valid?** | Trip pickup/dropoff identifiers (`PULocationID`, `DOLocationID`) matched to official taxi zone lookup (`LocationID`) | NYC TLC Yellow Taxi Trip Records + NYC Taxi Zone Lookup (`taxi_zone_lookup.csv`) | Trip: One row = one trip; Zone: One row = one zone reference | NYC TLC *(Internal operational ownership not available from dataset)* | Referential integrity is confirmed across all 265 zones, but source does not provide exact GPS polygon boundaries or pinpoint coordinates. |
| **Can the resulting operational metrics be trusted given the observed data quality?** | Timestamp chronology, referential integrity, distance validity, and null checks | Both sources validated via automated schema & DQ gates | System-wide across trip and zone grains | NYC TLC *(Validated downstream by pipeline gates)* | Telemetry lacks driver shift context, passenger demographics, and meter calibration logs to explain root causes of anomalies. |

---

## 4. Entity Relationships & Join Semantics

The Yellow Taxi Trip Records dataset represents transactional fact data, joined to the Taxi Zone Lookup reference dimension:

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

- **Foreign Key Constraints**: Both `PULocationID` and `DOLocationID` in `fact_trip` enforce foreign key integrity against `dim_zone.LocationID`.
- **Referential Match Rate**: 100% of July 2026 trips match valid zone references (`Invalid/Unmatched Location Rate = 0.00%`).

---

## 5. Known Source Gaps & Limitations

To ensure intellectual honesty and prevent unsupported causal claims, the following gaps in the source data are explicitly documented:
- **No Driver Identity**: Medallion, hack license, driver shift hours, and driver experience are completely absent.
- **No Passenger Identity**: Passenger demographics, party composition, and customer satisfaction ratings are unobserved.
- **No Traffic or Environmental Variables**: Road construction, traffic congestion incidents, rain, snow, and ambient temperature are not tracked.
- **No Customer Experience / Service Quality**: Customer wait times, dispatch latency, ratings, and driver tip motivations are not available.
- **No Causal Operational Explanations**: The data records *what* occurred (timestamps, meter distances, locations) but cannot determine *why* a particular journey was delayed.
- **Historical Partition vs. Real-Time Stream**: The dataset is a batch-published historical artifact (July 2026) rather than a live operational event bus.
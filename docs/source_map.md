# Source Map

## 1. Purpose

This source map connects each business question to the information
required to answer it and the source system that provides that
information.

The project uses a primary operational trip dataset and a reference
dataset for translating taxi location identifiers into business-readable
locations.

---

## 2. Source Overview

| Source | Purpose | Type | Grain | Key |
|---|---|---|---|---|
| NYC TLC Yellow Taxi Trip Records | Provides trip-level operational data | Parquet/file | One row per trip record | Trip record fields + location IDs |
| NYC TLC Taxi Zone Lookup | Maps taxi location IDs to zone and borough information | CSV/reference file | One row per taxi zone | LocationID |

---

## 3. Business Question → Required Information → Source

| Business Question | Required Information | Source | Important Fields |
|---|---|---|---|
| How much taxi activity occurred? | Trip records and reporting-period timestamps | TLC Trip Records | pickup timestamp |
| How long do trips typically take? | Pickup and drop-off timestamps | TLC Trip Records | pickup timestamp, drop-off timestamp |
| What distance is typically covered per trip? | Trip distance | TLC Trip Records | trip_distance |
| Which pickup locations have the highest trip activity? | Pickup location ID + location name + borough | TLC Trip Records + Taxi Zone Lookup | PULocationID, LocationID, Zone, Borough |
| What proportion of trips have invalid durations? | Pickup/drop-off timestamps | TLC Trip Records | pickup timestamp, drop-off timestamp |
| What proportion of trips have unmapped locations? | Pickup/drop-off location IDs + valid location reference IDs | TLC Trip Records + Taxi Zone Lookup | PULocationID, DOLocationID, LocationID |

---

## 4. Source Relationships

The trip dataset is the primary operational source.

The Taxi Zone Lookup is reference data used to interpret
PULocationID and DOLocationID.

```text
              TLC TRIP RECORDS
                     │
                     │ PULocationID
                     │ DOLocationID
                     ▼
             TAXI ZONE LOOKUP
                     │
                     ▼
           Zone / Borough Information
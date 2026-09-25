# Retrieval Manifest

## 1. Reporting Period

Target reporting period:

**July 2026**

Reporting period is defined using:

`tpep_pickup_datetime`

A trip belongs to the reporting period when:

- pickup >= 2026-07-01 00:00:00
- pickup < 2026-08-01 00:00:00

---

## 2. Source 1 — NYC TLC Yellow Taxi Trip Records

### Purpose

Provides operational taxi trip records required to calculate
trip-level business and data-quality metrics.

### Format

Parquet

### Local raw file

`data/raw/trips/yellow_tripdata_2026-07.parquet`

### Grain

One row represents one taxi trip record.

### Important fields

- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `trip_distance`

### Retrieval mode 1

The Parquet file was retrieved and loaded using Python/pandas.

```python
pd.read_parquet(...)
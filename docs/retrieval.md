# Data Retrieval Evidence

## Reporting Period

July 2026

## Source 1 — Yellow Taxi Trip Records

Source:
NYC Taxi and Limousine Commission

File:
yellow_tripdata_2026-07.parquet

Format:
Parquet

Retrieval method:
Downloaded source file and loaded using pandas/pyarrow.

Raw path:
data/raw/trips/yellow_tripdata_2026-07.parquet

Status:
Retrieved successfully

Row count:
3,530,109

Column count:
21

---

## Source 2 — Taxi Zone Lookup

Source:
NYC Taxi and Limousine Commission

File:
taxi_zone_lookup.csv

Format:
CSV

Retrieval method:
Downloaded source file and loaded using pandas.

Raw path:
data/raw/zones/taxi_zone_lookup.csv

Status:
Retrieved successfully

Row count:
265

Column count:
4

---

## Completeness Evidence

For the trip source, the selected reporting period is July 2026 and
the expected monthly Yellow Taxi Parquet file was successfully
retrieved and loaded.

For the zone reference source, the official Taxi Zone Lookup CSV was
successfully retrieved and loaded.

Raw inputs are preserved unchanged under data/raw/.
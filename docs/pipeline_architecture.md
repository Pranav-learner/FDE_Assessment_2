# Pipeline Architecture

## Objective

Build a repeatable and dependable pipeline that transforms
NYC TLC taxi trip data into validated modeled data and
business/data-quality metrics.

## Pipeline Flow

Extract → Validate → Transform → Model → Metrics → Save → Log

## Stages

### 1. Extract / Ingest
- Load NYC TLC trip Parquet data.
- Load taxi zone CSV reference data.
- Preserve raw inputs.

### 2. Validate
- Validate required columns.
- Validate zone reference uniqueness.
- Validate required timestamps.
- Validate location references.
- Validate distance rules.
- Stop publication when critical validation fails.

### 3. Transform
- Select July 2026 reporting period using pickup datetime.
- Derive trip duration.
- Generate validation flags.

### 4. Model
Create:
- fact_trip
- dim_zone

### 5. Metrics

Business:
- Trip Volume
- Average Trip Duration
- Median Trip Duration
- Average Trip Distance

Data Quality:
- Invalid Trip Duration Rate
- Invalid/Unmatched Location Rate

### 6. Save
Publish validated modeled data and metric outputs.

### 7. Logging
Record:
- run status
- row counts
- validation results
- retry attempts
- execution timing
- failures

## Reliability

### Retryable failures
- transient HTTP/server errors
- rate limiting
- temporary connection failures

### Non-retryable failures
- missing required columns
- incompatible schema
- invalid credentials
- business-rule validation failures

### Idempotency

A rerun for the same logical reporting period must rebuild or
replace the corresponding output rather than duplicate records.

### Failure Handling

If a critical validation gate fails:
- stop the pipeline
- log the failure
- do not publish processed output
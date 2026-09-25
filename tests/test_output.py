"""Unit and integration tests for Step 8I — Output Persistence and Logging.

Covers:
- Test 1: Modeled fact_trip output writing and read-back.
- Test 2: Modeled dim_zone output writing and schema preservation.
- Test 3: Metrics CSV output and KPI preservation.
- Test 4: Output sanity check (empty/invalid fact_trip fails).
- Test 5: Duplicate trip_id failure check.
- Test 6: Missing headline metric failure check.
- Test 7: Output manifest structure and content.
- Test 8: Logging level verification (INFO, WARNING, ERROR).
- Test 9: Atomic write failure safety (no dirty partial output).
- Test 10: Real July 2026 data end-to-end output publication.
"""

from datetime import datetime
import json
import logging
from pathlib import Path
import pytest
import pandas as pd

from src.config import PipelineConfig, create_config, default_config
from src.ingest import load_trip_data, load_zone_data
from src.logger import get_logger
from src.metrics import calculate_metrics, metrics_to_dataframe
from src.model import build_dim_zone, build_fact_trip
from src.output import (
    FACT_TRIP_REQUIRED_COLUMNS,
    DIM_ZONE_REQUIRED_COLUMNS,
    HEADLINE_METRIC_NAMES,
    OutputValidationError,
    OutputWriteError,
    atomic_write_csv,
    atomic_write_file,
    atomic_write_json,
    atomic_write_parquet,
    generate_manifest,
    publish_outputs,
    validate_outputs,
)
from src.transform import filter_reporting_period, transform_trip_data


@pytest.fixture
def sample_dim_zone() -> pd.DataFrame:
    """Fixture providing a synthetic 265-row dim_zone table."""
    records = []
    for loc_id in range(1, 266):
        records.append({
            "LocationID": loc_id,
            "Borough": "Manhattan" if loc_id <= 100 else "Queens",
            "Zone": f"Zone_{loc_id}",
            "service_zone": "Yellow Zone",
        })
    return pd.DataFrame(records)


@pytest.fixture
def sample_fact_trip(sample_dim_zone: pd.DataFrame) -> pd.DataFrame:
    """Fixture providing a valid synthetic fact_trip table."""
    return pd.DataFrame({
        "trip_id": [1, 2, 3],
        "pickup_datetime": pd.to_datetime([
            "2026-07-01 10:00:00",
            "2026-07-02 12:30:00",
            "2026-07-03 14:15:00",
        ]),
        "dropoff_datetime": pd.to_datetime([
            "2026-07-01 10:15:00",
            "2026-07-02 12:50:00",
            "2026-07-03 14:35:00",
        ]),
        "pickup_location_id": [1, 2, 3],
        "dropoff_location_id": [4, 5, 6],
        "trip_distance": [2.5, 3.8, 1.2],
        "trip_duration_minutes": [15.0, 20.0, 20.0],
        "is_valid_duration": [True, True, True],
        "is_valid_location": [True, True, True],
        "is_valid_distance": [True, True, True],
        "is_valid_trip": [True, True, True],
    })


@pytest.fixture
def sample_metrics_df() -> pd.DataFrame:
    """Fixture providing a valid 6-metric DataFrame."""
    return pd.DataFrame([
        {
            "Metric Name": "Trip Volume",
            "Type": "Business KPI",
            "Value": 3,
            "Display Value": "3",
            "Unit": "trips",
            "Numerator": 3,
            "Denominator": None,
            "Population": "All trips",
            "Definition": "Total count of trips",
        },
        {
            "Metric Name": "Average Trip Duration",
            "Type": "Business KPI",
            "Value": 18.33,
            "Display Value": "18.33 minutes",
            "Unit": "minutes",
            "Numerator": 55.0,
            "Denominator": 3,
            "Population": "Valid duration trips",
            "Definition": "Mean duration",
        },
        {
            "Metric Name": "Median Trip Duration",
            "Type": "Business KPI",
            "Value": 20.0,
            "Display Value": "20.00 minutes",
            "Unit": "minutes",
            "Numerator": None,
            "Denominator": 3,
            "Population": "Valid duration trips",
            "Definition": "50th percentile duration",
        },
        {
            "Metric Name": "Average Trip Distance",
            "Type": "Business KPI",
            "Value": 2.5,
            "Display Value": "2.50 miles",
            "Unit": "miles",
            "Numerator": 7.5,
            "Denominator": 3,
            "Population": "Valid distance trips",
            "Definition": "Mean distance",
        },
        {
            "Metric Name": "Invalid Trip Duration Rate",
            "Type": "Data Quality KPI",
            "Value": 0.0,
            "Display Value": "0.0000000%",
            "Unit": "percent",
            "Numerator": 0,
            "Denominator": 3,
            "Population": "All trips",
            "Definition": "Ratio of invalid duration",
        },
        {
            "Metric Name": "Invalid/Unmatched Location Rate",
            "Type": "Data Quality KPI",
            "Value": 0.0,
            "Display Value": "0.00%",
            "Unit": "percent",
            "Numerator": 0,
            "Denominator": 3,
            "Population": "All trips",
            "Definition": "Ratio of invalid locations",
        },
    ])


@pytest.fixture
def tmp_config(tmp_path: Path) -> PipelineConfig:
    """Fixture providing a PipelineConfig rooted in a temporary directory."""
    return create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)


# ============================================================================
# TEST 1 — FACT OUTPUT
# ============================================================================

def test_fact_output_writing_and_readback(
    tmp_path: Path,
    sample_fact_trip: pd.DataFrame,
):
    """Test 1: Write synthetic fact_trip, verify file creation, read-back, and row preservation."""
    target_path = tmp_path / "fact_trip" / "fact_trip_2026-07.parquet"
    atomic_write_parquet(sample_fact_trip, target_path)

    assert target_path.exists()
    assert target_path.is_file()

    read_back = pd.read_parquet(target_path)
    assert len(read_back) == len(sample_fact_trip)
    for col in FACT_TRIP_REQUIRED_COLUMNS:
        assert col in read_back.columns
    assert list(read_back["trip_id"]) == [1, 2, 3]


# ============================================================================
# TEST 2 — ZONE OUTPUT
# ============================================================================

def test_zone_output_writing_and_schema(
    tmp_path: Path,
    sample_dim_zone: pd.DataFrame,
):
    """Test 2: Write synthetic dim_zone, verify file exists, rows preserved, schema preserved."""
    target_path = tmp_path / "dim_zone" / "dim_zone_2026-07.parquet"
    atomic_write_parquet(sample_dim_zone, target_path)

    assert target_path.exists()
    read_back = pd.read_parquet(target_path)
    assert len(read_back) == 265
    for col in DIM_ZONE_REQUIRED_COLUMNS:
        assert col in read_back.columns


# ============================================================================
# TEST 3 — METRICS OUTPUT
# ============================================================================

def test_metrics_output_csv_and_values(
    tmp_path: Path,
    sample_metrics_df: pd.DataFrame,
):
    """Test 3: Write metrics CSV, verify existence, all 6 headline metrics, values preserved."""
    target_path = tmp_path / "metrics" / "metrics_2026-07.csv"
    atomic_write_csv(sample_metrics_df, target_path)

    assert target_path.exists()
    read_back = pd.read_csv(target_path)
    assert len(read_back) == 6
    assert "Metric Name" in read_back.columns
    assert "Display Value" in read_back.columns

    saved_metric_names = set(read_back["Metric Name"])
    for expected_name in HEADLINE_METRIC_NAMES:
        assert expected_name in saved_metric_names

    vol_row = read_back[read_back["Metric Name"] == "Trip Volume"].iloc[0]
    assert int(vol_row["Value"]) == 3
    assert vol_row["Display Value"] == "3"


# ============================================================================
# TEST 4 — OUTPUT SANITY CHECK (EMPTY / INVALID FACT_TRIP)
# ============================================================================

def test_empty_fact_trip_fails_sanity_check(
    sample_dim_zone: pd.DataFrame,
    sample_metrics_df: pd.DataFrame,
):
    """Test 4: Empty fact_trip DataFrame fails output validation with descriptive error."""
    empty_fact = pd.DataFrame(columns=FACT_TRIP_REQUIRED_COLUMNS)
    with pytest.raises(OutputValidationError, match="fact_trip must be a non-empty"):
        validate_outputs(empty_fact, sample_dim_zone, sample_metrics_df)


def test_missing_column_fact_trip_fails_sanity_check(
    sample_fact_trip: pd.DataFrame,
    sample_dim_zone: pd.DataFrame,
    sample_metrics_df: pd.DataFrame,
):
    """Missing required fact_trip column fails output validation."""
    invalid_fact = sample_fact_trip.drop(columns=["trip_duration_minutes"])
    with pytest.raises(OutputValidationError, match="missing required output column"):
        validate_outputs(invalid_fact, sample_dim_zone, sample_metrics_df)


# ============================================================================
# TEST 5 — DUPLICATE TRIP ID
# ============================================================================

def test_duplicate_trip_id_fails_sanity_check(
    sample_fact_trip: pd.DataFrame,
    sample_dim_zone: pd.DataFrame,
    sample_metrics_df: pd.DataFrame,
):
    """Test 5: Duplicate trip_id in fact_trip fails output validation."""
    duplicated_fact = sample_fact_trip.copy()
    duplicated_fact.loc[1, "trip_id"] = duplicated_fact.loc[0, "trip_id"]

    with pytest.raises(OutputValidationError, match="duplicate trip_id"):
        validate_outputs(duplicated_fact, sample_dim_zone, sample_metrics_df)


# ============================================================================
# TEST 6 — MISSING METRIC
# ============================================================================

def test_missing_headline_metric_fails_sanity_check(
    sample_fact_trip: pd.DataFrame,
    sample_dim_zone: pd.DataFrame,
    sample_metrics_df: pd.DataFrame,
):
    """Test 6: Metrics DataFrame missing one of the six headline metrics fails sanity validation."""
    # Drop "Invalid/Unmatched Location Rate"
    incomplete_metrics = sample_metrics_df[
        sample_metrics_df["Metric Name"] != "Invalid/Unmatched Location Rate"
    ]
    with pytest.raises(OutputValidationError, match="missing headline metric"):
        validate_outputs(sample_fact_trip, sample_dim_zone, incomplete_metrics)


def test_invalid_foreign_key_fails_sanity_check(
    sample_fact_trip: pd.DataFrame,
    sample_dim_zone: pd.DataFrame,
    sample_metrics_df: pd.DataFrame,
):
    """fact_trip with pickup location not in dim_zone fails foreign key check."""
    invalid_fk_fact = sample_fact_trip.copy()
    invalid_fk_fact.loc[0, "pickup_location_id"] = 9999

    with pytest.raises(OutputValidationError, match="invalid pickup_location_id reference"):
        validate_outputs(invalid_fk_fact, sample_dim_zone, sample_metrics_df)


# ============================================================================
# TEST 7 — MANIFEST
# ============================================================================

def test_manifest_structure_and_persistence(tmp_path: Path):
    """Test 7: Verify manifest contains all expected fields and can be read back."""
    fact_path = tmp_path / "fact_trip" / "fact_trip_2026-07.parquet"
    zone_path = tmp_path / "dim_zone" / "dim_zone_2026-07.parquet"
    metrics_path = tmp_path / "metrics" / "metrics_2026-07.csv"
    manifest_path = tmp_path / "manifest_2026-07.json"

    manifest = generate_manifest(
        reporting_period="2026-07",
        fact_trip_path=fact_path,
        fact_trip_rows=3530063,
        dim_zone_path=zone_path,
        dim_zone_rows=265,
        metrics_path=metrics_path,
        metric_count=6,
        pipeline_status="success",
    )

    assert manifest["reporting_period"] == "2026-07"
    assert manifest["fact_trip_path"] == str(fact_path)
    assert manifest["fact_trip_rows"] == 3530063
    assert manifest["dim_zone_path"] == str(zone_path)
    assert manifest["dim_zone_rows"] == 265
    assert manifest["metrics_path"] == str(metrics_path)
    assert manifest["metric_count"] == 6
    assert manifest["pipeline_status"] == "success"
    assert "generated_at" in manifest

    atomic_write_json(manifest, manifest_path)
    assert manifest_path.exists()

    with open(manifest_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    assert loaded["reporting_period"] == "2026-07"
    assert loaded["fact_trip_rows"] == 3530063
    assert loaded["dim_zone_rows"] == 265
    assert loaded["metric_count"] == 6


# ============================================================================
# TEST 8 — LOGGING
# ============================================================================

def test_logging_levels_and_file_destination(tmp_path: Path):
    """Test 8: Verify INFO, WARNING, and ERROR logging can be directed to a file and read back."""
    test_log_file = tmp_path / "logs" / "test_run.log"
    logger = get_logger(
        name="test_logger_pipeline",
        log_file=test_log_file,
        level=logging.INFO,
        clear_handlers=True,
    )

    logger.info("Pipeline started for period 2026-07")
    logger.warning("Invalid chronology records detected: 1")
    logger.error("Simulation of a pipeline failure condition")

    assert test_log_file.exists()
    content = test_log_file.read_text(encoding="utf-8")

    assert "[INFO]" in content
    assert "Pipeline started for period 2026-07" in content
    assert "[WARNING]" in content
    assert "Invalid chronology records detected: 1" in content
    assert "[ERROR]" in content
    assert "Simulation of a pipeline failure condition" in content


# ============================================================================
# TEST 9 — ATOMIC WRITE FAILURE SAFETY
# ============================================================================

def test_atomic_write_failure_leaves_no_dirty_file(tmp_path: Path):
    """Test 9: Simulate a write failure and verify that no corrupted final output or leftover temp file exists."""
    target_path = tmp_path / "outputs" / "critical_data.parquet"

    def faulty_writer(path: Path) -> None:
        # Write partial data then explode
        with open(path, "w") as f:
            f.write("partial corrupt content")
        raise IOError("Disk full or network partition simulation")

    with pytest.raises(OutputWriteError, match="Atomic persistence failed"):
        atomic_write_file(target_path, faulty_writer)

    # Destination file must NOT exist
    assert not target_path.exists()

    # No leftover .tmp files should remain in the directory
    temp_files = list(target_path.parent.glob(".*.tmp.*"))
    assert len(temp_files) == 0


def test_atomic_write_preserves_existing_file_on_failure(tmp_path: Path):
    """If target file already existed, a failed atomic write leaves original intact."""
    target_path = tmp_path / "outputs" / "existing.parquet"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text("pristine original content", encoding="utf-8")

    def faulty_writer(path: Path) -> None:
        raise RuntimeError("Failure before completing write")

    with pytest.raises(OutputWriteError):
        atomic_write_file(target_path, faulty_writer)

    assert target_path.exists()
    assert target_path.read_text(encoding="utf-8") == "pristine original content"


# ============================================================================
# TEST 10 — REAL-DATA OUTPUT TEST (Section 16)
# ============================================================================

def test_real_tlc_output_publication(tmp_path: Path):
    """Test 10 (Section 16): Execute output publication using real July 2026 TLC data.

    Verifies:
        fact_trip rows = 3,530,063
        dim_zone rows = 265
        metrics = 6 headline KPIs
        display values match Class 7 / 8H exact requirements.
    """
    # 1. Ingest
    raw_trips = load_trip_data(default_config)
    raw_zones = load_zone_data(default_config)

    # 2. Transform
    transformed_trips = transform_trip_data(raw_trips, raw_zones, default_config)

    # 3. Model
    dim_zone = build_dim_zone(raw_zones)
    fact_trip = build_fact_trip(transformed_trips)

    # 4. Metrics
    metrics_list = calculate_metrics(fact_trip, dim_zone)
    metrics_df = metrics_to_dataframe(metrics_list)

    # 5. Output config pointing to tmp_path
    output_cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    log_path = tmp_path / "logs" / "pipeline_run.log"
    logger = get_logger("real_data_test_logger", log_file=log_path, clear_handlers=True)

    # 6. Publish outputs
    manifest = publish_outputs(
        fact_trip=fact_trip,
        dim_zone=dim_zone,
        metrics_data=metrics_df,
        config=output_cfg,
        logger=logger,
        expected_trip_rows=3530063,
        expected_zone_rows=265,
    )

    # 7. Verify generated files
    expected_fact_path = output_cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    expected_zone_path = output_cfg.dim_zone_output_dir / "dim_zone_2026-07.parquet"
    expected_metrics_path = output_cfg.metrics_output_dir / "metrics_2026-07.csv"
    expected_manifest_path = output_cfg.processed_dir / "manifest_2026-07.json"

    assert expected_fact_path.exists()
    assert expected_zone_path.exists()
    assert expected_metrics_path.exists()
    assert expected_manifest_path.exists()

    # 8. Verify row counts and contents
    persisted_fact = pd.read_parquet(expected_fact_path)
    assert len(persisted_fact) == 3530063

    persisted_zone = pd.read_parquet(expected_zone_path)
    assert len(persisted_zone) == 265

    persisted_metrics = pd.read_csv(expected_metrics_path)
    assert len(persisted_metrics) == 6

    # 9. Verify metric display values from CSV match requirements
    metrics_lookup = dict(zip(persisted_metrics["Metric Name"], persisted_metrics["Display Value"]))
    assert metrics_lookup["Trip Volume"] == "3,530,063"
    assert metrics_lookup["Average Trip Duration"] == "17.29 minutes"
    assert metrics_lookup["Median Trip Duration"] == "14.17 minutes"
    assert metrics_lookup["Average Trip Distance"] == "5.55 miles"
    assert metrics_lookup["Invalid Trip Duration Rate"] == "0.0000283%"
    assert metrics_lookup["Invalid/Unmatched Location Rate"] == "0.00%"

    # 10. Verify manifest
    assert manifest["reporting_period"] == "2026-07"
    assert manifest["fact_trip_rows"] == 3530063
    assert manifest["dim_zone_rows"] == 265
    assert manifest["metric_count"] == 6
    assert manifest["pipeline_status"] == "success"

    # 11. Verify logs
    log_text = log_path.read_text(encoding="utf-8")
    assert "Output validation passed" in log_text
    assert "Saved fact_trip (3,530,063 rows)" in log_text
    assert "Saved dim_zone (265 rows)" in log_text
    assert "Saved metrics (6 KPIs)" in log_text
    assert "Saved manifest" in log_text
    assert "Pipeline outputs successfully published." in log_text

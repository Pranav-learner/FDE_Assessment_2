"""Unit and integration tests for Step 8K — Controlled Failure Demonstration.

Verifies:
1. Missing required source column ('trip_distance') causes pipeline execution to halt.
2. Error message clearly identifies the missing required column.
3. Transformation stage does NOT execute.
4. Modeling stage does NOT execute.
5. Metrics calculation stage does NOT execute.
6. No processed output files are published to an empty destination.
7. Existing valid outputs remain intact, unchanged, and uncorrupted.
8. No successful manifest is published on failure.
9. An ERROR-level log entry is recorded identifying the failing stage and column.
10. Critical validation failures are NOT retried by the retry mechanism.
11. Second critical failure scenario: negative trip distance halts execution safely.
"""

from pathlib import Path
from unittest.mock import patch
import pytest
import pandas as pd

from src.config import PipelineConfig
from src.logger import get_logger
from src.output import compute_fact_trip_fingerprint
from src.pipeline import run
from src.retry import is_retryable_error, retry_call
from src.validate import ValidationError


@pytest.fixture
def synthetic_valid_zones_csv(tmp_path: Path) -> Path:
    """Fixture creating a valid taxi_zone_lookup.csv in tmp_path."""
    zone_dir = tmp_path / "raw" / "zones"
    zone_dir.mkdir(parents=True, exist_ok=True)
    zone_file = zone_dir / "taxi_zone_lookup.csv"

    records = [
        {"LocationID": loc_id, "Borough": "Manhattan", "Zone": f"Zone_{loc_id}", "service_zone": "Yellow"}
        for loc_id in range(1, 266)
    ]
    pd.DataFrame(records).to_csv(zone_file, index=False)
    return zone_file


@pytest.fixture
def synthetic_invalid_trips_parquet(tmp_path: Path) -> Path:
    """Fixture creating an invalid yellow_tripdata_2026-07.parquet missing 'trip_distance'."""
    trip_dir = tmp_path / "raw" / "trips"
    trip_dir.mkdir(parents=True, exist_ok=True)
    trip_file = trip_dir / "yellow_tripdata_2026-07.parquet"

    # Trip records missing 'trip_distance'
    invalid_trips = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00", "2026-07-01 11:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00", "2026-07-01 11:20:00"]),
        "PULocationID": [1, 2],
        "DOLocationID": [3, 4],
        # 'trip_distance' is intentionally omitted!
    })
    invalid_trips.to_parquet(trip_file, index=False)
    return trip_file


@pytest.fixture
def failure_config(
    tmp_path: Path,
    synthetic_invalid_trips_parquet: Path,
    synthetic_valid_zones_csv: Path,
) -> PipelineConfig:
    """Fixture creating a PipelineConfig pointing to invalid inputs and a clean output directory."""
    project_root = tmp_path / "mock_project"
    cfg = PipelineConfig(
        project_root=project_root,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=synthetic_invalid_trips_parquet,
        zone_input_path=synthetic_valid_zones_csv,
        fact_trip_output_dir=project_root / "data" / "processed" / "fact_trip",
        dim_zone_output_dir=project_root / "data" / "processed" / "dim_zone",
        metrics_output_dir=project_root / "data" / "processed" / "metrics",
    )
    return cfg


# ============================================================================
# TEST 1 — MISSING REQUIRED COLUMN
# ============================================================================

def test_missing_required_column_fails_pipeline(failure_config: PipelineConfig):
    """Test 1: Removing required column 'trip_distance' causes pipeline execution to halt with ValidationError."""
    with pytest.raises(ValidationError):
        run(failure_config)


# ============================================================================
# TEST 2 — ERROR MESSAGE
# ============================================================================

def test_validation_error_message_identifies_missing_column(failure_config: PipelineConfig):
    """Test 2: Validation failure message explicitly identifies 'trip_distance' as the missing column."""
    with pytest.raises(ValidationError) as exc_info:
        run(failure_config)

    error_message = str(exc_info.value)
    assert "missing required trip columns" in error_message
    assert "trip_distance" in error_message


# ============================================================================
# TEST 3 — NO TRANSFORMATION
# ============================================================================

def test_transformation_does_not_execute_on_validation_failure(failure_config: PipelineConfig):
    """Test 3: Verify transform_trip_data is never called when validation fails."""
    with patch("src.pipeline.transform_trip_data") as mock_transform:
        with pytest.raises(ValidationError):
            run(failure_config)
        mock_transform.assert_not_called()


# ============================================================================
# TEST 4 — NO MODEL
# ============================================================================

def test_model_does_not_execute_on_validation_failure(failure_config: PipelineConfig):
    """Test 4: Verify build_fact_trip and build_dim_zone are never called when validation fails."""
    with patch("src.pipeline.build_fact_trip") as mock_fact, \
         patch("src.pipeline.build_dim_zone") as mock_dim:
        with pytest.raises(ValidationError):
            run(failure_config)
        mock_fact.assert_not_called()
        mock_dim.assert_not_called()


# ============================================================================
# TEST 5 — NO METRICS
# ============================================================================

def test_metrics_do_not_execute_on_validation_failure(failure_config: PipelineConfig):
    """Test 5: Verify calculate_metrics is never called when validation fails."""
    with patch("src.pipeline.calculate_metrics") as mock_metrics:
        with pytest.raises(ValidationError):
            run(failure_config)
        mock_metrics.assert_not_called()


# ============================================================================
# TEST 6 — NO OUTPUT
# ============================================================================

def test_no_processed_output_published_on_failure(failure_config: PipelineConfig):
    """Test 6: In an empty destination, a failed pipeline run writes zero processed files."""
    with pytest.raises(ValidationError):
        run(failure_config)

    if failure_config.fact_trip_output_dir.exists():
        fact_files = list(failure_config.fact_trip_output_dir.glob("*.parquet"))
        assert len(fact_files) == 0, f"Unexpected fact files published: {fact_files}"

    if failure_config.dim_zone_output_dir.exists():
        zone_files = list(failure_config.dim_zone_output_dir.glob("*.parquet"))
        assert len(zone_files) == 0, f"Unexpected zone files published: {zone_files}"

    if failure_config.metrics_output_dir.exists():
        metrics_files = list(failure_config.metrics_output_dir.glob("*.csv"))
        assert len(metrics_files) == 0, f"Unexpected metrics files published: {metrics_files}"

    manifest_file = failure_config.processed_dir / "manifest_2026-07.json"
    assert not manifest_file.exists(), "Manifest should not exist after failed run."


# ============================================================================
# TEST 7 — PREVIOUS VALID OUTPUT PRESERVED
# ============================================================================

def test_previous_valid_output_preserved_after_controlled_failure(
    tmp_path: Path,
    synthetic_valid_zones_csv: Path,
    synthetic_invalid_trips_parquet: Path,
):
    """Test 7: A failed second run leaves an existing successful output completely untouched."""
    project_root = tmp_path / "preserve_test_project"

    # Step 1: Create a valid trip source
    valid_trip_file = tmp_path / "valid_trips.parquet"
    valid_trips = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00", "2026-07-02 12:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00", "2026-07-02 12:20:00"]),
        "PULocationID": [1, 2],
        "DOLocationID": [3, 4],
        "trip_distance": [2.5, 3.0],
    })
    valid_trips.to_parquet(valid_trip_file, index=False)

    valid_cfg = PipelineConfig(
        project_root=project_root,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=valid_trip_file,
        zone_input_path=synthetic_valid_zones_csv,
        fact_trip_output_dir=project_root / "data" / "processed" / "fact_trip",
        dim_zone_output_dir=project_root / "data" / "processed" / "dim_zone",
        metrics_output_dir=project_root / "data" / "processed" / "metrics",
    )

    # Run 1: Succeeds
    res1 = run(valid_cfg, expected_trip_rows=2)
    assert res1["status"] == "success"

    fact_file = valid_cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    assert fact_file.exists()
    original_fact = pd.read_parquet(fact_file)
    original_rows = len(original_fact)
    original_fp = compute_fact_trip_fingerprint(original_fact)
    original_mtime = fact_file.stat().st_mtime_ns

    # Step 2: Run 2 with bad source pointing to the same output directory
    invalid_cfg = PipelineConfig(
        project_root=project_root,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=synthetic_invalid_trips_parquet,
        zone_input_path=synthetic_valid_zones_csv,
        fact_trip_output_dir=project_root / "data" / "processed" / "fact_trip",
        dim_zone_output_dir=project_root / "data" / "processed" / "dim_zone",
        metrics_output_dir=project_root / "data" / "processed" / "metrics",
    )

    with pytest.raises(ValidationError):
        run(invalid_cfg)

    # Step 3: Verify Run 1's output is preserved identically
    assert fact_file.exists()
    current_fact = pd.read_parquet(fact_file)
    assert len(current_fact) == original_rows
    assert compute_fact_trip_fingerprint(current_fact) == original_fp
    assert fact_file.stat().st_mtime_ns == original_mtime


# ============================================================================
# TEST 8 — NO SUCCESS MANIFEST
# ============================================================================

def test_no_success_manifest_published_on_failure(failure_config: PipelineConfig):
    """Test 8: A failed run never writes a manifest with pipeline_status == 'success'."""
    manifest_file = failure_config.processed_dir / "manifest_2026-07.json"

    with pytest.raises(ValidationError):
        run(failure_config)

    assert not manifest_file.exists()


# ============================================================================
# TEST 9 — ERROR LOG
# ============================================================================

def test_error_log_recorded_on_validation_failure(
    tmp_path: Path,
    failure_config: PipelineConfig,
):
    """Test 9: Failure produces an ERROR-level log entry identifying the stage and missing column."""
    log_file = tmp_path / "logs" / "failure_test.log"
    test_logger = get_logger(
        name="test_failure_logger",
        log_file=log_file,
        clear_handlers=True,
    )

    with pytest.raises(ValidationError):
        run(failure_config, logger=test_logger)

    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")

    assert "[ERROR]" in log_text
    assert "Pipeline failed during validation" in log_text
    assert "trip_distance" in log_text


# ============================================================================
# TEST 10 — NO RETRY
# ============================================================================

def test_critical_validation_failure_is_not_retried(failure_config: PipelineConfig):
    """Test 10: Validation failures are non-transient and not eligible for retry."""
    err = ValidationError("missing required trip columns: ['trip_distance']")
    assert is_retryable_error(err) is False

    call_count = 0

    def failing_validation_step():
        nonlocal call_count
        call_count += 1
        raise ValidationError("Critical schema failure")

    with pytest.raises(ValidationError):
        retry_call(failing_validation_step, max_attempts=3)

    assert call_count == 1, f"Expected exactly 1 attempt, but got {call_count}"


# ============================================================================
# TEST 11 — OPTIONAL SECOND CRITICAL FAILURE: NEGATIVE TRIP DISTANCE
# ============================================================================

def test_negative_trip_distance_halts_pipeline(
    tmp_path: Path,
    synthetic_valid_zones_csv: Path,
):
    """Test 11: Input containing negative trip distance halts execution before transformation."""
    trip_dir = tmp_path / "raw" / "trips"
    trip_dir.mkdir(parents=True, exist_ok=True)
    trip_file = trip_dir / "negative_distance.parquet"

    trips_with_negative_distance = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00"]),
        "PULocationID": [1],
        "DOLocationID": [2],
        "trip_distance": [-5.0],
    })
    trips_with_negative_distance.to_parquet(trip_file, index=False)

    project_root = tmp_path / "mock_project_neg_dist"
    cfg = PipelineConfig(
        project_root=project_root,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=trip_file,
        zone_input_path=synthetic_valid_zones_csv,
        fact_trip_output_dir=project_root / "data" / "processed" / "fact_trip",
        dim_zone_output_dir=project_root / "data" / "processed" / "dim_zone",
        metrics_output_dir=project_root / "data" / "processed" / "metrics",
    )

    with patch("src.pipeline.transform_trip_data") as mock_transform:
        with pytest.raises(ValidationError) as exc_info:
            run(cfg)

        assert "negative trip_distance" in str(exc_info.value)
        mock_transform.assert_not_called()

    # Verify no output published
    assert not (cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet").exists()

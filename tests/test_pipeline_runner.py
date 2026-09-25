"""Unit and integration tests for Step 8L — End-to-End Pipeline CLI Runner.

Covers:
- Test 1: Valid CLI execution succeeds with exit code 0.
- Test 2: Valid July run produces all required output artifacts (fact, dim, metrics, manifest).
- Test 3: CLI date parsing correctly resolves year, month, and reporting boundaries.
- Test 4: Invalid / malformed CLI date arguments are rejected with non-zero exit code.
- Test 5: Critical validation failure returns non-zero exit code.
- Test 6: Critical validation failure does not publish bad or partial output.
- Test 7: Pipeline execution follows the strict stage order.
- Test 8: Same-period rerun execution remains strictly idempotent.
- Test 9: Pipeline execution logs successful completion.
- Test 10: Pipeline logs the failing stage upon error.
"""

from datetime import datetime
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

from run_pipeline import main, parse_run_date, run
from src.config import PipelineConfig, create_config
from src.output import compute_fact_trip_fingerprint
from src.validate import ValidationError


@pytest.fixture
def synthetic_runner_environment(tmp_path: Path):
    """Fixture providing isolated project root, mock inputs, and output paths."""
    project_root = tmp_path / "runner_project"
    raw_trips_dir = project_root / "data" / "raw" / "trips"
    raw_zones_dir = project_root / "data" / "raw" / "zones"
    raw_trips_dir.mkdir(parents=True, exist_ok=True)
    raw_zones_dir.mkdir(parents=True, exist_ok=True)

    trip_file = raw_trips_dir / "yellow_tripdata_2026-07.parquet"
    zone_file = raw_zones_dir / "taxi_zone_lookup.csv"

    # Minimal valid trips
    trips_df = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00", "2026-07-02 12:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00", "2026-07-02 12:20:00"]),
        "PULocationID": [1, 2],
        "DOLocationID": [3, 4],
        "trip_distance": [2.5, 3.0],
    })
    trips_df.to_parquet(trip_file, index=False)

    # Minimal valid zones (all 265)
    zones_df = pd.DataFrame([
        {"LocationID": i, "Borough": "Manhattan", "Zone": f"Zone_{i}", "service_zone": "Yellow"}
        for i in range(1, 266)
    ])
    zones_df.to_csv(zone_file, index=False)

    return {
        "project_root": project_root,
        "trip_file": trip_file,
        "zone_file": zone_file,
    }


# ============================================================================
# TEST 1 — VALID RUN SUCCEEDS (EXIT CODE 0)
# ============================================================================

def test_valid_run_succeeds_exit_code_zero(synthetic_runner_environment):
    """Test 1: Valid execution returns exit code 0."""
    root = synthetic_runner_environment["project_root"]
    exit_code = main(["--run-date", "2026-07-31", "--project-root", str(root), "--expected-trip-rows", "2"])
    assert exit_code == 0


# ============================================================================
# TEST 2 — VALID RUN PRODUCES ALL OUTPUTS
# ============================================================================

def test_valid_run_produces_all_outputs(synthetic_runner_environment):
    """Test 2: Successful execution creates fact_trip, dim_zone, metrics, and manifest."""
    root = synthetic_runner_environment["project_root"]
    exit_code = main(["--run-date", "2026-07-31", "--project-root", str(root), "--expected-trip-rows", "2"])
    assert exit_code == 0

    fact_file = root / "data" / "processed" / "fact_trip" / "fact_trip_2026-07.parquet"
    zone_file = root / "data" / "processed" / "dim_zone" / "dim_zone_2026-07.parquet"
    metrics_file = root / "data" / "processed" / "metrics" / "metrics_2026-07.csv"
    manifest_file = root / "data" / "processed" / "manifest_2026-07.json"

    assert fact_file.exists()
    assert zone_file.exists()
    assert metrics_file.exists()
    assert manifest_file.exists()


# ============================================================================
# TEST 3 — CLI DATE RESOLVES CORRECT MONTH
# ============================================================================

def test_cli_date_resolves_correct_month():
    """Test 3: --run-date calendar dates resolve to reporting year, month, and month name."""
    year, month, name = parse_run_date("2026-07-31")
    assert year == 2026
    assert month == 7
    assert name == "July 2026"

    year, month, name = parse_run_date("2026-07-01")
    assert year == 2026
    assert month == 7

    year, month, name = parse_run_date("2026-12-15")
    assert year == 2026
    assert month == 12
    assert name == "December 2026"


# ============================================================================
# TEST 4 — INVALID DATE IS REJECTED
# ============================================================================

def test_invalid_date_rejected_with_nonzero_exit():
    """Test 4: Malformed or invalid dates are rejected with exit code 2."""
    assert main(["--run-date", "not-a-date"]) == 2
    assert main(["--run-date", "2026-13-01"]) == 2
    assert main(["--run-date", "2026/07/31"]) == 2
    assert main(["--run-date", "1999-07-31"]) == 2


# ============================================================================
# TEST 5 — CRITICAL VALIDATION FAILURE RETURNS NON-ZERO
# ============================================================================

def test_critical_validation_failure_returns_nonzero(synthetic_runner_environment):
    """Test 5: Critical schema error causes runner to exit with non-zero code."""
    root = synthetic_runner_environment["project_root"]
    trip_file = synthetic_runner_environment["trip_file"]

    # Remove 'trip_distance' to trigger critical validation failure
    invalid_trips = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00"]),
        "PULocationID": [1],
        "DOLocationID": [2],
    })
    invalid_trips.to_parquet(trip_file, index=False)

    exit_code = main(["--run-date", "2026-07-31", "--project-root", str(root)])
    assert exit_code == 1


# ============================================================================
# TEST 6 — CRITICAL VALIDATION FAILURE DOES NOT PUBLISH BAD OUTPUT
# ============================================================================

def test_critical_validation_failure_does_not_publish_output(synthetic_runner_environment):
    """Test 6: In a clean directory, a failed run publishes zero processed outputs."""
    root = synthetic_runner_environment["project_root"]
    trip_file = synthetic_runner_environment["trip_file"]

    # Corrupt trips
    invalid_trips = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00"]),
        "PULocationID": [1],
        "DOLocationID": [2],
    })
    invalid_trips.to_parquet(trip_file, index=False)

    main(["--run-date", "2026-07-31", "--project-root", str(root)])

    fact_file = root / "data" / "processed" / "fact_trip" / "fact_trip_2026-07.parquet"
    manifest_file = root / "data" / "processed" / "manifest_2026-07.json"

    assert not fact_file.exists()
    assert not manifest_file.exists()


# ============================================================================
# TEST 7 — PIPELINE STAGE ORDER IS CORRECT
# ============================================================================

def test_pipeline_stage_order_is_correct(synthetic_runner_environment):
    """Test 7: Verify pipeline executes strictly in order: ingest -> validate -> transform -> model -> metrics -> publish."""
    root = synthetic_runner_environment["project_root"]
    cfg = create_config(root, 2026, 7)

    call_order = []

    with patch("run_pipeline.load_trip_data", side_effect=lambda *args, **kwargs: call_order.append("ingest") or pd.DataFrame()), \
         patch("run_pipeline.load_zone_data", side_effect=lambda *args, **kwargs: pd.DataFrame()), \
         patch("run_pipeline.validate_sources", side_effect=lambda *args, **kwargs: call_order.append("validate") or MagicMock(passed=True, warnings=[])), \
         patch("run_pipeline.transform_trip_data", side_effect=lambda *args, **kwargs: call_order.append("transform") or pd.DataFrame()), \
         patch("run_pipeline.build_dim_zone", side_effect=lambda *args, **kwargs: pd.DataFrame()), \
         patch("run_pipeline.build_fact_trip", side_effect=lambda *args, **kwargs: call_order.append("model") or pd.DataFrame()), \
         patch("run_pipeline.calculate_metrics", side_effect=lambda *args, **kwargs: call_order.append("metrics") or []), \
         patch("run_pipeline.metrics_to_dataframe", side_effect=lambda *args, **kwargs: pd.DataFrame()), \
         patch("run_pipeline.publish_outputs", side_effect=lambda *args, **kwargs: call_order.append("publish") or {}):

        run(cfg)

    assert call_order == ["ingest", "validate", "transform", "model", "metrics", "publish"]


# ============================================================================
# TEST 8 — SAME-PERIOD EXECUTION REMAINS IDEMPOTENT
# ============================================================================

def test_same_period_execution_is_idempotent(synthetic_runner_environment):
    """Test 8: Rerunning main() on the same period maintains row counts and unique trip IDs."""
    root = synthetic_runner_environment["project_root"]

    # Run 1
    assert main(["--run-date", "2026-07-31", "--project-root", str(root), "--expected-trip-rows", "2"]) == 0
    fact_file = root / "data" / "processed" / "fact_trip" / "fact_trip_2026-07.parquet"
    fp1 = compute_fact_trip_fingerprint(pd.read_parquet(fact_file))
    rows1 = len(pd.read_parquet(fact_file))

    # Run 2
    assert main(["--run-date", "2026-07-31", "--project-root", str(root), "--expected-trip-rows", "2"]) == 0
    fp2 = compute_fact_trip_fingerprint(pd.read_parquet(fact_file))
    rows2 = len(pd.read_parquet(fact_file))

    assert rows1 == rows2 == 2
    assert fp1 == fp2


# ============================================================================
# TEST 9 — PIPELINE LOGS SUCCESSFUL COMPLETION
# ============================================================================

def test_pipeline_logs_successful_completion(synthetic_runner_environment):
    """Test 9: Successful execution writes 'Pipeline completed successfully' to logs."""
    root = synthetic_runner_environment["project_root"]
    assert main(["--run-date", "2026-07-31", "--project-root", str(root), "--expected-trip-rows", "2"]) == 0

    log_file = root.parent / "logs" / "pipeline.log"
    default_log = Path("logs/pipeline.log")
    target_log = log_file if log_file.exists() else default_log
    assert target_log.exists()
    content = target_log.read_text(encoding="utf-8")
    assert "Pipeline completed successfully." in content


# ============================================================================
# TEST 10 — PIPELINE LOGS FAILURE STAGE
# ============================================================================

def test_pipeline_logs_failure_stage(synthetic_runner_environment):
    """Test 10: Failed execution logs ERROR identifying the failed stage."""
    root = synthetic_runner_environment["project_root"]
    trip_file = synthetic_runner_environment["trip_file"]

    # Corrupt trips
    invalid_trips = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2026-07-01 10:15:00"]),
        "PULocationID": [1],
        "DOLocationID": [2],
    })
    invalid_trips.to_parquet(trip_file, index=False)

    main(["--run-date", "2026-07-31", "--project-root", str(root)])

    default_log = Path("logs/pipeline.log")
    assert default_log.exists()
    content = default_log.read_text(encoding="utf-8")
    assert "[ERROR]" in content
    assert "validation" in content

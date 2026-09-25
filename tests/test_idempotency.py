"""Unit and integration tests for Step 8J — Idempotency and Safe Rerun Behavior.

Verifies:
1. Same-period rerun rebuilds the same logical output without duplication.
2. No duplicate trip IDs exist after rerun (len == nunique).
3. Fact table logical fingerprint equality across reruns.
4. Dimension table logical fingerprint equality across reruns.
5. Metrics equality and fingerprint match across reruns.
6. Manifest logical values match across reruns.
7. No duplicate output files (*_1, *_2, timestamped duplicates) created.
8. Failed rerun preserves previous successful output.
9. Temporary files are cleaned up after failure.
10. Different reporting periods map to distinct output partitions.
11. Real July 2026 TLC data end-to-end double run idempotency proof.
"""

import json
from pathlib import Path
import pytest
import pandas as pd

from src.config import PipelineConfig, create_config, default_config
from src.ingest import load_trip_data, load_zone_data
from src.metrics import calculate_metrics, metrics_to_dataframe
from src.model import build_dim_zone, build_fact_trip
from src.output import (
    FACT_TRIP_REQUIRED_COLUMNS,
    DIM_ZONE_REQUIRED_COLUMNS,
    OutputWriteError,
    atomic_write_file,
    compute_dataframe_fingerprint,
    compute_dim_zone_fingerprint,
    compute_fact_trip_fingerprint,
    compute_metrics_fingerprint,
    get_dim_zone_output_filename,
    get_fact_trip_output_filename,
    get_logical_run_key,
    get_manifest_output_filename,
    get_metrics_output_filename,
    publish_outputs,
)
from src.transform import transform_trip_data


@pytest.fixture
def synthetic_dim_zone() -> pd.DataFrame:
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
def synthetic_fact_trip() -> pd.DataFrame:
    """Fixture providing a valid synthetic fact_trip table."""
    return pd.DataFrame({
        "trip_id": [101, 102, 103, 104],
        "pickup_datetime": pd.to_datetime([
            "2026-07-01 10:00:00",
            "2026-07-02 12:30:00",
            "2026-07-03 14:15:00",
            "2026-07-04 16:45:00",
        ]),
        "dropoff_datetime": pd.to_datetime([
            "2026-07-01 10:15:00",
            "2026-07-02 12:50:00",
            "2026-07-03 14:35:00",
            "2026-07-04 17:05:00",
        ]),
        "pickup_location_id": [1, 2, 3, 4],
        "dropoff_location_id": [5, 6, 7, 8],
        "trip_distance": [2.5, 3.8, 1.2, 4.0],
        "trip_duration_minutes": [15.0, 20.0, 20.0, 20.0],
        "is_valid_duration": [True, True, True, True],
        "is_valid_location": [True, True, True, True],
        "is_valid_distance": [True, True, True, True],
        "is_valid_trip": [True, True, True, True],
    })


@pytest.fixture
def synthetic_metrics_df() -> pd.DataFrame:
    """Fixture providing a valid 6-metric DataFrame."""
    return pd.DataFrame([
        {
            "Metric Name": "Trip Volume",
            "Type": "Business KPI",
            "Value": 4,
            "Display Value": "4",
            "Unit": "trips",
            "Numerator": 4,
            "Denominator": None,
            "Population": "All trips",
            "Definition": "Total count of trips",
        },
        {
            "Metric Name": "Average Trip Duration",
            "Type": "Business KPI",
            "Value": 18.75,
            "Display Value": "18.75 minutes",
            "Unit": "minutes",
            "Numerator": 75.0,
            "Denominator": 4,
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
            "Denominator": 4,
            "Population": "Valid duration trips",
            "Definition": "50th percentile duration",
        },
        {
            "Metric Name": "Average Trip Distance",
            "Type": "Business KPI",
            "Value": 2.875,
            "Display Value": "2.88 miles",
            "Unit": "miles",
            "Numerator": 11.5,
            "Denominator": 4,
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
            "Denominator": 4,
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
            "Denominator": 4,
            "Population": "All trips",
            "Definition": "Ratio of invalid locations",
        },
    ])


# ============================================================================
# TEST 1 — SAME-PERIOD RERUN
# ============================================================================

def test_same_period_rerun_preserves_row_count(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 1: Executing pipeline output publishing twice does not duplicate rows."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    # Run 1
    publish_outputs(
        fact_trip=synthetic_fact_trip,
        dim_zone=synthetic_dim_zone,
        metrics_data=synthetic_metrics_df,
        config=cfg,
        expected_trip_rows=4,
        expected_zone_rows=265,
    )

    fact_file = cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    zone_file = cfg.dim_zone_output_dir / "dim_zone_2026-07.parquet"
    metrics_file = cfg.metrics_output_dir / "metrics_2026-07.csv"

    assert len(pd.read_parquet(fact_file)) == 4
    assert len(pd.read_parquet(zone_file)) == 265
    assert len(pd.read_csv(metrics_file)) == 6

    # Run 2 (Rerun same logical partition)
    publish_outputs(
        fact_trip=synthetic_fact_trip,
        dim_zone=synthetic_dim_zone,
        metrics_data=synthetic_metrics_df,
        config=cfg,
        expected_trip_rows=4,
        expected_zone_rows=265,
    )

    # Verify no row count doubling
    assert len(pd.read_parquet(fact_file)) == 4
    assert len(pd.read_parquet(zone_file)) == 265
    assert len(pd.read_csv(metrics_file)) == 6


# ============================================================================
# TEST 2 — NO DUPLICATE TRIP IDS
# ============================================================================

def test_no_duplicate_trip_ids_after_rerun(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 2: Verify fact_trip primary key uniqueness is strictly maintained across reruns."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    for _ in range(2):
        publish_outputs(
            fact_trip=synthetic_fact_trip,
            dim_zone=synthetic_dim_zone,
            metrics_data=synthetic_metrics_df,
            config=cfg,
            expected_trip_rows=4,
        )

    fact_file = cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    fact_df = pd.read_parquet(fact_file)
    assert fact_df["trip_id"].nunique() == len(fact_df) == 4


# ============================================================================
# TEST 3 — FACT TABLE FINGERPRINT EQUALITY
# ============================================================================

def test_fact_table_fingerprint_equality(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 3: Fact table logical SHA-256 fingerprint is identical between runs."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)
    fact_file = cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    fp1 = compute_fact_trip_fingerprint(pd.read_parquet(fact_file))

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    fp2 = compute_fact_trip_fingerprint(pd.read_parquet(fact_file))

    assert fp1 == fp2


# ============================================================================
# TEST 4 — DIMENSION FINGERPRINT EQUALITY
# ============================================================================

def test_dimension_fingerprint_equality(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 4: Zone dimension logical SHA-256 fingerprint is identical between runs."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)
    zone_file = cfg.dim_zone_output_dir / "dim_zone_2026-07.parquet"

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    fp1 = compute_dim_zone_fingerprint(pd.read_parquet(zone_file))

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    fp2 = compute_dim_zone_fingerprint(pd.read_parquet(zone_file))

    assert fp1 == fp2


# ============================================================================
# TEST 5 — METRICS EQUALITY
# ============================================================================

def test_metrics_equality(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 5: Metrics values, labels, populations, and fingerprints match across reruns."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)
    metrics_file = cfg.metrics_output_dir / "metrics_2026-07.csv"

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    m1 = pd.read_csv(metrics_file)
    fp1 = compute_metrics_fingerprint(m1)

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    m2 = pd.read_csv(metrics_file)
    fp2 = compute_metrics_fingerprint(m2)

    assert fp1 == fp2
    pd.testing.assert_frame_equal(m1, m2)


# ============================================================================
# TEST 6 — MANIFEST LOGICAL EQUALITY
# ============================================================================

def test_manifest_logical_equality(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 6: Manifest logical values match across reruns while generated_at updates."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    m1 = publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    m2 = publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)

    assert m1["reporting_period"] == m2["reporting_period"] == "2026-07"
    assert m1["fact_trip_rows"] == m2["fact_trip_rows"] == 4
    assert m1["dim_zone_rows"] == m2["dim_zone_rows"] == 265
    assert m1["metric_count"] == m2["metric_count"] == 6
    assert m1["pipeline_status"] == m2["pipeline_status"] == "success"


# ============================================================================
# TEST 7 — NO DUPLICATE OUTPUT FILES
# ============================================================================

def test_no_duplicate_output_files(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 7: Rerunning does not generate secondary files (*_1, *_2, or timestamped duplicates)."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    for _ in range(2):
        publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)

    fact_files = list(cfg.fact_trip_output_dir.glob("*.parquet"))
    assert len(fact_files) == 1
    assert fact_files[0].name == "fact_trip_2026-07.parquet"

    zone_files = list(cfg.dim_zone_output_dir.glob("*.parquet"))
    assert len(zone_files) == 1
    assert zone_files[0].name == "dim_zone_2026-07.parquet"

    metrics_files = list(cfg.metrics_output_dir.glob("*.csv"))
    assert len(metrics_files) == 1
    assert metrics_files[0].name == "metrics_2026-07.csv"


# ============================================================================
# TEST 8 — FAILED RERUN PRESERVES PREVIOUS SUCCESSFUL OUTPUT
# ============================================================================

def test_failed_rerun_preserves_previous_successful_output(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 8: If Run 2 fails mid-write, Run 1's valid outputs remain intact and uncorrupted."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg, expected_trip_rows=4)
    fact_file = cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    original_bytes = fact_file.read_bytes()
    original_fp = compute_fact_trip_fingerprint(pd.read_parquet(fact_file))

    def exploding_writer(path: Path) -> None:
        with open(path, "w") as f:
            f.write("corrupted partial state")
        raise IOError("Simulated disk error during rerun")

    with pytest.raises(OutputWriteError):
        atomic_write_file(fact_file, exploding_writer)

    assert fact_file.exists()
    assert fact_file.read_bytes() == original_bytes
    assert compute_fact_trip_fingerprint(pd.read_parquet(fact_file)) == original_fp


# ============================================================================
# TEST 9 — TEMPORARY FILES CLEANED AFTER FAILURE
# ============================================================================

def test_temporary_files_are_cleaned_after_failure(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 9: Any temporary file generated during an aborted write is cleaned up."""
    cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)
    fact_file = cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"

    def exploding_writer(path: Path) -> None:
        path.write_text("bad data")
        raise RuntimeError("Abort mid-write")

    with pytest.raises(OutputWriteError):
        atomic_write_file(fact_file, exploding_writer)

    temp_files = list(cfg.fact_trip_output_dir.glob(".*.tmp.*"))
    assert len(temp_files) == 0


# ============================================================================
# TEST 10 — DIFFERENT REPORTING PERIODS MAP TO DISTINCT PARTITIONS
# ============================================================================

def test_different_reporting_periods_partition_correctly(
    tmp_path: Path,
    synthetic_fact_trip: pd.DataFrame,
    synthetic_dim_zone: pd.DataFrame,
    synthetic_metrics_df: pd.DataFrame,
):
    """Test 10: Different reporting periods map to separate partitions and coexist without collision."""
    cfg_july = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)
    cfg_aug = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=8)

    assert get_logical_run_key(2026, 7) == "2026-07"
    assert get_logical_run_key(2026, 8) == "2026-08"

    assert get_fact_trip_output_filename(2026, 7) == "fact_trip_2026-07.parquet"
    assert get_fact_trip_output_filename(2026, 8) == "fact_trip_2026-08.parquet"

    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg_july, expected_trip_rows=4)
    publish_outputs(synthetic_fact_trip, synthetic_dim_zone, synthetic_metrics_df, cfg_aug, expected_trip_rows=4)

    fact_july = cfg_july.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    fact_aug = cfg_aug.fact_trip_output_dir / "fact_trip_2026-08.parquet"
    assert fact_july.exists()
    assert fact_aug.exists()

    manifest_july = cfg_july.processed_dir / "manifest_2026-07.json"
    manifest_aug = cfg_aug.processed_dir / "manifest_2026-08.json"
    assert manifest_july.exists()
    assert manifest_aug.exists()


# ============================================================================
# TEST 11 — REAL DATA END-TO-END IDEMPOTENCY PROOF (Section 19)
# ============================================================================

def test_real_tlc_data_idempotency_proof(tmp_path: Path):
    """Test 11 (Section 19): Run pipeline twice on real July 2026 TLC data.

    Proves:
        fact_trip rows = 3,530,063 in both runs
        dim_zone rows = 265 in both runs
        unique trip IDs = 3,530,063 in both runs
        logical fingerprints match exactly
        headline metrics match exactly
    """
    raw_trips = load_trip_data(default_config)
    raw_zones = load_zone_data(default_config)

    transformed_trips = transform_trip_data(raw_trips, raw_zones, default_config)
    dim_zone = build_dim_zone(raw_zones)
    fact_trip = build_fact_trip(transformed_trips)
    metrics_list = calculate_metrics(fact_trip, dim_zone)
    metrics_df = metrics_to_dataframe(metrics_list)

    test_cfg = create_config(project_root=tmp_path, reporting_year=2026, reporting_month=7)

    # Run 1
    publish_outputs(
        fact_trip=fact_trip,
        dim_zone=dim_zone,
        metrics_data=metrics_df,
        config=test_cfg,
        expected_trip_rows=3530063,
        expected_zone_rows=265,
    )

    fact_file = test_cfg.fact_trip_output_dir / "fact_trip_2026-07.parquet"
    zone_file = test_cfg.dim_zone_output_dir / "dim_zone_2026-07.parquet"
    metrics_file = test_cfg.metrics_output_dir / "metrics_2026-07.csv"

    run1_fact = pd.read_parquet(fact_file)
    run1_zone = pd.read_parquet(zone_file)
    run1_metrics = pd.read_csv(metrics_file)

    fp_fact_1 = compute_fact_trip_fingerprint(run1_fact)
    fp_zone_1 = compute_dim_zone_fingerprint(run1_zone)
    fp_metrics_1 = compute_metrics_fingerprint(run1_metrics)

    # Run 2
    publish_outputs(
        fact_trip=fact_trip,
        dim_zone=dim_zone,
        metrics_data=metrics_df,
        config=test_cfg,
        expected_trip_rows=3530063,
        expected_zone_rows=265,
    )

    run2_fact = pd.read_parquet(fact_file)
    run2_zone = pd.read_parquet(zone_file)
    run2_metrics = pd.read_csv(metrics_file)

    fp_fact_2 = compute_fact_trip_fingerprint(run2_fact)
    fp_zone_2 = compute_dim_zone_fingerprint(run2_zone)
    fp_metrics_2 = compute_metrics_fingerprint(run2_metrics)

    # Verification: row counts
    assert len(run1_fact) == 3530063
    assert len(run2_fact) == 3530063
    assert len(run1_zone) == 265
    assert len(run2_zone) == 265
    assert len(run1_metrics) == 6
    assert len(run2_metrics) == 6

    # Verification: uniqueness
    assert run2_fact["trip_id"].nunique() == 3530063
    assert run2_zone["LocationID"].nunique() == 265

    # Verification: fingerprints
    assert fp_fact_1 == fp_fact_2
    assert fp_zone_1 == fp_zone_2
    assert fp_metrics_1 == fp_metrics_2

    # Verification: metric display values
    m_dict1 = dict(zip(run1_metrics["Metric Name"], run1_metrics["Display Value"]))
    m_dict2 = dict(zip(run2_metrics["Metric Name"], run2_metrics["Display Value"]))
    assert m_dict1 == m_dict2
    assert m_dict2["Trip Volume"] == "3,530,063"
    assert m_dict2["Average Trip Duration"] == "17.29 minutes"
    assert m_dict2["Median Trip Duration"] == "14.17 minutes"
    assert m_dict2["Average Trip Distance"] == "5.55 miles"
    assert m_dict2["Invalid Trip Duration Rate"] == "0.0000283%"
    assert m_dict2["Invalid/Unmatched Location Rate"] == "0.00%"

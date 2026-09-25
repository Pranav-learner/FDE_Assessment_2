"""Unit and integration tests for Step 8E — Validation Gates."""

from pathlib import Path
import pytest
import pandas as pd

from src.config import create_config, PipelineConfig
from src.ingest import ingest_sources
from src.validate import (
    validate_sources,
    ValidationResult,
    ValidationError,
)


@pytest.fixture
def mock_config(tmp_path: Path) -> PipelineConfig:
    return create_config(tmp_path, 2026, 7)


@pytest.fixture
def valid_trips_df() -> pd.DataFrame:
    return pd.DataFrame({
        "tpep_pickup_datetime": [
            "2026-07-01 10:00:00",
            "2026-07-15 14:30:00",
        ],
        "tpep_dropoff_datetime": [
            "2026-07-01 10:15:00",
            "2026-07-15 14:50:00",
        ],
        "PULocationID": [1, 2],
        "DOLocationID": [2, 1],
        "trip_distance": [2.5, 4.0],
    })


@pytest.fixture
def valid_zones_df() -> pd.DataFrame:
    return pd.DataFrame({
        "LocationID": [1, 2],
        "Borough": ["Manhattan", "Queens"],
        "Zone": ["Central Park", "JFK Airport"],
        "service_zone": ["Yellow Zone", "Airports"],
    })


def test_valid_synthetic_source_passes(valid_trips_df, valid_zones_df, mock_config):
    """1. Valid synthetic source passes all gates with 0 errors and 0 warnings."""
    result = validate_sources(valid_trips_df, valid_zones_df, mock_config)
    assert result.passed is True
    assert len(result.errors) == 0
    assert len(result.warnings) == 0
    assert result.checks["trip_required_columns"] == "PASS"
    assert result.checks["zone_required_columns"] == "PASS"


def test_missing_required_trip_column_fails(valid_trips_df, valid_zones_df, mock_config):
    """2. Missing required trip column fails validation."""
    df_missing = valid_trips_df.drop(columns=["trip_distance"])
    result = validate_sources(df_missing, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["trip_required_columns"] == "FAIL"
    assert any("trip_distance" in err for err in result.errors)


def test_missing_required_zone_column_fails(valid_trips_df, valid_zones_df, mock_config):
    """3. Missing required zone column fails validation."""
    df_missing = valid_zones_df.drop(columns=["Zone"])
    result = validate_sources(valid_trips_df, df_missing, mock_config)
    assert result.passed is False
    assert result.checks["zone_required_columns"] == "FAIL"
    assert any("Zone" in err for err in result.errors)


def test_duplicate_zone_location_id_fails(valid_trips_df, valid_zones_df, mock_config):
    """4. Duplicate zone LocationID fails validation."""
    dup_zones = pd.concat([valid_zones_df, valid_zones_df.iloc[[0]]], ignore_index=True)
    result = validate_sources(valid_trips_df, dup_zones, mock_config)
    assert result.passed is False
    assert result.checks["zone_location_id_uniqueness"] == "FAIL"
    assert any("duplicate LocationID" in err for err in result.errors)


def test_null_zone_location_id_fails(valid_trips_df, valid_zones_df, mock_config):
    """5. Null zone LocationID fails validation."""
    null_zones = valid_zones_df.copy()
    null_zones.loc[0, "LocationID"] = None
    result = validate_sources(valid_trips_df, null_zones, mock_config)
    assert result.passed is False
    assert result.checks["zone_location_id_completeness"] == "FAIL"
    assert any("null LocationID" in err for err in result.errors)


def test_empty_trip_dataframe_fails(valid_zones_df, mock_config):
    """6. Empty trip DataFrame fails validation."""
    empty_trips = pd.DataFrame(columns=mock_config.trip_required_columns)
    result = validate_sources(empty_trips, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["non_empty_sources"] == "FAIL"


def test_empty_zone_dataframe_fails(valid_trips_df, mock_config):
    """7. Empty zone DataFrame fails validation."""
    empty_zones = pd.DataFrame(columns=mock_config.zone_required_columns)
    result = validate_sources(valid_trips_df, empty_zones, mock_config)
    assert result.passed is False
    assert result.checks["non_empty_sources"] == "FAIL"


def test_missing_pickup_timestamp_fails(valid_trips_df, valid_zones_df, mock_config):
    """8. Missing pickup timestamp fails validation."""
    trips_null_pu = valid_trips_df.copy()
    trips_null_pu.loc[0, "tpep_pickup_datetime"] = None
    result = validate_sources(trips_null_pu, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["timestamp_completeness"] == "FAIL"
    assert any("missing pickup timestamp" in err for err in result.errors)


def test_missing_dropoff_timestamp_fails(valid_trips_df, valid_zones_df, mock_config):
    """9. Missing dropoff timestamp fails validation."""
    trips_null_do = valid_trips_df.copy()
    trips_null_do.loc[0, "tpep_dropoff_datetime"] = None
    result = validate_sources(trips_null_do, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["timestamp_completeness"] == "FAIL"
    assert any("missing dropoff timestamp" in err for err in result.errors)


def test_missing_pickup_location_id_fails(valid_trips_df, valid_zones_df, mock_config):
    """10. Missing pickup LocationID fails validation."""
    trips_null_pu = valid_trips_df.copy()
    trips_null_pu.loc[0, "PULocationID"] = None
    result = validate_sources(trips_null_pu, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["location_completeness"] == "FAIL"
    assert any("missing PULocationID" in err for err in result.errors)


def test_missing_dropoff_location_id_fails(valid_trips_df, valid_zones_df, mock_config):
    """11. Missing dropoff LocationID fails validation."""
    trips_null_do = valid_trips_df.copy()
    trips_null_do.loc[0, "DOLocationID"] = None
    result = validate_sources(trips_null_do, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["location_completeness"] == "FAIL"
    assert any("missing DOLocationID" in err for err in result.errors)


def test_unmatched_pickup_location_id_fails(valid_trips_df, valid_zones_df, mock_config):
    """12. Unmatched pickup LocationID fails validation."""
    trips_unmatched = valid_trips_df.copy()
    trips_unmatched.loc[0, "PULocationID"] = 999  # Not in zones
    result = validate_sources(trips_unmatched, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["location_reference_integrity"] == "FAIL"
    assert any("unmatched PULocationID" in err for err in result.errors)


def test_unmatched_dropoff_location_id_fails(valid_trips_df, valid_zones_df, mock_config):
    """13. Unmatched dropoff LocationID fails validation."""
    trips_unmatched = valid_trips_df.copy()
    trips_unmatched.loc[0, "DOLocationID"] = 888  # Not in zones
    result = validate_sources(trips_unmatched, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["location_reference_integrity"] == "FAIL"
    assert any("unmatched DOLocationID" in err for err in result.errors)


def test_negative_trip_distance_fails(valid_trips_df, valid_zones_df, mock_config):
    """14. Negative trip_distance fails validation."""
    trips_neg = valid_trips_df.copy()
    trips_neg.loc[0, "trip_distance"] = -1.5
    result = validate_sources(trips_neg, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["distance_validity"] == "FAIL"
    assert any("negative trip_distance" in err for err in result.errors)


def test_null_trip_distance_fails(valid_trips_df, valid_zones_df, mock_config):
    """15. Null trip_distance fails validation."""
    trips_null = valid_trips_df.copy()
    trips_null.loc[0, "trip_distance"] = None
    result = validate_sources(trips_null, valid_zones_df, mock_config)
    assert result.passed is False
    assert result.checks["distance_validity"] == "FAIL"
    assert any("null trip_distance" in err for err in result.errors)


def test_invalid_chronology_generates_warning_only(valid_trips_df, valid_zones_df, mock_config):
    """16. Invalid chronology produces a WARNING but passed remains True."""
    trips_chrono = valid_trips_df.copy()
    trips_chrono.loc[0, "tpep_pickup_datetime"] = "2026-07-25 22:24:27"
    trips_chrono.loc[0, "tpep_dropoff_datetime"] = "2026-07-25 22:24:17"  # 10s earlier!

    result = validate_sources(trips_chrono, valid_zones_df, mock_config)
    assert result.passed is True
    assert result.checks["chronology"] == "WARNING"
    assert len(result.warnings) == 1
    assert "chronology" in result.warnings[0]


def test_validation_does_not_mutate_inputs(valid_trips_df, valid_zones_df, mock_config):
    """17. Confirm validation does not mutate input DataFrames."""
    trips_before = valid_trips_df.copy(deep=True)
    zones_before = valid_zones_df.copy(deep=True)

    validate_sources(valid_trips_df, valid_zones_df, mock_config)

    pd.testing.assert_frame_equal(valid_trips_df, trips_before)
    pd.testing.assert_frame_equal(valid_zones_df, zones_before)


def test_real_tlc_data_validation():
    """18. Integration test against actual TLC source data."""
    project_root = Path(__file__).resolve().parent.parent
    config = create_config(project_root, 2026, 7)
    trips, zones = ingest_sources(config)

    result = validate_sources(trips, zones, config)

    # Must pass overall
    assert result.passed is True
    assert len(result.errors) == 0

    # Exactly 1 chronology warning
    assert result.checks["chronology"] == "WARNING"
    assert len(result.warnings) == 1
    assert result.evidence["invalid_chronology_count"] == 1

    # Check evidence metrics
    assert result.evidence["trip_row_count"] == 3_530_109
    assert result.evidence["zone_row_count"] == 265
    assert result.evidence["missing_pickup_timestamps"] == 0
    assert result.evidence["missing_dropoff_timestamps"] == 0
    assert result.evidence["missing_pickup_locations"] == 0
    assert result.evidence["missing_dropoff_locations"] == 0
    assert result.evidence["unmatched_pickup_locations"] == 0
    assert result.evidence["unmatched_dropoff_locations"] == 0
    assert result.evidence["invalid_distance_count"] == 0

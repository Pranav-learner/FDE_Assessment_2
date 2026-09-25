"""Unit tests for Step 8C — Configuration layer."""

from datetime import datetime
from pathlib import Path
import pytest

from src.config import (
    PipelineConfig,
    create_config,
    get_reporting_period,
    validate_config,
)


@pytest.fixture
def dummy_root(tmp_path: Path) -> Path:
    return tmp_path / "mock_project"


def test_july_reporting_period():
    """1. July reporting period boundaries: 2026-07-01 to 2026-08-01."""
    start, end = get_reporting_period(2026, 7)
    assert start == datetime(2026, 7, 1, 0, 0, 0)
    assert end == datetime(2026, 8, 1, 0, 0, 0)


def test_december_rollover_reporting_period():
    """2. December rollover: 2026-12-01 to 2027-01-01."""
    start, end = get_reporting_period(2026, 12)
    assert start == datetime(2026, 12, 1, 0, 0, 0)
    assert end == datetime(2027, 1, 1, 0, 0, 0)


def test_create_config_trip_input_path(dummy_root: Path):
    """3. create_config() builds the correct July trip path."""
    cfg = create_config(dummy_root, 2026, 7)
    expected = dummy_root / "data" / "raw" / "trips" / "yellow_tripdata_2026-07.parquet"
    assert cfg.trip_input_path == expected


def test_create_config_zone_input_path(dummy_root: Path):
    """4. create_config() builds the correct zone path."""
    cfg = create_config(dummy_root, 2026, 7)
    expected = dummy_root / "data" / "raw" / "zones" / "taxi_zone_lookup.csv"
    assert cfg.zone_input_path == expected


def test_create_config_output_directories(dummy_root: Path):
    """5. create_config() builds the correct output directories."""
    cfg = create_config(dummy_root, 2026, 7)
    assert cfg.fact_trip_output_dir == dummy_root / "data" / "processed" / "fact_trip"
    assert cfg.dim_zone_output_dir == dummy_root / "data" / "processed" / "dim_zone"
    assert cfg.metrics_output_dir == dummy_root / "data" / "processed" / "metrics"


def test_max_attempts_default(dummy_root: Path):
    """6. max_attempts defaults to 3."""
    cfg = create_config(dummy_root, 2026, 7)
    assert cfg.max_attempts == 3


def test_required_trip_columns(dummy_root: Path):
    """7. required trip columns are correct."""
    cfg = create_config(dummy_root, 2026, 7)
    expected_trip_cols = (
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
    )
    assert cfg.trip_required_columns == expected_trip_cols


def test_required_zone_columns(dummy_root: Path):
    """8. required zone columns are correct."""
    cfg = create_config(dummy_root, 2026, 7)
    expected_zone_cols = (
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    )
    assert cfg.zone_required_columns == expected_zone_cols


def test_invalid_reporting_month_raises(dummy_root: Path):
    """9. invalid reporting month raises ValueError."""
    with pytest.raises(ValueError, match="reporting_month must be between 1 and 12"):
        get_reporting_period(2026, 13)

    with pytest.raises(ValueError, match="reporting_month must be between 1 and 12"):
        get_reporting_period(2026, 0)

    with pytest.raises(ValueError, match="Invalid reporting_month"):
        create_config(dummy_root, 2026, 13)


def test_max_attempts_less_than_one_raises(dummy_root: Path):
    """10. max_attempts < 1 raises ValueError."""
    invalid_cfg = PipelineConfig(
        project_root=dummy_root,
        reporting_year=2026,
        reporting_month=7,
        max_attempts=0,
        trip_input_path=dummy_root / "data" / "raw" / "trips" / "trip.parquet",
        zone_input_path=dummy_root / "data" / "raw" / "zones" / "zones.csv",
    )
    with pytest.raises(ValueError, match="Invalid max_attempts"):
        validate_config(invalid_cfg)


def test_missing_trip_input_path_raises(dummy_root: Path):
    """11. missing trip_input_path raises ValueError."""
    invalid_cfg = PipelineConfig(
        project_root=dummy_root,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=None,
        zone_input_path=dummy_root / "data" / "raw" / "zones" / "zones.csv",
    )
    with pytest.raises(ValueError, match="trip_input_path is missing or None"):
        validate_config(invalid_cfg)


def test_missing_zone_input_path_raises(dummy_root: Path):
    """12. missing zone_input_path raises ValueError."""
    invalid_cfg = PipelineConfig(
        project_root=dummy_root,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=dummy_root / "data" / "raw" / "trips" / "trip.parquet",
        zone_input_path=None,
    )
    with pytest.raises(ValueError, match="zone_input_path is missing or None"):
        validate_config(invalid_cfg)

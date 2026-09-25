"""Unit tests for Step 8D — Ingestion layer."""

from pathlib import Path
import pytest
import pandas as pd

from src.config import create_config, PipelineConfig
from src.ingest import load_trip_data, load_zone_data, ingest_sources


@pytest.fixture(scope="module")
def project_config() -> PipelineConfig:
    project_root = Path(__file__).resolve().parent.parent
    return create_config(project_root, 2026, 7)


def test_load_trip_data_success(project_config: PipelineConfig):
    """1. Trip source loads successfully."""
    trips = load_trip_data(project_config)
    assert isinstance(trips, pd.DataFrame)
    assert not trips.empty


def test_load_zone_data_success(project_config: PipelineConfig):
    """2. Zone source loads successfully."""
    zones = load_zone_data(project_config)
    assert isinstance(zones, pd.DataFrame)
    assert not zones.empty


def test_trip_row_count(project_config: PipelineConfig):
    """3. Trip row count is 3,530,109."""
    trips = load_trip_data(project_config)
    assert len(trips) == 3_530_109


def test_zone_row_count(project_config: PipelineConfig):
    """4. Zone row count is 265."""
    zones = load_zone_data(project_config)
    assert len(zones) == 265


def test_expected_trip_columns(project_config: PipelineConfig):
    """5. Expected raw trip columns are present."""
    trips = load_trip_data(project_config)
    expected_trip_cols = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
    ]
    for col in expected_trip_cols:
        assert col in trips.columns, f"Expected column '{col}' missing from raw trips"


def test_expected_zone_columns(project_config: PipelineConfig):
    """6. Expected zone columns are present."""
    zones = load_zone_data(project_config)
    expected_zone_cols = [
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    ]
    for col in expected_zone_cols:
        assert col in zones.columns, f"Expected column '{col}' missing from raw zones"


def test_ingestion_does_not_filter_july(project_config: PipelineConfig):
    """7. Confirm ingestion does NOT perform July filtering (preserves all 3,530,109 rows)."""
    trips, _ = ingest_sources(project_config)
    assert len(trips) == 3_530_109
    assert len(trips) != 3_530_063


def test_missing_trip_source_raises(tmp_path: Path):
    """8. Test missing trip source behavior (raises FileNotFoundError with path)."""
    missing_path = tmp_path / "nonexistent_trips.parquet"
    cfg = PipelineConfig(
        project_root=tmp_path,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=missing_path,
        zone_input_path=tmp_path / "zones.csv",
    )
    with pytest.raises(FileNotFoundError, match="Failed to load trip source"):
        load_trip_data(cfg)


def test_missing_zone_source_raises(tmp_path: Path):
    """9. Test missing zone source behavior (raises FileNotFoundError with path)."""
    missing_path = tmp_path / "nonexistent_zones.csv"
    cfg = PipelineConfig(
        project_root=tmp_path,
        reporting_year=2026,
        reporting_month=7,
        trip_input_path=tmp_path / "trips.parquet",
        zone_input_path=missing_path,
    )
    with pytest.raises(FileNotFoundError, match="Failed to load zone source"):
        load_zone_data(cfg)

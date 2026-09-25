"""Unit and integration tests for Step 8G — Transformation Layer."""

from datetime import datetime
from pathlib import Path
import pytest
import pandas as pd

from src.config import create_config, PipelineConfig
from src.ingest import ingest_sources
from src.transform import (
    filter_reporting_period,
    add_trip_duration,
    add_validation_flags,
    transform_trip_data,
)


@pytest.fixture
def sample_zones() -> pd.DataFrame:
    return pd.DataFrame({
        "LocationID": [1, 2, 3],
        "Borough": ["Manhattan", "Queens", "Brooklyn"],
        "Zone": ["Central Park", "JFK Airport", "Williamsburg"],
        "service_zone": ["Yellow Zone", "Airports", "Boro Zone"],
    })


@pytest.fixture
def sample_trips() -> pd.DataFrame:
    return pd.DataFrame({
        "tpep_pickup_datetime": [
            "2026-06-30 23:59:59",  # Before July
            "2026-07-01 00:00:00",  # Exact boundary start
            "2026-07-15 12:00:00",  # Valid mid-July
            "2026-07-31 23:59:59",  # Valid end-of-July
            "2026-08-01 00:00:00",  # Exact boundary end (excluded)
            "2026-08-05 10:00:00",  # After July
        ],
        "tpep_dropoff_datetime": [
            "2026-07-01 00:15:00",
            "2026-07-01 00:20:00",
            "2026-07-15 12:30:00",
            "2026-08-01 00:15:00",  # Dropoff in August (valid!)
            "2026-08-01 00:30:00",
            "2026-08-05 10:25:00",
        ],
        "PULocationID": [1, 1, 2, 3, 1, 2],
        "DOLocationID": [2, 2, 1, 2, 2, 1],
        "trip_distance": [1.5, 2.0, 3.5, 5.0, 1.0, 4.0],
    })


def test_july_filtering(sample_trips):
    """1. Only pickup timestamps in [July 1, August 1) are retained."""
    filtered = filter_reporting_period(
        sample_trips,
        start_datetime="2026-07-01 00:00:00",
        end_datetime="2026-08-01 00:00:00",
    )
    assert len(filtered) == 3
    for dt in pd.to_datetime(filtered["tpep_pickup_datetime"]):
        assert dt.month == 7 and dt.year == 2026


def test_boundary_behavior(sample_trips):
    """2. 2026-07-01 00:00:00 is included; 2026-08-01 00:00:00 is excluded."""
    filtered = filter_reporting_period(
        sample_trips,
        start_datetime="2026-07-01 00:00:00",
        end_datetime="2026-08-01 00:00:00",
    )
    pickups = list(filtered["tpep_pickup_datetime"])
    assert "2026-07-01 00:00:00" in pickups
    assert "2026-07-31 23:59:59" in pickups
    assert "2026-08-01 00:00:00" not in pickups
    assert "2026-06-30 23:59:59" not in pickups


def test_dropoff_in_next_month_preserved(sample_trips):
    """Confirm a July 31 pickup with August 1 dropoff is preserved."""
    filtered = filter_reporting_period(
        sample_trips,
        start_datetime="2026-07-01 00:00:00",
        end_datetime="2026-08-01 00:00:00",
    )
    late_trip = filtered[filtered["tpep_pickup_datetime"] == "2026-07-31 23:59:59"]
    assert len(late_trip) == 1
    assert late_trip["tpep_dropoff_datetime"].iloc[0] == "2026-08-01 00:15:00"


def test_duration_calculation():
    """3. Duration calculation: 10:00 to 10:30 yields 30.0 minutes."""
    trips = pd.DataFrame({
        "tpep_pickup_datetime": ["2026-07-10 10:00:00"],
        "tpep_dropoff_datetime": ["2026-07-10 10:30:00"],
    })
    res = add_trip_duration(trips)
    assert "trip_duration_minutes" in res.columns
    assert res["trip_duration_minutes"].iloc[0] == pytest.approx(30.0)


def test_invalid_chronology_flag_and_retention(sample_zones):
    """4. Invalid chronology: dropoff before pickup flags is_valid_duration=False but preserves row."""
    trips = pd.DataFrame({
        "tpep_pickup_datetime": ["2026-07-25 22:24:27"],
        "tpep_dropoff_datetime": ["2026-07-25 22:24:17"],  # 10s earlier
        "PULocationID": [1],
        "DOLocationID": [2],
        "trip_distance": [1.0],
    })
    with_duration = add_trip_duration(trips)
    res = add_validation_flags(with_duration, sample_zones)

    assert len(res) == 1
    assert res["trip_duration_minutes"].iloc[0] < 0
    assert res["is_valid_duration"].iloc[0] == False
    assert res["is_valid_trip"].iloc[0] == False


def test_location_validity_flags(sample_zones):
    """5. Valid zone IDs produce is_valid_location=True; unmatched produce False."""
    trips = pd.DataFrame({
        "tpep_pickup_datetime": ["2026-07-10 10:00:00", "2026-07-10 10:00:00"],
        "tpep_dropoff_datetime": ["2026-07-10 10:15:00", "2026-07-10 10:15:00"],
        "PULocationID": [1, 999],  # 999 is unmatched
        "DOLocationID": [2, 2],
        "trip_distance": [2.0, 2.0],
    })
    res = add_validation_flags(trips, sample_zones)
    assert res.loc[0, "is_valid_location"] == True
    assert res.loc[1, "is_valid_location"] == False


def test_distance_validation_flags(sample_zones):
    """6. Distance logic: positive/zero -> valid; negative/null -> invalid."""
    trips = pd.DataFrame({
        "tpep_pickup_datetime": ["2026-07-10 10:00:00"] * 4,
        "tpep_dropoff_datetime": ["2026-07-10 10:15:00"] * 4,
        "PULocationID": [1] * 4,
        "DOLocationID": [2] * 4,
        "trip_distance": [2.5, 0.0, -1.0, None],
    })
    res = add_validation_flags(trips, sample_zones)
    assert res.loc[0, "is_valid_distance"] == True  # positive
    assert res.loc[1, "is_valid_distance"] == True  # zero is valid!
    assert res.loc[2, "is_valid_distance"] == False  # negative
    assert res.loc[3, "is_valid_distance"] == False  # null


def test_overall_validity_composite(sample_zones):
    """7. is_valid_trip requires duration & location & distance."""
    trips = pd.DataFrame({
        "tpep_pickup_datetime": [
            "2026-07-10 10:00:00",
            "2026-07-10 10:30:00",  # invalid duration
            "2026-07-10 10:00:00",  # invalid location
            "2026-07-10 10:00:00",  # invalid distance
        ],
        "tpep_dropoff_datetime": [
            "2026-07-10 10:15:00",
            "2026-07-10 10:20:00",
            "2026-07-10 10:15:00",
            "2026-07-10 10:15:00",
        ],
        "PULocationID": [1, 1, 999, 1],
        "DOLocationID": [2, 2, 2, 2],
        "trip_distance": [2.0, 2.0, 2.0, -0.5],
    })
    with_duration = add_trip_duration(trips)
    res = add_validation_flags(with_duration, sample_zones)

    assert res.loc[0, "is_valid_trip"] == True
    assert res.loc[1, "is_valid_trip"] == False
    assert res.loc[2, "is_valid_trip"] == False
    assert res.loc[3, "is_valid_trip"] == False


def test_input_immutability(sample_trips, sample_zones):
    """8. Confirm transformation functions do not mutate input DataFrames."""
    trips_before = sample_trips.copy(deep=True)
    zones_before = sample_zones.copy(deep=True)

    cfg = create_config(Path(__file__).resolve().parent.parent, 2026, 7)
    transform_trip_data(sample_trips, sample_zones, cfg)

    pd.testing.assert_frame_equal(sample_trips, trips_before)
    pd.testing.assert_frame_equal(sample_zones, zones_before)


def test_real_tlc_transformation():
    """9. Integration check with actual TLC July data."""
    project_root = Path(__file__).resolve().parent.parent
    cfg = create_config(project_root, 2026, 7)
    trips, zones = ingest_sources(cfg)

    transformed = transform_trip_data(trips, zones, cfg)

    # 3,530,063 in reporting period
    assert len(transformed) == 3_530_063

    # Quality flag breakdown
    assert transformed["is_valid_duration"].sum() == 3_530_062
    assert (~transformed["is_valid_duration"]).sum() == 1
    assert (~transformed["is_valid_location"]).sum() == 0
    assert (~transformed["is_valid_distance"]).sum() == 0
    assert (~transformed["is_valid_trip"]).sum() == 1

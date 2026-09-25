"""Unit and integration tests for Step 8G — Modeling Layer."""

from pathlib import Path
import pytest
import pandas as pd

from src.config import create_config
from src.ingest import ingest_sources
from src.transform import transform_trip_data
from src.model import build_dim_zone, build_fact_trip, build_business_model


@pytest.fixture
def sample_transformed():
    return pd.DataFrame({
        "tpep_pickup_datetime": [
            "2026-07-01 10:00:00",
            "2026-07-25 22:24:27",  # Invalid duration record
        ],
        "tpep_dropoff_datetime": [
            "2026-07-01 10:15:00",
            "2026-07-25 22:24:17",
        ],
        "PULocationID": [1, 2],
        "DOLocationID": [2, 1],
        "trip_distance": [2.5, 1.0],
        "trip_duration_minutes": [15.0, -0.166667],
        "is_valid_duration": [True, False],
        "is_valid_location": [True, True],
        "is_valid_distance": [True, True],
        "is_valid_trip": [True, False],
    })


@pytest.fixture
def sample_zones():
    return pd.DataFrame({
        "LocationID": [1, 2],
        "Borough": ["Manhattan", "Queens"],
        "Zone": ["Central Park", "JFK Airport"],
        "service_zone": ["Yellow Zone", "Airports"],
    })


def test_build_dim_zone_structure(sample_zones):
    """Confirm dim_zone contains expected columns and non-null unique IDs."""
    dim = build_dim_zone(sample_zones)
    expected_cols = ["LocationID", "Borough", "Zone", "service_zone"]
    assert list(dim.columns) == expected_cols
    assert dim["LocationID"].isna().sum() == 0
    assert dim["LocationID"].is_unique


def test_build_fact_trip_structure(sample_transformed):
    """Confirm fact_trip generates unique trip_id and expected business fields."""
    fact = build_fact_trip(sample_transformed)
    expected_cols = [
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "trip_duration_minutes",
        "is_valid_duration",
        "is_valid_location",
        "is_valid_distance",
        "is_valid_trip",
    ]
    assert list(fact.columns) == expected_cols
    assert len(fact) == 2
    assert fact["trip_id"].tolist() == [1, 2]
    assert fact["trip_id"].is_unique
    assert fact["trip_id"].isna().sum() == 0


def test_model_immutability(sample_transformed, sample_zones):
    """Confirm modeling does not mutate input DataFrames."""
    trips_before = sample_transformed.copy(deep=True)
    zones_before = sample_zones.copy(deep=True)

    build_business_model(sample_transformed, sample_zones)

    pd.testing.assert_frame_equal(sample_transformed, trips_before)
    pd.testing.assert_frame_equal(sample_zones, zones_before)


def test_real_tlc_business_model():
    """Integration test against actual TLC source data verifying all 12 model requirements."""
    project_root = Path(__file__).resolve().parent.parent
    cfg = create_config(project_root, 2026, 7)
    trips, zones = ingest_sources(cfg)
    transformed = transform_trip_data(trips, zones, cfg)

    fact_trip, dim_zone = build_business_model(transformed, zones)

    # 1. dim_zone row count is 265
    assert len(dim_zone) == 265

    # 2. dim_zone LocationID uniqueness
    assert dim_zone["LocationID"].is_unique

    # 3. dim_zone LocationID non-null
    assert dim_zone["LocationID"].isna().sum() == 0

    # 4. fact_trip July row count is 3,530,063
    assert len(fact_trip) == 3_530_063

    # 5. fact_trip trip_id uniqueness
    assert fact_trip["trip_id"].nunique() == 3_530_063

    # 6. fact_trip trip_id non-null
    assert fact_trip["trip_id"].isna().sum() == 0

    # 7. exactly one row per modeled trip
    assert len(fact_trip) == len(transformed)

    # 8. fact_trip expected business fields
    expected_cols = [
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "trip_duration_minutes",
        "is_valid_duration",
        "is_valid_location",
        "is_valid_distance",
        "is_valid_trip",
    ]
    assert list(fact_trip.columns) == expected_cols

    # 9. One invalid duration record remains present
    invalid_durations = fact_trip[fact_trip["is_valid_duration"] == False]
    assert len(invalid_durations) == 1
    assert invalid_durations["trip_duration_minutes"].iloc[0] < 0

    # 10. Quality flag counts
    assert (fact_trip["is_valid_duration"] == False).sum() == 1
    assert (fact_trip["is_valid_location"] == False).sum() == 0
    assert (fact_trip["is_valid_distance"] == False).sum() == 0
    assert (fact_trip["is_valid_trip"] == False).sum() == 1

    # 11. Referential integrity: foreign keys match dim_zone.LocationID
    valid_zone_ids = set(dim_zone["LocationID"])
    unmatched_pu = set(fact_trip["pickup_location_id"]) - valid_zone_ids
    unmatched_do = set(fact_trip["dropoff_location_id"]) - valid_zone_ids
    assert len(unmatched_pu) == 0
    assert len(unmatched_do) == 0

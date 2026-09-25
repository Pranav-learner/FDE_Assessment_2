"""Unit and integration tests for Step 8H — Metrics Layer."""

from pathlib import Path
import pytest
import pandas as pd

from src.config import create_config
from src.ingest import ingest_sources
from src.transform import transform_trip_data
from src.model import build_business_model
from src.metrics import (
    calculate_trip_volume,
    calculate_average_trip_duration,
    calculate_median_trip_duration,
    calculate_average_trip_distance,
    calculate_invalid_trip_duration_rate,
    calculate_invalid_location_rate,
    calculate_trip_volume_by_pickup_zone,
    calculate_metrics,
    MetricComputationError,
)


@pytest.fixture
def base_synthetic_fact():
    return pd.DataFrame({
        "trip_id": [1, 2, 3],
        "pickup_datetime": pd.to_datetime(["2026-07-01 10:00:00", "2026-07-01 11:00:00", "2026-07-01 12:00:00"]),
        "dropoff_datetime": pd.to_datetime(["2026-07-01 10:10:00", "2026-07-01 11:20:00", "2026-07-01 12:30:00"]),
        "pickup_location_id": [1, 2, 1],
        "dropoff_location_id": [2, 1, 2],
        "trip_distance": [2.0, 4.0, 6.0],
        "trip_duration_minutes": [10.0, 20.0, 30.0],
        "is_valid_duration": [True, True, True],
        "is_valid_location": [True, True, True],
        "is_valid_distance": [True, True, True],
        "is_valid_trip": [True, True, True],
    })


def test_calculate_trip_volume(base_synthetic_fact):
    """TEST 1: Small synthetic fact_trip with 3 rows yields volume = 3."""
    res = calculate_trip_volume(base_synthetic_fact)
    assert res.value == 3
    assert res.unit == "trips"
    assert res.metric_name == "Trip Volume"


def test_calculate_average_duration(base_synthetic_fact):
    """TEST 2: Durations 10, 20, 30 yield average = 20.0."""
    res = calculate_average_trip_duration(base_synthetic_fact)
    assert res.value == pytest.approx(20.0)
    assert res.denominator == 3


def test_invalid_duration_excluded():
    """TEST 3: Durations 10, 20, -5 with True, True, False yields average = 15.0, not 8.33."""
    fact = pd.DataFrame({
        "trip_id": [1, 2, 3],
        "trip_duration_minutes": [10.0, 20.0, -5.0],
        "is_valid_duration": [True, True, False],
    })
    res = calculate_average_trip_duration(fact)
    assert res.value == pytest.approx(15.0)
    assert res.value != pytest.approx(8.333333, rel=1e-2)
    assert res.denominator == 2


def test_calculate_median_duration(base_synthetic_fact):
    """TEST 4: Valid durations 10, 20, 30 yield median = 20.0."""
    res = calculate_median_trip_duration(base_synthetic_fact)
    assert res.value == pytest.approx(20.0)


def test_median_with_invalid_record():
    """TEST 5: Durations 10, 20, -100 with True, True, False yields median = 15.0."""
    fact = pd.DataFrame({
        "trip_id": [1, 2, 3],
        "trip_duration_minutes": [10.0, 20.0, -100.0],
        "is_valid_duration": [True, True, False],
    })
    res = calculate_median_trip_duration(fact)
    assert res.value == pytest.approx(15.0)
    assert res.denominator == 2


def test_calculate_average_distance(base_synthetic_fact):
    """TEST 6: Distances 2, 4, 6 yield average = 4.0."""
    res = calculate_average_trip_distance(base_synthetic_fact)
    assert res.value == pytest.approx(4.0)
    assert res.denominator == 3


def test_invalid_distance_excluded():
    """TEST 7: Distances 2, 4, -6 with True, True, False yields average = 3.0."""
    fact = pd.DataFrame({
        "trip_id": [1, 2, 3],
        "trip_distance": [2.0, 4.0, -6.0],
        "is_valid_distance": [True, True, False],
    })
    res = calculate_average_trip_distance(fact)
    assert res.value == pytest.approx(3.0)
    assert res.denominator == 2


def test_invalid_duration_rate():
    """TEST 8: 3 total trips, 1 invalid duration yields 33.3333%."""
    fact = pd.DataFrame({
        "trip_id": [1, 2, 3],
        "is_valid_duration": [True, True, False],
    })
    res = calculate_invalid_trip_duration_rate(fact)
    assert res.value == pytest.approx(100.0 / 3.0)
    assert res.numerator == 1
    assert res.denominator == 3


def test_invalid_location_rate():
    """TEST 9: 4 total trips, 1 invalid location yields 25.0%."""
    fact = pd.DataFrame({
        "trip_id": [1, 2, 3, 4],
        "is_valid_location": [True, True, True, False],
    })
    res = calculate_invalid_location_rate(fact)
    assert res.value == pytest.approx(25.0)
    assert res.numerator == 1
    assert res.denominator == 4


def test_zero_invalid_location_rate(base_synthetic_fact):
    """TEST 10: 3 total trips, 0 invalid locations yields 0.0%."""
    res = calculate_invalid_location_rate(base_synthetic_fact)
    assert res.value == pytest.approx(0.0)
    assert res.numerator == 0
    assert res.denominator == 3


def test_empty_dataframe_raises():
    """TEST 11: Empty DataFrame raises MetricComputationError."""
    empty_df = pd.DataFrame(columns=["trip_id", "is_valid_duration"])
    with pytest.raises(MetricComputationError, match="empty fact_trip"):
        calculate_trip_volume(empty_df)


def test_missing_column_raises():
    """TEST 12: Missing required column raises MetricComputationError."""
    fact = pd.DataFrame({"some_other_col": [1, 2, 3]})
    with pytest.raises(MetricComputationError, match="missing required metric column"):
        calculate_average_trip_duration(fact)


def test_supporting_pickup_zone_breakdown(base_synthetic_fact):
    """TEST 14: Supporting breakdown correctly groups by pickup zone."""
    dim_zone = pd.DataFrame({
        "LocationID": [1, 2],
        "Borough": ["Manhattan", "Queens"],
        "Zone": ["Central Park", "JFK Airport"],
        "service_zone": ["Yellow Zone", "Airports"],
    })
    breakdown = calculate_trip_volume_by_pickup_zone(base_synthetic_fact, dim_zone)
    assert len(breakdown) == 2
    # Location 1 had 2 trips, Location 2 had 1 trip
    assert breakdown.iloc[0]["Zone"] == "Central Park"
    assert breakdown.iloc[0]["trip_volume"] == 2
    assert breakdown.iloc[1]["Zone"] == "JFK Airport"
    assert breakdown.iloc[1]["trip_volume"] == 1


def test_real_tlc_metrics():
    """TEST 13: Full metrics integration run against real July 2026 data."""
    project_root = Path(__file__).resolve().parent.parent
    cfg = create_config(project_root, 2026, 7)
    trips, zones = ingest_sources(cfg)
    transformed = transform_trip_data(trips, zones, cfg)
    fact_trip, dim_zone = build_business_model(transformed, zones)

    metrics = calculate_metrics(fact_trip, dim_zone)
    metrics_map = {m.metric_name: m for m in metrics}

    # 1. Trip Volume
    vol = metrics_map["Trip Volume"]
    assert vol.value == 3_530_063
    assert vol.unit == "trips"

    # 2. Average Trip Duration
    avg_dur = metrics_map["Average Trip Duration"]
    assert avg_dur.denominator == 3_530_062
    assert avg_dur.value == pytest.approx(17.29, abs=0.05)

    # 3. Median Trip Duration
    med_dur = metrics_map["Median Trip Duration"]
    assert med_dur.denominator == 3_530_062
    assert med_dur.value == pytest.approx(14.17, abs=0.05)

    # 4. Average Trip Distance
    avg_dist = metrics_map["Average Trip Distance"]
    assert avg_dist.denominator == 3_530_063
    assert avg_dist.value == pytest.approx(5.55, abs=0.05)

    # 5. Invalid Trip Duration Rate
    inv_dur = metrics_map["Invalid Trip Duration Rate"]
    assert inv_dur.numerator == 1
    assert inv_dur.denominator == 3_530_063
    assert inv_dur.value == pytest.approx(0.0000283281, rel=1e-4)

    # 6. Invalid / Unmatched Location Rate
    inv_loc = metrics_map["Invalid/Unmatched Location Rate"]
    assert inv_loc.numerator == 0
    assert inv_loc.denominator == 3_530_063
    assert inv_loc.value == 0.0

    # Supporting breakdown check
    zone_breakdown = calculate_trip_volume_by_pickup_zone(fact_trip, dim_zone)
    top_zones = list(zone_breakdown["Zone"].head(5))
    expected_top = ["Midtown Center", "JFK Airport", "Upper East Side South", "Upper East Side North", "Penn Station/Madison Sq West"]
    assert top_zones == expected_top

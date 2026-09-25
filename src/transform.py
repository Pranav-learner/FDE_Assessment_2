"""Transformation layer for NYC Taxi Operations Intelligence Pipeline.

Applies reporting-period filtering, derives operational features (duration),
and attaches record-level data quality validation flags without mutating raw inputs.
"""

from datetime import datetime
from typing import Set
import pandas as pd

from .config import PipelineConfig, get_reporting_period


def filter_reporting_period(
    trips: pd.DataFrame,
    start_datetime: datetime | str,
    end_datetime: datetime | str,
) -> pd.DataFrame:
    """Filter trips to the reporting period based on pickup datetime.

    The boundary convention is:
        pickup_datetime >= start_datetime AND pickup_datetime < end_datetime

    Dropoff timestamp is explicitly not filtered; trips initiating in the reporting
    period but finishing afterwards remain valid reporting records.

    Args:
        trips: Raw trip records DataFrame.
        start_datetime: Lower inclusive bound.
        end_datetime: Upper exclusive bound.

    Returns:
        pd.DataFrame: New DataFrame containing only in-period trip records.
    """
    start = pd.Timestamp(start_datetime)
    end = pd.Timestamp(end_datetime)

    pickup_series = pd.to_datetime(trips["tpep_pickup_datetime"])
    mask = (pickup_series >= start) & (pickup_series < end)
    return trips[mask].copy()


def add_trip_duration(trips: pd.DataFrame) -> pd.DataFrame:
    """Derive trip_duration_minutes from pickup and dropoff timestamps.

    Formula:
        (dropoff_datetime - pickup_datetime).total_seconds() / 60

    Preserves original timestamps and does not alter or clamp negative durations.

    Args:
        trips: Trips DataFrame containing pickup and dropoff timestamps.

    Returns:
        pd.DataFrame: Copy of DataFrame with trip_duration_minutes added.
    """
    df = trips.copy()
    pickup = pd.to_datetime(df["tpep_pickup_datetime"])
    dropoff = pd.to_datetime(df["tpep_dropoff_datetime"])

    df["trip_duration_minutes"] = (dropoff - pickup).dt.total_seconds() / 60.0
    return df


def add_validation_flags(
    trips: pd.DataFrame,
    zones: pd.DataFrame,
) -> pd.DataFrame:
    """Derive record-level business data quality validation flags.

    Quality dimensions:
        - is_valid_duration: dropoff_datetime >= pickup_datetime
        - is_valid_location: PULocationID and DOLocationID both exist in zone LocationID
        - is_valid_distance: trip_distance is not null and >= 0 (zero distance is allowed)
        - is_valid_trip: composite flag (duration & location & distance)

    Args:
        trips: Trips DataFrame.
        zones: Taxi zone reference DataFrame.

    Returns:
        pd.DataFrame: Copy of DataFrame with validation flags added.
    """
    df = trips.copy()
    pickup = pd.to_datetime(df["tpep_pickup_datetime"])
    dropoff = pd.to_datetime(df["tpep_dropoff_datetime"])

    # 1. Duration validity
    df["is_valid_duration"] = pickup.notna() & dropoff.notna() & (dropoff >= pickup)

    # 2. Location validity
    valid_zone_ids: Set[int] = set(zones["LocationID"].dropna().astype(int))
    pu_valid = df["PULocationID"].isin(valid_zone_ids)
    do_valid = df["DOLocationID"].isin(valid_zone_ids)
    df["is_valid_location"] = pu_valid & do_valid

    # 3. Distance validity (zero is allowed; negative or null is invalid)
    df["is_valid_distance"] = df["trip_distance"].notna() & (df["trip_distance"] >= 0)

    # 4. Overall trip validity
    df["is_valid_trip"] = (
        df["is_valid_duration"] & df["is_valid_location"] & df["is_valid_distance"]
    )

    return df


def transform_trip_data(
    trips: pd.DataFrame,
    zones: pd.DataFrame,
    config: PipelineConfig,
) -> pd.DataFrame:
    """End-to-end transformation coordinator for the reporting dataset.

    Executes:
        1. Reporting period filtering based on config.reporting_year/month
        2. Trip duration calculation
        3. Quality validation flag generation

    Args:
        trips: Raw ingested trips DataFrame.
        zones: Raw ingested zones DataFrame.
        config: Pipeline configuration containing reporting boundaries.

    Returns:
        pd.DataFrame: Transformed reporting period trips.
    """
    start_dt, end_dt = get_reporting_period(config.reporting_year, config.reporting_month)
    filtered = filter_reporting_period(trips, start_dt, end_dt)
    with_duration = add_trip_duration(filtered)
    transformed = add_validation_flags(with_duration, zones)
    return transformed

"""Dimensional modeling layer for NYC Taxi Operations Intelligence Pipeline.

Builds the Class 7 business entities:
    - fact_trip: Operational trip facts with synthetic surrogate key and quality flags.
    - dim_zone: Taxi zone geographic reference dimension.
"""

from typing import Tuple
import pandas as pd


def build_dim_zone(zones: pd.DataFrame) -> pd.DataFrame:
    """Build the dim_zone dimension table.

    Selects the 4 core geographic reference attributes and ensures LocationID
    is a non-null, unique integer identifier.

    Args:
        zones: Raw taxi zone lookup DataFrame.

    Returns:
        pd.DataFrame: Cleaned dim_zone table with 4 columns.
    """
    dim_cols = ["LocationID", "Borough", "Zone", "service_zone"]
    dim_zone = zones[dim_cols].copy().reset_index(drop=True)

    # Ensure typed identifier
    dim_zone["LocationID"] = dim_zone["LocationID"].astype(int)
    return dim_zone


def build_fact_trip(transformed_trips: pd.DataFrame) -> pd.DataFrame:
    """Build the fact_trip fact table for the reporting dataset.

    Generates a deterministic synthetic surrogate key (trip_id), maps
    source columns to business terminology, and preserves operational
    features and record-level validation flags.

    Args:
        transformed_trips: Transformed trips DataFrame containing derived
            duration and validation flags.

    Returns:
        pd.DataFrame: Structured fact_trip table with exactly one row per trip.
    """
    df = transformed_trips.reset_index(drop=True)

    fact = pd.DataFrame({
        "trip_id": range(1, len(df) + 1),
        "pickup_datetime": pd.to_datetime(df["tpep_pickup_datetime"]),
        "dropoff_datetime": pd.to_datetime(df["tpep_dropoff_datetime"]),
        "pickup_location_id": df["PULocationID"].astype(int),
        "dropoff_location_id": df["DOLocationID"].astype(int),
        "trip_distance": df["trip_distance"].astype(float),
        "trip_duration_minutes": df["trip_duration_minutes"].astype(float),
        "is_valid_duration": df["is_valid_duration"].astype(bool),
        "is_valid_location": df["is_valid_location"].astype(bool),
        "is_valid_distance": df["is_valid_distance"].astype(bool),
        "is_valid_trip": df["is_valid_trip"].astype(bool),
    })

    return fact


def build_business_model(
    transformed_trips: pd.DataFrame,
    zones: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build both fact and dimension entities for the business model.

    Args:
        transformed_trips: Transformed trips DataFrame.
        zones: Zone lookup DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (fact_trip, dim_zone).
    """
    dim_zone = build_dim_zone(zones)
    fact_trip = build_fact_trip(transformed_trips)
    return fact_trip, dim_zone

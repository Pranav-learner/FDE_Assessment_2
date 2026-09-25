"""Ingestion layer for NYC Taxi Operations Intelligence Pipeline.

Loads raw Parquet trip data and CSV zone reference data without performing
any business transformations, filtering, or schema validation.
"""

from pathlib import Path
import pandas as pd

from .config import PipelineConfig


def load_trip_data(config: PipelineConfig) -> pd.DataFrame:
    """Load the raw yellow taxi trips Parquet file as-is.

    Preserves all rows and columns without applying reporting-period filtering,
    deduplication, column selection, or transformations.

    Args:
        config: Centralized pipeline configuration containing trip_input_path.

    Returns:
        pd.DataFrame: Unmodified raw trip records.

    Raises:
        FileNotFoundError: If the trip input path does not exist or is None.
        RuntimeError: If reading the Parquet file fails.
    """
    if config.trip_input_path is None or not config.trip_input_path.exists():
        raise FileNotFoundError(
            f"Failed to load trip source: {config.trip_input_path}"
        )

    try:
        return pd.read_parquet(config.trip_input_path)
    except Exception as e:
        raise RuntimeError(
            f"Failed to load trip source: {config.trip_input_path}. Error: {e}"
        ) from e


def load_zone_data(config: PipelineConfig) -> pd.DataFrame:
    """Load the raw taxi zone lookup CSV file as-is.

    Preserves all rows and columns without applying deduplication,
    validation, or geographic transformations.

    Args:
        config: Centralized pipeline configuration containing zone_input_path.

    Returns:
        pd.DataFrame: Unmodified raw taxi zone records.

    Raises:
        FileNotFoundError: If the zone input path does not exist or is None.
        RuntimeError: If reading the CSV file fails.
    """
    if config.zone_input_path is None or not config.zone_input_path.exists():
        raise FileNotFoundError(
            f"Failed to load zone source: {config.zone_input_path}"
        )

    try:
        return pd.read_csv(config.zone_input_path)
    except Exception as e:
        raise RuntimeError(
            f"Failed to load zone source: {config.zone_input_path}. Error: {e}"
        ) from e


def ingest_sources(
    config: PipelineConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ingest both trip and zone raw sources without modifications.

    Loads the raw sources into memory and returns them as a tuple of DataFrames
    for downstream validation and transformation stages.

    Args:
        config: Centralized pipeline configuration.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: (raw_trips_df, raw_zones_df).
    """
    trips_df = load_trip_data(config)
    zones_df = load_zone_data(config)
    return trips_df, zones_df

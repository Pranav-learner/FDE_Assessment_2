"""Centralized configuration layer for the NYC Taxi Operations Intelligence Pipeline."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration container for the NYC Taxi Operations Pipeline.

    Encapsulates project directory layout, reporting boundaries, retry
    policies, and schema constraints to prevent hardcoded values across modules.
    """

    # Project paths
    project_root: Path

    # Reporting period
    reporting_year: int
    reporting_month: int

    # Execution tracking (identifies when the pipeline run occurred)
    run_date: str = "2026-09-22"

    # Retry configuration
    max_attempts: int = 3

    # Input paths
    trip_input_path: Path | None = None
    zone_input_path: Path | None = None

    # Output paths
    fact_trip_output_dir: Path | None = None
    dim_zone_output_dir: Path | None = None
    metrics_output_dir: Path | None = None

    # Required source columns
    trip_required_columns: tuple[str, ...] = (
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
    )

    zone_required_columns: tuple[str, ...] = (
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    )

    @property
    def reporting_period(self) -> tuple[datetime, datetime]:
        """Convenience property returning (start, end) datetimes for the configured reporting period."""
        return get_reporting_period(self.reporting_year, self.reporting_month)

    @property
    def processed_dir(self) -> Path:
        """Convenience property returning the base directory for processed outputs."""
        return self.project_root / "data" / "processed"


def get_reporting_period(
    year: int,
    month: int,
) -> tuple[datetime, datetime]:
    """Calculate the start and end datetime bounds for a reporting month.

    The reporting period convention is:
        pickup_datetime >= start AND pickup_datetime < end

    Args:
        year: The reporting year (e.g. 2026).
        month: The reporting month (1-12).

    Returns:
        tuple of (start_datetime, end_datetime) where start is the first day of the
        reporting month (00:00:00) and end is the first day of the following month (00:00:00).

    Raises:
        ValueError: If month is not between 1 and 12, or year is < 2000.
    """
    if month < 1 or month > 12:
        raise ValueError(f"reporting_month must be between 1 and 12, got: {month}")

    if year < 2000:
        raise ValueError(f"reporting_year must be >= 2000, got: {year}")

    start = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0)

    return start, end


def validate_config(config: PipelineConfig) -> None:
    """Validate configuration integrity and parameter boundaries.

    Ensures reporting year, month, retry parameters, and required input paths
    are specified and within valid operational ranges.

    Args:
        config: The PipelineConfig instance to validate.

    Raises:
        ValueError: If any configuration value violates operational requirements.
    """
    if config.reporting_month < 1 or config.reporting_month > 12:
        raise ValueError(
            f"Invalid reporting_month: {config.reporting_month}. Must be between 1 and 12."
        )

    if config.reporting_year < 2000:
        raise ValueError(
            f"Invalid reporting_year: {config.reporting_year}. Must be >= 2000."
        )

    if config.max_attempts < 1:
        raise ValueError(
            f"Invalid max_attempts: {config.max_attempts}. Must be at least 1."
        )

    if config.trip_input_path is None:
        raise ValueError("trip_input_path is missing or None.")

    if config.zone_input_path is None:
        raise ValueError("zone_input_path is missing or None.")


def create_config(
    project_root: Path,
    reporting_year: int,
    reporting_month: int,
) -> PipelineConfig:
    """Factory function to construct and validate a PipelineConfig instance.

    Dynamically resolves raw input file paths and processed output directory
    paths relative to the provided project root.

    Args:
        project_root: Base path of the project.
        reporting_year: Target reporting year (e.g. 2026).
        reporting_month: Target reporting month (1-12, e.g. 7).

    Returns:
        A validated PipelineConfig instance.
    """
    trip_input_path = (
        project_root
        / "data"
        / "raw"
        / "trips"
        / f"yellow_tripdata_{reporting_year}-{reporting_month:02d}.parquet"
    )

    zone_input_path = (
        project_root
        / "data"
        / "raw"
        / "zones"
        / "taxi_zone_lookup.csv"
    )

    fact_trip_output_dir = (
        project_root
        / "data"
        / "processed"
        / "fact_trip"
    )

    dim_zone_output_dir = (
        project_root
        / "data"
        / "processed"
        / "dim_zone"
    )

    metrics_output_dir = (
        project_root
        / "data"
        / "processed"
        / "metrics"
    )

    cfg = PipelineConfig(
        project_root=project_root,
        reporting_year=reporting_year,
        reporting_month=reporting_month,
        trip_input_path=trip_input_path,
        zone_input_path=zone_input_path,
        fact_trip_output_dir=fact_trip_output_dir,
        dim_zone_output_dir=dim_zone_output_dir,
        metrics_output_dir=metrics_output_dir,
    )

    validate_config(cfg)
    return cfg


# Default configuration instance for July 2026
_DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent.parent
default_config = create_config(
    project_root=_DEFAULT_PROJECT_ROOT,
    reporting_year=2026,
    reporting_month=7,
)
config = default_config
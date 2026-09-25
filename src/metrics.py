"""Metrics calculation layer for NYC Taxi Operations Intelligence Pipeline.

Calculates the 4 business KPIs and 2 data quality KPIs defined in Class 7
from fact_trip and provides supporting analytical breakdowns.
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
import pandas as pd


class MetricComputationError(ValueError):
    """Raised when metric computation fails due to missing inputs or schema."""
    pass


@dataclass(frozen=True)
class MetricResult:
    """Structured metric result container with full population and formula traceability.

    Attributes:
        metric_name: Human-readable name of the metric.
        metric_type: Classification ('Business KPI' or 'Data Quality KPI').
        value: Exact unrounded numerical value.
        unit: Measurement unit ('trips', 'minutes', 'miles', 'percent').
        display_value: Formatted representation for reporting.
        population: Description of the record slice used in calculation.
        definition: Formal business definition.
        numerator: Count or sum representing the numerator (if applicable).
        denominator: Count or sum representing the denominator (if applicable).
    """

    metric_name: str
    metric_type: str
    value: float | int
    unit: str
    display_value: str
    population: str
    definition: str
    numerator: Optional[float | int] = None
    denominator: Optional[float | int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert metric result to standard dictionary."""
        return asdict(self)


def _validate_fact_trip_input(fact_trip: pd.DataFrame, required_cols: List[str]) -> None:
    """Validate that fact_trip is non-empty and contains required columns."""
    if not isinstance(fact_trip, pd.DataFrame):
        raise MetricComputationError("Input fact_trip must be a pandas DataFrame.")
    if len(fact_trip) == 0:
        raise MetricComputationError("Cannot compute metrics on empty fact_trip DataFrame (0 rows).")

    missing = [c for c in required_cols if c not in fact_trip.columns]
    if missing:
        raise MetricComputationError(f"fact_trip is missing required metric column(s): {missing}")


def calculate_trip_volume(fact_trip: pd.DataFrame) -> MetricResult:
    """Calculate Trip Volume (Business KPI 1).

    Definition: Total number of July reporting-period taxi trips in fact_trip.
    Population: All July 2026 trip records in fact_trip.
    Formula: COUNT(trip_id)
    """
    _validate_fact_trip_input(fact_trip, ["trip_id"])
    volume = int(fact_trip["trip_id"].count())

    return MetricResult(
        metric_name="Trip Volume",
        metric_type="Business KPI",
        value=volume,
        unit="trips",
        display_value=f"{volume:,}",
        population="All July 2026 trip records in fact_trip",
        definition="Total number of taxi trips in the reporting period",
        numerator=None,
        denominator=None,
    )


def calculate_average_trip_duration(fact_trip: pd.DataFrame) -> MetricResult:
    """Calculate Average Trip Duration (Business KPI 2).

    Definition: Mean duration among trips with valid chronology.
    Population: Trips where is_valid_duration == True.
    Formula: SUM(valid trip_duration_minutes) / COUNT(valid trip_duration_minutes)
    """
    _validate_fact_trip_input(fact_trip, ["trip_duration_minutes", "is_valid_duration"])
    valid_mask = fact_trip["is_valid_duration"] == True
    valid_trips = fact_trip[valid_mask]
    denominator = int(len(valid_trips))

    if denominator == 0:
        raise MetricComputationError("Cannot compute Average Trip Duration: 0 trips with valid duration.")

    duration_sum = float(valid_trips["trip_duration_minutes"].sum())
    avg_duration = duration_sum / denominator

    return MetricResult(
        metric_name="Average Trip Duration",
        metric_type="Business KPI",
        value=avg_duration,
        unit="minutes",
        display_value=f"{avg_duration:.2f} minutes",
        population="Trips with valid duration (is_valid_duration == True)",
        definition="Mean duration in minutes among trips with valid chronology",
        numerator=duration_sum,
        denominator=denominator,
    )


def calculate_median_trip_duration(fact_trip: pd.DataFrame) -> MetricResult:
    """Calculate Median Trip Duration (Business KPI 3).

    Definition: Median duration among trips with valid chronology (outlier-resistant).
    Population: Trips where is_valid_duration == True.
    Formula: MEDIAN(valid trip_duration_minutes)
    """
    _validate_fact_trip_input(fact_trip, ["trip_duration_minutes", "is_valid_duration"])
    valid_mask = fact_trip["is_valid_duration"] == True
    valid_trips = fact_trip[valid_mask]
    denominator = int(len(valid_trips))

    if denominator == 0:
        raise MetricComputationError("Cannot compute Median Trip Duration: 0 trips with valid duration.")

    median_duration = float(valid_trips["trip_duration_minutes"].median())

    return MetricResult(
        metric_name="Median Trip Duration",
        metric_type="Business KPI",
        value=median_duration,
        unit="minutes",
        display_value=f"{median_duration:.2f} minutes",
        population="Trips with valid duration (is_valid_duration == True)",
        definition="Median duration in minutes among trips with valid chronology",
        numerator=None,
        denominator=denominator,
    )


def calculate_average_trip_distance(fact_trip: pd.DataFrame) -> MetricResult:
    """Calculate Average Trip Distance (Business KPI 4).

    Definition: Mean distance among trips with valid non-null distance.
    Population: Trips where is_valid_distance == True (>= 0).
    Formula: SUM(valid trip_distance) / COUNT(valid trip_distance)
    """
    _validate_fact_trip_input(fact_trip, ["trip_distance", "is_valid_distance"])
    valid_mask = fact_trip["is_valid_distance"] == True
    valid_trips = fact_trip[valid_mask]
    denominator = int(len(valid_trips))

    if denominator == 0:
        raise MetricComputationError("Cannot compute Average Trip Distance: 0 trips with valid distance.")

    distance_sum = float(valid_trips["trip_distance"].sum())
    avg_distance = distance_sum / denominator

    return MetricResult(
        metric_name="Average Trip Distance",
        metric_type="Business KPI",
        value=avg_distance,
        unit="miles",
        display_value=f"{avg_distance:.2f} miles",
        population="Trips with valid distance (is_valid_distance == True)",
        definition="Mean distance in miles among trips with valid, non-negative distance",
        numerator=distance_sum,
        denominator=denominator,
    )


def calculate_invalid_trip_duration_rate(fact_trip: pd.DataFrame) -> MetricResult:
    """Calculate Invalid Trip Duration Rate (Data Quality KPI 5).

    Definition: Percentage of trips with invalid chronology (dropoff < pickup).
    Population: Trips with both required timestamps available in fact_trip.
    Formula: (invalid duration count / duration denominator) * 100
    """
    _validate_fact_trip_input(fact_trip, ["is_valid_duration"])
    denominator = int(len(fact_trip))
    if denominator == 0:
        raise MetricComputationError("Cannot compute Invalid Trip Duration Rate: 0 trips.")

    numerator = int((fact_trip["is_valid_duration"] == False).sum())
    rate = (numerator / denominator) * 100.0

    return MetricResult(
        metric_name="Invalid Trip Duration Rate",
        metric_type="Data Quality KPI",
        value=rate,
        unit="percent",
        display_value=f"{rate:.7f}%",
        population="All July reporting trips with required timestamps",
        definition="Percentage of trips with invalid chronology (dropoff < pickup)",
        numerator=numerator,
        denominator=denominator,
    )


def calculate_invalid_location_rate(fact_trip: pd.DataFrame) -> MetricResult:
    """Calculate Invalid / Unmatched Location Rate (Data Quality KPI 6).

    Definition: Percentage of reporting trips with unmapped location IDs.
    Population: All July reporting-period trip records in fact_trip.
    Formula: (invalid location count / all July trips) * 100
    """
    _validate_fact_trip_input(fact_trip, ["is_valid_location"])
    denominator = int(len(fact_trip))
    if denominator == 0:
        raise MetricComputationError("Cannot compute Invalid Location Rate: 0 trips.")

    numerator = int((fact_trip["is_valid_location"] == False).sum())
    rate = (numerator / denominator) * 100.0

    return MetricResult(
        metric_name="Invalid/Unmatched Location Rate",
        metric_type="Data Quality KPI",
        value=rate,
        unit="percent",
        display_value=f"{rate:.2f}%",
        population="All July 2026 trip records in fact_trip",
        definition="Percentage of trips with unmapped pickup or dropoff location IDs",
        numerator=numerator,
        denominator=denominator,
    )


def calculate_trip_volume_by_pickup_zone(
    fact_trip: pd.DataFrame,
    dim_zone: pd.DataFrame,
) -> pd.DataFrame:
    """Compute supporting analytical breakdown of trip volume by pickup zone.

    Note: This is an analytical breakdown, NOT a headline KPI.

    Args:
        fact_trip: Operational fact table.
        dim_zone: Geographic zone dimension table.

    Returns:
        pd.DataFrame: Breakdown with LocationID, Borough, Zone, and trip_volume,
        sorted descending by trip_volume.
    """
    _validate_fact_trip_input(fact_trip, ["pickup_location_id"])
    if not isinstance(dim_zone, pd.DataFrame) or len(dim_zone) == 0:
        raise MetricComputationError("dim_zone must be a non-empty DataFrame.")

    merged = fact_trip.merge(
        dim_zone,
        left_on="pickup_location_id",
        right_on="LocationID",
        how="left",
    )

    breakdown = (
        merged.groupby(["LocationID", "Borough", "Zone"], as_index=False)
        .size()
        .rename(columns={"size": "trip_volume"})
        .sort_values(by="trip_volume", ascending=False)
        .reset_index(drop=True)
    )

    return breakdown


def calculate_metrics(
    fact_trip: pd.DataFrame,
    dim_zone: Optional[pd.DataFrame] = None,
) -> List[MetricResult]:
    """Calculate all six headline business and data-quality KPIs.

    Args:
        fact_trip: Operational fact table.
        dim_zone: Optional zone dimension table for supporting breakdowns.

    Returns:
        List[MetricResult]: Ordered list of the six headline metric results.
    """
    return [
        calculate_trip_volume(fact_trip),
        calculate_average_trip_duration(fact_trip),
        calculate_median_trip_duration(fact_trip),
        calculate_average_trip_distance(fact_trip),
        calculate_invalid_trip_duration_rate(fact_trip),
        calculate_invalid_location_rate(fact_trip),
    ]


def metrics_to_dataframe(metrics: List[MetricResult]) -> pd.DataFrame:
    """Format list of MetricResults into a presentation/evidence DataFrame."""
    records = []
    for m in metrics:
        records.append({
            "Metric Name": m.metric_name,
            "Type": m.metric_type,
            "Value": m.value,
            "Display Value": m.display_value,
            "Unit": m.unit,
            "Numerator": m.numerator,
            "Denominator": m.denominator,
            "Population": m.population,
            "Definition": m.definition,
        })
    return pd.DataFrame(records)

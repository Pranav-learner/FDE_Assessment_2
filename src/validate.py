"""Validation gates layer for NYC Taxi Operations Intelligence Pipeline.

Enforces structural, completeness, reference integrity, and business rule gates
immediately following ingestion and prior to transformation/modeling.
"""

from dataclasses import dataclass, field
from typing import Any
import pandas as pd

from .config import PipelineConfig


class PipelineExecutionError(Exception):
    """Base exception for pipeline execution failures."""
    pass


class ValidationError(PipelineExecutionError):
    """Raised when critical validation fails and the pipeline execution must halt."""

    def __init__(self, message: str, result: "ValidationResult | None" = None) -> None:
        super().__init__(message)
        self.result = result


@dataclass
class ValidationResult:
    """Structured evidence and status from executing pipeline validation gates.

    Attributes:
        passed: True if all critical gates passed; False if any critical gate failed.
        errors: List of human-readable critical error descriptions.
        warnings: List of non-critical data quality anomalies (e.g. chronology).
        checks: Mapping of check name to outcome ('PASS', 'FAIL', 'WARNING', 'SKIPPED').
        evidence: Dictionary of observational metrics collected across gates.
    """

    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks: dict[str, str] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)


def check_non_empty_sources(
    trips: pd.DataFrame,
    zones: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 9 / 1: Verify that ingested trip and zone DataFrames are non-empty."""
    trip_count = len(trips)
    zone_count = len(zones)
    result.evidence["trip_row_count"] = trip_count
    result.evidence["zone_row_count"] = zone_count

    if trip_count == 0 or zone_count == 0:
        result.passed = False
        result.checks["non_empty_sources"] = "FAIL"
        if trip_count == 0:
            result.errors.append("Critical failure: Ingested trip DataFrame is empty (0 rows).")
        if zone_count == 0:
            result.errors.append("Critical failure: Ingested zone DataFrame is empty (0 rows).")
    else:
        result.checks["non_empty_sources"] = "PASS"


def check_required_trip_columns(
    trips: pd.DataFrame,
    config: PipelineConfig,
    result: ValidationResult,
) -> None:
    """Gate 1: Verify that all required trip columns exist in the DataFrame."""
    missing = [col for col in config.trip_required_columns if col not in trips.columns]
    if missing:
        result.passed = False
        result.checks["trip_required_columns"] = "FAIL"
        result.errors.append(f"Validation failed: missing required trip columns: {missing}")
    else:
        result.checks["trip_required_columns"] = "PASS"


def check_required_zone_columns(
    zones: pd.DataFrame,
    config: PipelineConfig,
    result: ValidationResult,
) -> None:
    """Gate 2: Verify that all required zone columns exist in the DataFrame."""
    missing = [col for col in config.zone_required_columns if col not in zones.columns]
    if missing:
        result.passed = False
        result.checks["zone_required_columns"] = "FAIL"
        result.errors.append(f"Validation failed: missing required zone columns: {missing}")
    else:
        result.checks["zone_required_columns"] = "PASS"


def check_zone_location_id_completeness(
    zones: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 3A: Verify that LocationID in zone reference contains no null values."""
    if "LocationID" not in zones.columns:
        result.checks["zone_location_id_completeness"] = "SKIPPED"
        return

    null_count = int(zones["LocationID"].isna().sum())
    result.evidence["zone_null_location_id_count"] = null_count

    if null_count > 0:
        result.passed = False
        result.checks["zone_location_id_completeness"] = "FAIL"
        result.errors.append(
            f"Critical failure: Zone reference contains {null_count} null LocationID value(s)."
        )
    else:
        result.checks["zone_location_id_completeness"] = "PASS"


def check_zone_location_id_uniqueness(
    zones: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 3B: Verify that LocationID in zone reference contains no duplicate values."""
    if "LocationID" not in zones.columns:
        result.checks["zone_location_id_uniqueness"] = "SKIPPED"
        return

    dup_count = int(zones["LocationID"].duplicated().sum())
    result.evidence["zone_duplicate_location_id_count"] = dup_count

    if dup_count > 0:
        result.passed = False
        result.checks["zone_location_id_uniqueness"] = "FAIL"
        result.errors.append(
            f"Critical failure: Zone reference contains {dup_count} duplicate LocationID value(s)."
        )
    else:
        result.checks["zone_location_id_uniqueness"] = "PASS"


def check_timestamp_completeness(
    trips: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 4: Verify that pickup and dropoff timestamps are non-null."""
    cols_to_check = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    missing_cols = [c for c in cols_to_check if c not in trips.columns]
    if missing_cols:
        result.checks["timestamp_completeness"] = "SKIPPED"
        return

    missing_pickup = int(trips["tpep_pickup_datetime"].isna().sum())
    missing_dropoff = int(trips["tpep_dropoff_datetime"].isna().sum())
    result.evidence["missing_pickup_timestamps"] = missing_pickup
    result.evidence["missing_dropoff_timestamps"] = missing_dropoff

    if missing_pickup > 0 or missing_dropoff > 0:
        result.passed = False
        result.checks["timestamp_completeness"] = "FAIL"
        if missing_pickup > 0:
            result.errors.append(
                f"Critical failure: Trip source contains {missing_pickup} missing pickup timestamp(s)."
            )
        if missing_dropoff > 0:
            result.errors.append(
                f"Critical failure: Trip source contains {missing_dropoff} missing dropoff timestamp(s)."
            )
    else:
        result.checks["timestamp_completeness"] = "PASS"


def check_location_completeness(
    trips: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 5: Verify that pickup and dropoff LocationIDs are non-null."""
    cols_to_check = ["PULocationID", "DOLocationID"]
    missing_cols = [c for c in cols_to_check if c not in trips.columns]
    if missing_cols:
        result.checks["location_completeness"] = "SKIPPED"
        return

    missing_pu = int(trips["PULocationID"].isna().sum())
    missing_do = int(trips["DOLocationID"].isna().sum())
    result.evidence["missing_pickup_locations"] = missing_pu
    result.evidence["missing_dropoff_locations"] = missing_do

    if missing_pu > 0 or missing_do > 0:
        result.passed = False
        result.checks["location_completeness"] = "FAIL"
        if missing_pu > 0:
            result.errors.append(
                f"Critical failure: Trip source contains {missing_pu} missing PULocationID value(s)."
            )
        if missing_do > 0:
            result.errors.append(
                f"Critical failure: Trip source contains {missing_do} missing DOLocationID value(s)."
            )
    else:
        result.checks["location_completeness"] = "PASS"


def check_location_reference_integrity(
    trips: pd.DataFrame,
    zones: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 6: Verify that trip PULocationID and DOLocationID exist in zone reference."""
    if "LocationID" not in zones.columns or "PULocationID" not in trips.columns or "DOLocationID" not in trips.columns:
        result.checks["location_reference_integrity"] = "SKIPPED"
        return

    valid_zone_ids = set(zones["LocationID"].dropna())
    pu_ids = set(trips["PULocationID"].dropna())
    do_ids = set(trips["DOLocationID"].dropna())

    unmatched_pu = pu_ids - valid_zone_ids
    unmatched_do = do_ids - valid_zone_ids

    result.evidence["unmatched_pickup_locations"] = len(unmatched_pu)
    result.evidence["unmatched_dropoff_locations"] = len(unmatched_do)

    if unmatched_pu or unmatched_do:
        result.passed = False
        result.checks["location_reference_integrity"] = "FAIL"
        if unmatched_pu:
            sample = sorted(list(unmatched_pu))[:5]
            result.errors.append(
                f"Critical failure: Found {len(unmatched_pu)} unmatched PULocationID value(s). Examples: {sample}"
            )
        if unmatched_do:
            sample = sorted(list(unmatched_do))[:5]
            result.errors.append(
                f"Critical failure: Found {len(unmatched_do)} unmatched DOLocationID value(s). Examples: {sample}"
            )
    else:
        result.checks["location_reference_integrity"] = "PASS"


def check_distance_validity(
    trips: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 7: Verify that trip_distance is non-null and >= 0 (zero distance is allowed)."""
    if "trip_distance" not in trips.columns:
        result.checks["distance_validity"] = "SKIPPED"
        return

    null_count = int(trips["trip_distance"].isna().sum())
    negative_count = int((trips["trip_distance"] < 0).sum())
    total_invalid = null_count + negative_count
    result.evidence["invalid_distance_count"] = total_invalid

    if total_invalid > 0:
        result.passed = False
        result.checks["distance_validity"] = "FAIL"
        if null_count > 0:
            result.errors.append(
                f"Critical failure: Trip source contains {null_count} null trip_distance value(s)."
            )
        if negative_count > 0:
            result.errors.append(
                f"Critical failure: Trip source contains {negative_count} negative trip_distance value(s)."
            )
    else:
        result.checks["distance_validity"] = "PASS"


def check_chronology_anomalies(
    trips: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Gate 8: Inspect pickup and dropoff timestamp chronology.

    Dropoff before pickup is recorded as a data-quality WARNING.
    It does NOT fail the pipeline or mutate the source DataFrame.
    """
    cols_to_check = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    missing_cols = [c for c in cols_to_check if c not in trips.columns]
    if missing_cols:
        result.checks["chronology"] = "SKIPPED"
        return

    # Observational comparison without mutating the input DataFrame
    pickup_dt = pd.to_datetime(trips["tpep_pickup_datetime"])
    dropoff_dt = pd.to_datetime(trips["tpep_dropoff_datetime"])
    invalid_chronology = int((dropoff_dt < pickup_dt).sum())

    result.evidence["invalid_chronology_count"] = invalid_chronology

    if invalid_chronology > 0:
        result.checks["chronology"] = "WARNING"
        result.warnings.append(
            f"{invalid_chronology} trip(s) have invalid pickup/dropoff chronology (dropoff < pickup)."
        )
    else:
        result.checks["chronology"] = "PASS"


def validate_sources(
    trips: pd.DataFrame,
    zones: pd.DataFrame,
    config: PipelineConfig,
) -> ValidationResult:
    """Execute all validation gates across ingested trip and zone datasets.

    Gates are observational and strictly preserve the input DataFrames.
    If critical errors exist, result.passed is False.
    Chronology anomalies are captured as warnings with result.passed remaining True.

    Args:
        trips: Ingested raw trip DataFrame.
        zones: Ingested raw zone reference DataFrame.
        config: Centralized pipeline configuration.

    Returns:
        ValidationResult: Detailed validation outcome, check dictionary, warnings, and errors.
    """
    result = ValidationResult()

    # 1. Non-empty sources
    check_non_empty_sources(trips, zones, result)

    # 2. Required trip columns
    check_required_trip_columns(trips, config, result)

    # 3. Required zone columns
    check_required_zone_columns(zones, config, result)

    # 4. Zone LocationID completeness
    check_zone_location_id_completeness(zones, result)

    # 5. Zone LocationID uniqueness
    check_zone_location_id_uniqueness(zones, result)

    # 6. Timestamp completeness
    check_timestamp_completeness(trips, result)

    # 7. Location completeness
    check_location_completeness(trips, result)

    # 8. Location reference integrity
    check_location_reference_integrity(trips, zones, result)

    # 9. Distance validity
    check_distance_validity(trips, result)

    # 10. Chronology anomaly inspection (Warning only)
    check_chronology_anomalies(trips, result)

    return result

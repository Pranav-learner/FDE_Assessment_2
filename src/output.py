"""Output persistence and validation layer for NYC Taxi Operations Intelligence Pipeline.

Provides atomic writing, schema/sanity verification before persisting,
and manifest creation for:
    - fact_trip (Parquet)
    - dim_zone (Parquet)
    - metrics (CSV)
    - manifest (JSON)
"""

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, List, Optional
import uuid

import pandas as pd
from pandas.util import hash_pandas_object

from src.config import PipelineConfig
from src.metrics import MetricResult, metrics_to_dataframe


class OutputValidationError(ValueError):
    """Raised when processed outputs fail pre-save sanity checks."""
    pass


class OutputWriteError(RuntimeError):
    """Raised when atomic file persistence encounters an I/O error."""
    pass


FACT_TRIP_REQUIRED_COLUMNS: tuple[str, ...] = (
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
)

DIM_ZONE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
)

HEADLINE_METRIC_NAMES: tuple[str, ...] = (
    "Trip Volume",
    "Average Trip Duration",
    "Median Trip Duration",
    "Average Trip Distance",
    "Invalid Trip Duration Rate",
    "Invalid/Unmatched Location Rate",
)


def get_fact_trip_output_filename(year: int, month: int) -> str:
    """Return deterministic filename for modeled fact_trip."""
    return f"fact_trip_{year}-{month:02d}.parquet"


def get_dim_zone_output_filename(year: int, month: int) -> str:
    """Return deterministic filename for modeled dim_zone."""
    return f"dim_zone_{year}-{month:02d}.parquet"


def get_metrics_output_filename(year: int, month: int) -> str:
    """Return deterministic filename for computed metrics."""
    return f"metrics_{year}-{month:02d}.csv"


def get_manifest_output_filename(year: int, month: int) -> str:
    """Return deterministic filename for execution manifest."""
    return f"manifest_{year}-{month:02d}.json"


def get_logical_run_key(year: int, month: int) -> str:
    """Return the deterministic logical partition/run key for a reporting period.

    Distinguishes the logical data partition (e.g. '2026-07') from individual
    execution timestamps or run IDs, ensuring multiple pipeline executions for
    the same reporting period map to the same partition and replace outputs safely.
    """
    return f"{year}-{month:02d}"


def compute_dataframe_fingerprint(
    df: pd.DataFrame,
    sort_by: Optional[str | List[str]] = None,
    columns: Optional[List[str]] = None,
) -> str:
    """Calculate a deterministic logical SHA-256 fingerprint of DataFrame contents.

    Sorts rows by designated key column(s), projects to specified or stable columns,
    hashes row contents using pandas.util.hash_pandas_object, and returns the SHA-256 hex digest.
    This guarantees logical content equality independent of file metadata, writer version,
    or internal DataFrame row ordering.

    Args:
        df: Input DataFrame.
        sort_by: Column or list of columns to sort by before hashing.
        columns: Optional list of columns to include. If None, uses all columns in df.

    Returns:
        str: 64-character hexadecimal SHA-256 string.
    """
    if df.empty:
        return hashlib.sha256(b"EMPTY").hexdigest()

    cols = list(columns) if columns is not None else list(df.columns)
    sub_df = df[cols]

    if sort_by is not None:
        sort_cols = [sort_by] if isinstance(sort_by, str) else list(sort_by)
        sub_df = sub_df.sort_values(by=sort_cols).reset_index(drop=True)

    row_hashes = hash_pandas_object(sub_df, index=False).values
    return hashlib.sha256(row_hashes.tobytes()).hexdigest()


def compute_fact_trip_fingerprint(fact_trip: pd.DataFrame) -> str:
    """Calculate deterministic logical content fingerprint for fact_trip."""
    return compute_dataframe_fingerprint(
        fact_trip,
        sort_by="trip_id",
        columns=list(FACT_TRIP_REQUIRED_COLUMNS),
    )


def compute_dim_zone_fingerprint(dim_zone: pd.DataFrame) -> str:
    """Calculate deterministic logical content fingerprint for dim_zone."""
    return compute_dataframe_fingerprint(
        dim_zone,
        sort_by="LocationID",
        columns=list(DIM_ZONE_REQUIRED_COLUMNS),
    )


def compute_metrics_fingerprint(metrics_df: pd.DataFrame) -> str:
    """Calculate deterministic logical content fingerprint for metrics DataFrame."""
    metric_col = "Metric Name" if "Metric Name" in metrics_df.columns else "metric_name"
    cols = [
        c
        for c in [
            "Metric Name",
            "Type",
            "Value",
            "Display Value",
            "Unit",
            "Numerator",
            "Denominator",
            "Population",
            "Definition",
        ]
        if c in metrics_df.columns
    ]
    return compute_dataframe_fingerprint(
        metrics_df,
        sort_by=metric_col,
        columns=cols if cols else None,
    )


def validate_outputs(
    fact_trip: pd.DataFrame,
    dim_zone: pd.DataFrame,
    metrics_df: pd.DataFrame,
    expected_trip_rows: Optional[int] = None,
    expected_zone_rows: Optional[int] = 265,
) -> None:
    """Perform pre-save sanity checks on all processed outputs.

    Args:
        fact_trip: Modeled trip fact table.
        dim_zone: Modeled zone dimension table.
        metrics_df: DataFrame containing computed metrics.
        expected_trip_rows: Optional expected row count for fact_trip.
        expected_zone_rows: Optional expected row count for dim_zone (defaults to 265).

    Raises:
        OutputValidationError: If any output fails sanity verification.
    """
    # 1. Non-empty checks
    if not isinstance(fact_trip, pd.DataFrame) or len(fact_trip) == 0:
        raise OutputValidationError("fact_trip must be a non-empty pandas DataFrame.")

    if not isinstance(dim_zone, pd.DataFrame) or len(dim_zone) == 0:
        raise OutputValidationError("dim_zone must be a non-empty pandas DataFrame.")

    if not isinstance(metrics_df, pd.DataFrame) or len(metrics_df) == 0:
        raise OutputValidationError("metrics output must be a non-empty pandas DataFrame.")

    # 2. Schema checks - fact_trip
    missing_fact_cols = [c for c in FACT_TRIP_REQUIRED_COLUMNS if c not in fact_trip.columns]
    if missing_fact_cols:
        raise OutputValidationError(
            f"fact_trip is missing required output column(s): {missing_fact_cols}"
        )

    # 3. Schema checks - dim_zone
    missing_zone_cols = [c for c in DIM_ZONE_REQUIRED_COLUMNS if c not in dim_zone.columns]
    if missing_zone_cols:
        raise OutputValidationError(
            f"dim_zone is missing required output column(s): {missing_zone_cols}"
        )

    # 4. Row count checks
    if expected_trip_rows is not None and len(fact_trip) != expected_trip_rows:
        raise OutputValidationError(
            f"fact_trip row count mismatch: expected {expected_trip_rows:,}, got {len(fact_trip):,}."
        )

    if expected_zone_rows is not None and len(dim_zone) != expected_zone_rows:
        raise OutputValidationError(
            f"dim_zone row count mismatch: expected {expected_zone_rows}, got {len(dim_zone)}."
        )

    # 5. Primary key uniqueness - fact_trip
    if fact_trip["trip_id"].duplicated().any():
        dup_count = int(fact_trip["trip_id"].duplicated().sum())
        raise OutputValidationError(
            f"fact_trip contains {dup_count} duplicate trip_id value(s)."
        )

    # 6. Foreign key integrity - pickup / dropoff locations exist in dim_zone
    valid_zone_ids = set(dim_zone["LocationID"].dropna().unique())
    invalid_pu = ~fact_trip["pickup_location_id"].isin(valid_zone_ids)
    if invalid_pu.any():
        count_pu = int(invalid_pu.sum())
        raise OutputValidationError(
            f"fact_trip contains {count_pu} invalid pickup_location_id reference(s) not in dim_zone."
        )

    invalid_do = ~fact_trip["dropoff_location_id"].isin(valid_zone_ids)
    if invalid_do.any():
        count_do = int(invalid_do.sum())
        raise OutputValidationError(
            f"fact_trip contains {count_do} invalid dropoff_location_id reference(s) not in dim_zone."
        )

    # 7. Metrics completeness - all 6 headline metrics must be present
    metric_col = "Metric Name" if "Metric Name" in metrics_df.columns else "metric_name"
    if metric_col not in metrics_df.columns:
        raise OutputValidationError(
            f"metrics DataFrame must contain a '{metric_col}' column."
        )

    present_metrics = set(metrics_df[metric_col].astype(str).values)
    missing_metrics = [m for m in HEADLINE_METRIC_NAMES if m not in present_metrics]
    if missing_metrics:
        raise OutputValidationError(
            f"metrics output missing headline metric(s): {missing_metrics}."
        )


def atomic_write_file(target_path: Path, write_fn: Callable[[Path], None]) -> None:
    """Write content to target_path using an atomic temp-file replace strategy.

    1. Writes content to a hidden temporary file in the target directory.
    2. Atomically replaces the target file via os.replace upon successful write.
    3. Cleans up the temporary file if write_fn raises an exception, ensuring
       no partially written or corrupted files are left behind.

    Args:
        target_path: The final destination path.
        write_fn: Callback accepting the temporary Path to write data to.

    Raises:
        OutputWriteError: If write_fn fails or atomic replacement encounters an error.
    """
    target = Path(target_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.parent / f".{target.name}.tmp.{uuid.uuid4().hex}"

    try:
        write_fn(temp_path)
        os.replace(temp_path, target)
    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        raise OutputWriteError(
            f"Atomic persistence failed for destination '{target}': {exc}"
        ) from exc


def atomic_write_parquet(df: pd.DataFrame, target_path: Path) -> None:
    """Atomically persist a pandas DataFrame to Parquet format."""
    atomic_write_file(target_path, lambda tmp: df.to_parquet(tmp, index=False))


def atomic_write_csv(df: pd.DataFrame, target_path: Path) -> None:
    """Atomically persist a pandas DataFrame to CSV format."""
    atomic_write_file(target_path, lambda tmp: df.to_csv(tmp, index=False))


def atomic_write_json(data: dict[str, Any], target_path: Path) -> None:
    """Atomically persist a dictionary to JSON format."""
    def _write_json(tmp: Path) -> None:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    atomic_write_file(target_path, _write_json)


def generate_manifest(
    reporting_period: str,
    fact_trip_path: Path,
    fact_trip_rows: int,
    dim_zone_path: Path,
    dim_zone_rows: int,
    metrics_path: Path,
    metric_count: int,
    pipeline_status: str = "success",
) -> dict[str, Any]:
    """Build the output manifest dictionary capturing execution evidence."""
    return {
        "reporting_period": reporting_period,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fact_trip_path": str(fact_trip_path),
        "fact_trip_rows": int(fact_trip_rows),
        "dim_zone_path": str(dim_zone_path),
        "dim_zone_rows": int(dim_zone_rows),
        "metrics_path": str(metrics_path),
        "metric_count": int(metric_count),
        "pipeline_status": pipeline_status,
    }


def publish_outputs(
    fact_trip: pd.DataFrame,
    dim_zone: pd.DataFrame,
    metrics_data: List[MetricResult] | pd.DataFrame,
    config: PipelineConfig,
    logger: Optional[logging.Logger] = None,
    expected_trip_rows: Optional[int] = None,
    expected_zone_rows: Optional[int] = 265,
) -> dict[str, Any]:
    """Validate and atomically persist all pipeline outputs and execution manifest.

    Args:
        fact_trip: Modeled fact_trip DataFrame.
        dim_zone: Modeled dim_zone DataFrame.
        metrics_data: List of MetricResult or DataFrame of metrics.
        config: PipelineConfig instance with output directory paths.
        logger: Optional logger for progress and failure reporting.
        expected_trip_rows: Optional expected row count for fact_trip.
            If None and July 2026, defaults to 3,530,063.
        expected_zone_rows: Optional expected row count for dim_zone (defaults to 265).

    Returns:
        dict: Generated execution manifest.

    Raises:
        OutputValidationError: If sanity checks fail before writing.
        OutputWriteError: If any atomic write fails.
    """
    period_str = f"{config.reporting_year}-{config.reporting_month:02d}"

    # Default expected trip rows for July 2026 if not explicitly overridden
    if expected_trip_rows is None and config.reporting_year == 2026 and config.reporting_month == 7:
        expected_trip_rows = 3530063

    # Format metrics to DataFrame if list of MetricResults provided
    if isinstance(metrics_data, list):
        metrics_df = metrics_to_dataframe(metrics_data)
    else:
        metrics_df = metrics_data

    # Perform pre-save sanity validation
    try:
        validate_outputs(
            fact_trip=fact_trip,
            dim_zone=dim_zone,
            metrics_df=metrics_df,
            expected_trip_rows=expected_trip_rows,
            expected_zone_rows=expected_zone_rows,
        )
    except OutputValidationError as val_err:
        if logger:
            logger.error(f"Output validation failed: {val_err}")
        raise

    if logger:
        logger.info(
            f"Output validation passed (fact_trip: {len(fact_trip):,} rows, "
            f"dim_zone: {len(dim_zone)} rows, metrics: {len(metrics_df)} KPIs)."
        )

    # Resolve target paths from config
    fact_trip_dir = config.fact_trip_output_dir or (config.processed_dir / "fact_trip")
    dim_zone_dir = config.dim_zone_output_dir or (config.processed_dir / "dim_zone")
    metrics_dir = config.metrics_output_dir or (config.processed_dir / "metrics")
    manifest_dir = config.processed_dir

    fact_trip_path = fact_trip_dir / get_fact_trip_output_filename(
        config.reporting_year, config.reporting_month
    )
    dim_zone_path = dim_zone_dir / get_dim_zone_output_filename(
        config.reporting_year, config.reporting_month
    )
    metrics_path = metrics_dir / get_metrics_output_filename(
        config.reporting_year, config.reporting_month
    )
    manifest_path = manifest_dir / get_manifest_output_filename(
        config.reporting_year, config.reporting_month
    )

    # 1. Atomically persist fact_trip
    atomic_write_parquet(fact_trip, fact_trip_path)
    if logger:
        logger.info(f"Saved fact_trip ({len(fact_trip):,} rows) -> {fact_trip_path}")

    # 2. Atomically persist dim_zone
    atomic_write_parquet(dim_zone, dim_zone_path)
    if logger:
        logger.info(f"Saved dim_zone ({len(dim_zone):,} rows) -> {dim_zone_path}")

    # 3. Atomically persist metrics
    atomic_write_csv(metrics_df, metrics_path)
    if logger:
        logger.info(f"Saved metrics ({len(metrics_df)} KPIs) -> {metrics_path}")

    # 4. Generate and atomically persist manifest
    manifest = generate_manifest(
        reporting_period=period_str,
        fact_trip_path=fact_trip_path,
        fact_trip_rows=len(fact_trip),
        dim_zone_path=dim_zone_path,
        dim_zone_rows=len(dim_zone),
        metrics_path=metrics_path,
        metric_count=len(metrics_df),
        pipeline_status="success",
    )
    atomic_write_json(manifest, manifest_path)
    if logger:
        logger.info(f"Saved manifest -> {manifest_path}")
        logger.info("Pipeline outputs successfully published.")

    return manifest

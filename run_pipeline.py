#!/usr/bin/env python3
"""CLI entrypoint script to run the NYC Taxi Operations Intelligence Pipeline.

Usage:
    python run_pipeline.py --run-date 2026-07-31

Derives the calendar reporting month from the supplied run date, validates
all schema and business constraints, executes the transformation and modeling
layers, computes operational KPIs, and atomically publishes versioned outputs.
"""

import argparse
from datetime import datetime
import logging
from pathlib import Path
import sys
import time
from typing import Any, Optional, Sequence

from src.config import PipelineConfig, create_config, default_config
from src.ingest import load_trip_data, load_zone_data
from src.logger import get_logger
from src.metrics import calculate_metrics, metrics_to_dataframe
from src.model import build_dim_zone, build_fact_trip
from src.output import OutputValidationError, OutputWriteError, publish_outputs
from src.transform import transform_trip_data
from src.validate import PipelineExecutionError, ValidationError, validate_sources


def run(
    config: Optional[PipelineConfig] = None,
    logger: Optional[logging.Logger] = None,
    expected_trip_rows: Optional[int] = None,
) -> dict[str, Any]:
    """Execute the end-to-end NYC Taxi intelligence pipeline.

    Flow:
        1. Ingest: Load raw trip Parquet and zone CSV data.
        2. Validate: Run validation gates on schema, nulls, and referential constraints.
           Critical failures halt execution immediately with ValidationError.
        3. Transform: Filter reporting boundaries and derive quality flags.
        4. Model: Construct relational fact_trip and dim_zone tables.
        5. Metrics: Calculate the 6 headline KPIs and analytical breakdowns.
        6. Output: Pre-save sanity validation and atomic output persistence.

    Args:
        config: Pipeline configuration instance (defaults to default_config for July 2026).
        logger: Logger instance (defaults to 'nyc_taxi_pipeline').
        expected_trip_rows: Optional expected row count for fact_trip.

    Returns:
        dict: Execution summary with status, timing, record counts, and manifest.

    Raises:
        ValidationError: If ingestion validation gates fail.
        OutputValidationError: If pre-save output sanity checks fail.
        PipelineExecutionError: If any execution stage fails.
    """
    cfg = config or default_config
    log = logger or get_logger("nyc_taxi_pipeline")
    start_time = time.time()
    current_stage = "initialization"

    period_str = f"{cfg.reporting_year}-{cfg.reporting_month:02d}"
    period_name = datetime(cfg.reporting_year, cfg.reporting_month, 1).strftime("%B %Y")

    log.info("Pipeline execution initiated.")
    log.info(f"Reporting period: {period_str} ({period_name})")
    log.info(f"Trip input path: {cfg.trip_input_path}")
    log.info(f"Zone input path: {cfg.zone_input_path}")

    try:
        # 1. Ingestion
        current_stage = "ingest"
        raw_trips = load_trip_data(cfg)
        raw_zones = load_zone_data(cfg)
        log.info(f"Ingestion complete: {len(raw_trips):,} raw trips, {len(raw_zones):,} raw zones.")
        log.info(f"Raw trip source path: {cfg.trip_input_path}")
        log.info(f"Raw trips DataFrame type: {type(raw_trips).__name__}")
        log.info(f"Raw trips DataFrame shape: {raw_trips.shape}")
        log.info(f"Raw trips columns ({len(raw_trips.columns)}): {list(raw_trips.columns)}")
        log.info(f"First 5 column names: {list(raw_trips.columns[:5])}")
        log.info(f"Column 'trip_distance' exists: {'trip_distance' in raw_trips.columns}")
        log.info(f"Required trip columns: {list(cfg.trip_required_columns)}")

        # 2. Validation Gates (Critical Failure Stop Point)
        current_stage = "validation"
        val_result = validate_sources(raw_trips, raw_zones, cfg)
        if not val_result.passed:
            error_msg = f"Pipeline failed during validation: {'; '.join(val_result.errors)}"
            log.error(error_msg)
            val_err = ValidationError(error_msg, result=val_result)
            val_err.stage = "validation"
            raise val_err

        # Log non-critical data quality warnings
        if val_result.warnings:
            for warning in val_result.warnings:
                log.warning(warning)

        log.info("Validation gates passed successfully.")

        # 3. Transformation
        current_stage = "transform"
        transformed_trips = transform_trip_data(raw_trips, raw_zones, cfg)
        log.info(f"Transformation complete: {len(transformed_trips):,} reporting period trips.")

        # 4. Modeling
        current_stage = "model"
        dim_zone = build_dim_zone(raw_zones)
        fact_trip = build_fact_trip(transformed_trips)
        log.info(f"Modeling complete: fact_trip={len(fact_trip):,} rows, dim_zone={len(dim_zone):,} rows.")

        # 5. Metrics
        current_stage = "metrics"
        metrics_list = calculate_metrics(fact_trip, dim_zone)
        metrics_df = metrics_to_dataframe(metrics_list)
        log.info(f"Metrics computation complete: {len(metrics_df)} headline metrics.")

        # 6. Output Publication
        current_stage = "publish"
        manifest = publish_outputs(
            fact_trip=fact_trip,
            dim_zone=dim_zone,
            metrics_data=metrics_df,
            config=cfg,
            logger=log,
            expected_trip_rows=expected_trip_rows,
        )
        log.info("Pipeline completed successfully.")

    except (ValidationError, OutputValidationError) as domain_err:
        domain_err.stage = getattr(domain_err, "stage", current_stage)
        raise
    except Exception as exc:
        log.error(f"Pipeline failed during stage '{current_stage}': {exc}")
        exec_err = PipelineExecutionError(f"Stage '{current_stage}' failed: {exc}")
        exec_err.stage = current_stage
        raise exec_err from exc

    elapsed = round(time.time() - start_time, 2)
    return {
        "status": "success",
        "reporting_period": period_str,
        "reporting_period_name": period_name,
        "elapsed_seconds": elapsed,
        "raw_trip_count": len(raw_trips),
        "reporting_trip_count": len(fact_trip),
        "dim_zone_count": len(dim_zone),
        "metrics": [
            {
                "name": m.metric_name,
                "display_value": m.display_value,
                "unit": m.unit,
                "value": m.value,
            }
            for m in metrics_list
        ],
        "manifest": manifest,
    }


def parse_run_date(date_str: str) -> tuple[int, int, str]:
    """Parse and validate run-date argument in YYYY-MM-DD format.

    Args:
        date_str: Date string in YYYY-MM-DD format.

    Returns:
        tuple: (year: int, month: int, formatted_month_name: str)

    Raises:
        ValueError: If the string is malformed or represents an invalid calendar date.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt.year < 2000:
            raise ValueError(f"Year must be >= 2000, got {dt.year}")
        month_name = dt.strftime("%B %Y")
        return dt.year, dt.month, month_name
    except Exception as exc:
        raise ValueError(
            f"Invalid run-date '{date_str}'. Expected format: YYYY-MM-DD (e.g. 2026-07-31). Reason: {exc}"
        ) from exc


def print_success_summary(result: dict) -> None:
    """Print clean terminal summary upon successful pipeline completion."""
    period_name = result.get("reporting_period_name", "July 2026")
    raw_trips = result.get("raw_trip_count", 0)
    rep_trips = result.get("reporting_trip_count", 0)
    fact_trips = rep_trips
    dim_zones = result.get("dim_zone_count", 0)

    print("==========================================")
    print("PIPELINE SUCCESS")
    print("==========================================")
    print(f"Reporting Period: {period_name}\n")
    print(f"Raw trips:       {raw_trips:,}")
    print(f"July trips:      {rep_trips:,}")
    print(f"fact_trip:       {fact_trips:,}")
    print(f"dim_zone:        {dim_zones:,}\n")
    print("Metrics:")

    # Map display names and units
    metric_displays = {
        "Trip Volume": ("Trip Volume:", "3,530,063"),
        "Average Trip Duration": ("Average Trip Duration:", "17.29 min"),
        "Median Trip Duration": ("Median Trip Duration:", "14.17 min"),
        "Average Trip Distance": ("Average Trip Distance:", "5.55 mi"),
        "Invalid Trip Duration Rate": ("Invalid Duration Rate:", "0.0000283%"),
        "Invalid/Unmatched Location Rate": ("Invalid Location Rate:", "0.00%"),
    }

    metrics = result.get("metrics", [])
    for m in metrics:
        name = m.get("name", "")
        disp = m.get("display_value", "")
        if name in metric_displays:
            label, default_fmt = metric_displays[name]
            # Format unit concisely for terminal
            clean_disp = disp.replace(" minutes", " min").replace(" miles", " mi").replace(" trips", "")
            print(f"  {label:<32} {clean_disp:>10}")
        else:
            print(f"  {name:<32} {disp:>10}")

    print("\nOutputs:")
    print("  fact_trip:  published")
    print("  dim_zone:   published")
    print("  metrics:    published")
    print("  manifest:   published\n")
    print("Status: SUCCESS")
    print("==========================================")


def print_failure_summary(period_name: str, stage: str, error_message: str) -> None:
    """Print clean terminal summary upon pipeline execution failure."""
    print("==========================================")
    print("PIPELINE FAILED")
    print("==========================================")
    print(f"Reporting Period: {period_name}")
    print(f"Failed Stage: {stage}\n")
    print("Error:")
    print(f"{error_message}\n")
    print("Processed output was not published.\n")
    print("Status: FAILED")
    print("==========================================")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI main entrypoint. Parses arguments and executes the pipeline."""
    parser = argparse.ArgumentParser(
        description="NYC Taxi Operations Intelligence Pipeline — Production Runner"
    )
    parser.add_argument(
        "--run-date",
        type=str,
        default="2026-07-31",
        help="Calendar date (YYYY-MM-DD) determining the reporting month (default: 2026-07-31).",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Root directory of the project.",
    )
    parser.add_argument(
        "--expected-trip-rows",
        type=int,
        default=None,
        help="Optional expected row count for fact_trip validation.",
    )

    args = parser.parse_args(argv)

    # 1. Parse and validate CLI run-date
    try:
        year, month, period_name = parse_run_date(args.run_date)
    except ValueError as val_err:
        print(f"CLI Error: {val_err}", file=sys.stderr)
        return 2

    logger = get_logger("nyc_taxi_pipeline")

    # 2. Build PipelineConfig
    try:
        config = create_config(
            project_root=args.project_root,
            reporting_year=year,
            reporting_month=month,
        )
    except Exception as cfg_err:
        logger.error(f"Configuration initialization failed: {cfg_err}")
        print_failure_summary(period_name, "config", str(cfg_err))
        return 1

    # 3. Execute Pipeline
    try:
        result = run(config, logger=logger, expected_trip_rows=args.expected_trip_rows)
        print_success_summary(result)
        return 0

    except (ValidationError, OutputValidationError, PipelineExecutionError) as known_err:
        stage = getattr(known_err, "stage", "pipeline")
        # Format clean error message
        if isinstance(known_err, ValidationError) and known_err.result and known_err.result.errors:
            clean_errors = []
            for err in known_err.result.errors:
                if "missing required trip columns:" in err.lower():
                    if "['trip_distance']" in err:
                        clean_errors.append("Missing required trip column: trip_distance")
                    else:
                        clean_errors.append(err.replace("Validation failed: ", "").capitalize())
                else:
                    clean_errors.append(err)
            error_details = "\n".join(clean_errors)
        else:
            error_details = str(known_err)

        print_failure_summary(period_name, stage, error_details)
        return 1

    except Exception as unexpected_err:
        logger.error(f"Unexpected pipeline termination: {unexpected_err}")
        print_failure_summary(period_name, "unknown", str(unexpected_err))
        return 1


if __name__ == "__main__":
    sys.exit(main())

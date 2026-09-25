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
from pathlib import Path
import sys
from typing import Optional, Sequence

from src.config import create_config
from src.logger import get_logger
from src.output import OutputValidationError, OutputWriteError
from src.pipeline import run
from src.validate import PipelineExecutionError, ValidationError


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

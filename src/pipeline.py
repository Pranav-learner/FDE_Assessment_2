"""Pipeline orchestration layer for NYC Taxi Operations Intelligence Pipeline.

Orchestrates sequential execution across components:
    Ingestion -> Validation Gates -> Transformation -> Modeling -> Metrics -> Output Publication
Halts immediately upon critical validation errors, preventing invalid data from reaching
downstream modeling or published outputs.
"""

from datetime import datetime
import logging
from pathlib import Path
import time
from typing import Any, Optional

from src.config import PipelineConfig, default_config
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

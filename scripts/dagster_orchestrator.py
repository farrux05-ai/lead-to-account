"""
Dagster Orchestration — B2B RevOps Pipeline
============================================
Defines the full asset graph:
  1. extract_hubspot_dlt   → Ingests new CRM deals via dlt (daily)
  2. transform_warehouse_dbt → Rebuilds all dbt models after ingestion

Run locally:
  dagster dev -f scripts/dagster_orchestrator.py

Access the Dagster UI at: http://localhost:3000
"""

from dagster import (
    asset,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetExecutionContext,
)
import subprocess
import os


@asset(group_name="ingestion")
def extract_hubspot_dlt(context: AssetExecutionContext):
    """
    Step 1: Extract mock Deals using DLT and load into DuckDB.
    This acts as the daily 'Ingestion' step.

    Production: Set HUBSPOT_API_KEY env variable, and update
    the dlt_hubspot_pipeline.py resource to call the real API.
    """
    script_path = os.path.join(os.path.dirname(__file__), "dlt_hubspot_pipeline.py")
    result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
    context.log.info(result.stdout)
    return "DLT Ingestion successful"


@asset(group_name="transformation", deps=[extract_hubspot_dlt])
def transform_warehouse_dbt(context: AssetExecutionContext):
    """
    Step 2: Transform the loaded raw data using dbt.
    Runs only after DLT successfully pulls new data.

    Uses `dbt build` which runs all models + tests in one command,
    ensuring data quality gates are enforced before marts are materialized.
    """
    project_root = os.path.dirname(os.path.dirname(__file__))
    result = subprocess.run(
        ["dbt", "build"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True
    )
    context.log.info(result.stdout)
    return "dbt transformation successful"


# ── Job & Schedule Definition ──────────────────────────────────────────────────
# Wraps the two assets into a single runnable job.
daily_revops_job = define_asset_job(
    name="daily_revops_pipeline",
    selection=[extract_hubspot_dlt, transform_warehouse_dbt],
)

# Triggers the job every day at 06:00 UTC (before business hours start).
daily_schedule = ScheduleDefinition(
    job=daily_revops_job,
    cron_schedule="0 6 * * *",  # 6:00 AM UTC daily
    name="daily_06_utc_schedule",
)

defs = Definitions(
    assets=[extract_hubspot_dlt, transform_warehouse_dbt],
    jobs=[daily_revops_job],
    schedules=[daily_schedule],
)

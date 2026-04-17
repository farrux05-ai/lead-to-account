"""
Dagster Orchestration — B2B RevOps Pipeline (Granular dbt Tracking)
==================================================================
Defines the full asset graph with granular dbt models.
"""

from dagster import (
    AssetExecutionContext,
    Definitions,
    asset,
    define_asset_job,
    ScheduleDefinition,
)
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    get_asset_key_for_source,
)
import subprocess
import os
import sys
from pathlib import Path

# ── 1. dbt Configuration ──────────────────────────────────────────────────────
# Full path to the dbt project and venv
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
DBT_PROJECT_DIR = PROJECT_ROOT
DB_PATH = DBT_PROJECT_DIR / "dev.duckdb"

# Resource to manage dbt CLI calls
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    dbt_executable=os.fspath(DBT_PROJECT_DIR / "venv" / "bin" / "dbt"),
    profiles_dir=os.path.expanduser("~/.dbt"),
)

# Path to the manifest file
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# Custom Translator to link dbt sources to Dagster assets
class MarketingTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        return super().get_asset_key(dbt_resource_props)

# ── 2. Ingestion Asset (dlt) ──────────────────────────────────────────────────
@asset(group_name="ingestion")
def extract_hubspot_dlt(context: AssetExecutionContext):
    """
    Ingests HubSpot CRM data using the DLT library.
    """
    script_path = PROJECT_ROOT / "scripts" / "dlt_hubspot_pipeline.py"
    result = subprocess.run(
        [sys.executable, str(script_path)], 
        capture_output=True, 
        text=True, 
        check=True
    )
    context.log.info(result.stdout)
    return "Ingestion successful"

# ── 3. Granular dbt Assets ────────────────────────────────────────────────────
# This decorator automatically maps all models in manifest.json to Dagster assets
@dbt_assets(
    manifest=MANIFEST_PATH,
    dagster_dbt_translator=MarketingTranslator(),
)
def marketing_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Run the dbt models
    yield from dbt.cli(["build"], context=context).stream()

# ── 4. Job & Definitions ──────────────────────────────────────────────────────
# Job to run the whole pipeline
revops_pipeline_job = define_asset_job(
    name="revops_full_pipeline",
    selection="*",
)

# Daily Schedule
daily_schedule = ScheduleDefinition(
    job=revops_pipeline_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[extract_hubspot_dlt, marketing_dbt_assets],
    resources={
        "dbt": dbt_resource,
    },
    jobs=[revops_pipeline_job],
    schedules=[daily_schedule],
)

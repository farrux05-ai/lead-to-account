from dagster import asset, Definitions
import subprocess
import os

@asset(group_name="ingestion")
def extract_hubspot_dlt():
    """
    Step 1: Extract mock Deals using DLT and load into DuckDB.
    This acts as the daily 'Ingestion' step.
    """
    script_path = os.path.join(os.path.dirname(__file__), "dlt_hubspot_pipeline.py")
    subprocess.run(["python", script_path], check=True)
    return "DLT Ingestion successful"

@asset(group_name="transformation", deps=[extract_hubspot_dlt])
def transform_warehouse_dbt():
    """
    Step 2: Transform the loaded raw data using dbt.
    Runs only after DLT successfully pulls new data.
    """
    # Change directory to project root to run dbt
    project_root = os.path.dirname(os.path.dirname(__file__))
    subprocess.run(["dbt", "build"], cwd=project_root, check=True)
    return "dbt transformation successful"

defs = Definitions(
    assets=[extract_hubspot_dlt, transform_warehouse_dbt],
)

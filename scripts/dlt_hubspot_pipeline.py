import dlt
import os
import random
from datetime import datetime

# The DuckDB path and dataset name are configurable via environment variables.
# This makes the pipeline portable across dev/staging/prod environments.
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "dev.duckdb")
DATASET_NAME = os.environ.get("DLT_DATASET_NAME", "main_raw_hubspot")

# 1. Define the Data Source (Mock API)
@dlt.resource(name="hubspot_deals_incremental", write_disposition="append")
def fetch_hubspot_deals_from_api():
    """
    Mock API generator wrapped in a DLT resource.
    In a real production project, this would look like: 
    yield requests.get('https://api.hubapi.com/crm/v3/objects/deals', headers=auth).json()['results']
    
    Production swap:
        - Set HUBSPOT_API_KEY env variable
        - Replace yield below with actual API call
    """
    b2b_domains = ['acme.com', 'stark.com', 'wayne.com', 'oscorp.com', 'cyberdyne.com']
    stages = ['discovery', 'presentation', 'negotiation', 'closed_won', 'closed_lost']
    
    # Simulate API fetching 20 new deals created "today"
    now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"📡 Fetching {20} Deals from HubSpot API at {now_str}...")
    
    deals_batch = []
    for i in range(20):
        # Force the first deal to be closed_won for dashboard visibility
        deal_stage = 'closed_won' if i == 0 else random.choice(stages)
        deals_batch.append({
            "deal_id": f"HSDL_DLT_{random.randint(1000, 9999)}",
            "contact_id": f"HSCT{random.randint(1, 100):05d}",
            "deal_name": f"{random.choice(b2b_domains).upper().split('.')[0]} - Expansion License",
            "deal_stage": deal_stage,
            "amount": random.randint(5000, 50000) if deal_stage != 'closed_lost' else 0,
            "created_at": now_str,
            "closed_at": now_str if deal_stage in ['closed_won', 'closed_lost'] else None,
            "_loaded_at": now_str
        })
        
    yield deals_batch

# 2. Define the Pipeline
if __name__ == "__main__":
    # Configure DLT to push securely to our local DuckDB warehouse.
    # Override via environment variables for staging/production deployments.
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_crm_pipeline",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name=DATASET_NAME  # Targets the exact schema dbt expects!
    )
    
    # 3. Run the Pipeline (Extract -> Normalize -> Load)
    load_info = pipeline.run(fetch_hubspot_deals_from_api())
    
    print("\n✅ DLT Pipeline Run Successful!")
    print(load_info)

import dlt
import os
import random
from datetime import datetime, timedelta

# The DuckDB path and dataset name are configurable via environment variables.
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "dev.duckdb")
DATASET_NAME = os.environ.get("DLT_DATASET_NAME", "main_raw_hubspot")

def get_random_date_in_range(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = end - start
    random_days = random.randrange(delta.days)
    return start + timedelta(days=random_days)

# 1. Define the Data Source (Mock API)
@dlt.resource(name="hubspot_deals_incremental", write_disposition="append")
def fetch_hubspot_deals_from_api():
    """
    Mock API generator with stable IDs and historical dates.
    Aligns with spend seeds (2026-01-01 to 2026-03-31).
    """
    b2b_domains = ['acme.com', 'stark.com', 'wayne.com', 'oscorp.com', 'cyberdyne.com']
    stages = ['discovery', 'presentation', 'negotiation', 'closed_won', 'closed_lost']
    
    # Range of marketing spend in seeds
    start_date = "2026-01-01"
    end_date = "2026-03-31"
    
    print(f"📡 Generating 20 Stable Deals aligned with Marketing Spend period ({start_date} to {end_date})...")
    
    deals_batch = []
    for i in range(20):
        # STABLE ID: Using index instead of random number ensures DLT + dbt dedup
        # will update existing records rather than infinite accumulation.
        deal_id = f"HSDL_DLT_{i}"
        
        # HISTORICAL DATE: Align with spend seeds
        created_dt = get_random_date_in_range(start_date, end_date)
        created_at_str = created_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Random stage
        deal_stage = random.choice(stages)
        
        deals_batch.append({
            "deal_id": deal_id,
            "contact_id": f"HSCT{random.randint(1, 100):05d}",
            "deal_name": f"{random.choice(b2b_domains).upper().split('.')[0]} - Enterprise License",
            "deal_stage": deal_stage,
            "amount": random.randint(3000, 15000) if deal_stage != 'closed_lost' else 0,
            "created_at": created_at_str,
            "closed_at": created_at_str if deal_stage in ['closed_won', 'closed_lost'] else None,
            "_loaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        })
        
    yield deals_batch

# 2. Define the Pipeline
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_crm_pipeline",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name=DATASET_NAME
    )
    
    # 3. Run the Pipeline
    load_info = pipeline.run(fetch_hubspot_deals_from_api())
    
    print("\n✅ DLT Pipeline Run Successful!")
    print(load_info)

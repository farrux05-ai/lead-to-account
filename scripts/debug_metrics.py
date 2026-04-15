import duckdb
import pandas as pd

# Connect in read_only mode to avoid lock issues with Streamlit
conn = duckdb.connect('my_marketing_project/dev.duckdb', read_only=True)

print("--- REVENUE CHECK ---")
revenue_check = conn.execute("""
    SELECT 
        is_closed_won,
        COUNT(*) as deal_count,
        COUNT(DISTINCT deal_key) as distinct_deal_keys,
        SUM(amount_usd) as total_amount
    FROM main_marts.fct_pipeline_revenue
    GROUP BY 1
""").df()
print(revenue_check)

print("\n--- SPEND CHECK ---")
spend_check = conn.execute("""
    SELECT 
        SUM(daily_spend_usd) as total_spend
    FROM main_marts.fct_ad_performance_daily
""").df()
print(spend_check)

print("\n--- STAGING CHECK (DEDUP EFFECT) ---")
staging_check = conn.execute("""
    SELECT 
        deal_stage,
        COUNT(*) as cnt,
        SUM(amount_usd) as amt
    FROM main_marts.stg_hubspot__deals
    GROUP BY 1
""").df()
print(staging_check)

conn.close()

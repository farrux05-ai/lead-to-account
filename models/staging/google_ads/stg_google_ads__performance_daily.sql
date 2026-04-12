-- ============================================================================
-- MODEL: stg_google_ads__performance_daily
-- ============================================================================
-- GRAIN: One row per campaign, per day.
-- PURPOSE: Clean and standardize daily performance metrics from Google Ads.
--
-- NOTES:
-- - Google Ads API returns cost in micros (millionths of local currency).
--   We immediately pass this through the global `micros_to_dollars` macro
--   so all downstream models utilize standard decimal representation.
-- - Metrics (impressions, clicks) are coalesced to 0. A NULL impression
--   creates fragile analytical logic in Metabase (e.g. SUMs might fail).
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('google_ads', 'google_ads_performance_daily') }}
),

renamed_and_typed AS (
    SELECT
        CAST(campaign_id AS VARCHAR)                     AS source_campaign_id,
        CAST(date AS DATE)                               AS metric_date,
        COALESCE(CAST(impressions AS INTEGER), 0)        AS daily_impressions,
        COALESCE(CAST(clicks AS INTEGER), 0)             AS daily_clicks,
        {{ micros_to_dollars('cost_micros') }}           AS daily_spend_original_currency,
        CAST(currency_code AS VARCHAR)                   AS currency_code,
        CAST(_loaded_at AS TIMESTAMP)                    AS _loaded_at
    FROM source
),

-- Safe dedup: avoids QUALIFY inside views which triggers a DuckDB planner crash.
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_campaign_id, metric_date
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM renamed_and_typed
)

SELECT
    source_campaign_id,
    metric_date,
    daily_impressions,
    daily_clicks,
    daily_spend_original_currency,
    currency_code,
    _loaded_at
FROM ranked
WHERE _row_num = 1

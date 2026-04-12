-- ============================================================================
-- MODEL: stg_meta_ads__performance_daily
-- GRAIN: One row per campaign per day.
-- NOTE: Safe ROW_NUMBER dedup instead of QUALIFY — avoids DuckDB bug.
-- Meta spend is in cents → cents_to_dollars macro converts to decimal USD.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('meta_ads', 'meta_ads_performance_daily') }}
),

renamed_and_typed AS (
    SELECT
        CAST(campaign_id AS VARCHAR)                    AS source_campaign_id,
        CAST(date_start AS DATE)                        AS metric_date,
        COALESCE(CAST(impressions AS INTEGER), 0)       AS daily_impressions,
        COALESCE(CAST(clicks AS INTEGER), 0)            AS daily_clicks,
        {{ cents_to_dollars('spend_cents') }}           AS daily_spend_original_currency,
        CAST(currency AS VARCHAR)                       AS currency_code,
        CAST(_loaded_at AS TIMESTAMP)                   AS _loaded_at
    FROM source
),

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

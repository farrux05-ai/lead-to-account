{% macro generate_stg_ad_performance(source_name, table_name, date_col, spend_col, spend_divisor, currency_col) %}

-- ============================================================================
-- MACRO: generate_stg_ad_performance
-- PURPOSE: DRY generation of ad performance staging models across platforms.
-- Resolves column naming discrepancies and normalizes currency values inline.
-- Safe dedup avoids duckdb QUALIFY bugs.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source(source_name, table_name) }}
),

renamed_and_typed AS (
    SELECT
        CAST(campaign_id AS VARCHAR)                     AS source_campaign_id,
        CAST({{ date_col }} AS DATE)                     AS metric_date,
        COALESCE(CAST(impressions AS INTEGER), 0)        AS daily_impressions,
        COALESCE(CAST(clicks AS INTEGER), 0)             AS daily_clicks,
        CAST({{ spend_col }} AS DOUBLE)                  AS daily_spend_raw,
        CAST({{ spend_divisor }} AS DOUBLE)              AS spend_divisor,
        CAST({{ currency_col }} AS VARCHAR)              AS currency_code,
        CAST(_loaded_at AS TIMESTAMP)                    AS _loaded_at
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
    daily_spend_raw,
    spend_divisor,
    currency_code,
    _loaded_at
FROM ranked
WHERE _row_num = 1

{% endmacro %}

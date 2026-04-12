-- ============================================================================
-- MODEL: stg_linkedin_ads__campaigns
-- GRAIN: One row per LinkedIn campaign.
-- NOTE: Safe ROW_NUMBER dedup instead of QUALIFY — avoids DuckDB bug.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('linkedin_ads', 'linkedin_ads_campaigns') }}
),

renamed_and_typed AS (
    SELECT
        CAST(campaign_id AS VARCHAR)             AS source_campaign_id,
        CAST(campaign_name AS VARCHAR)           AS campaign_name,
        CAST(status AS VARCHAR)                  AS campaign_status,
        CAST(daily_budget_currency AS VARCHAR)   AS currency_code,
        CAST(_loaded_at AS TIMESTAMP)            AS _loaded_at
    FROM source
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_campaign_id
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM renamed_and_typed
)

SELECT
    source_campaign_id,
    campaign_name,
    campaign_status,
    currency_code,
    _loaded_at
FROM ranked
WHERE _row_num = 1

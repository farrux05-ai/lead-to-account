-- ============================================================================
-- MODEL: stg_meta_ads__campaigns
-- GRAIN: One row per Meta campaign.
-- NOTE: Safe ROW_NUMBER dedup used instead of QUALIFY to avoid a DuckDB
--       planner assertion error triggered by QUALIFY inside chained views.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('meta_ads', 'meta_ads_campaigns') }}
),

renamed_and_typed AS (
    SELECT
        CAST(campaign_id AS VARCHAR)    AS source_campaign_id,
        CAST(campaign_name AS VARCHAR)  AS campaign_name,
        CAST(objective AS VARCHAR)      AS campaign_objective,
        CAST(status AS VARCHAR)         AS campaign_status,
        CAST(currency AS VARCHAR)       AS currency_code,
        CAST(_loaded_at AS TIMESTAMP)   AS _loaded_at
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
    campaign_objective,
    campaign_status,
    currency_code,
    _loaded_at
FROM ranked
WHERE _row_num = 1

-- ============================================================================
-- MODEL: stg_google_ads__campaigns
-- ============================================================================
-- GRAIN: One row per unique Google Ads campaign.
-- PURPOSE: Extract campaign attributes, standardize naming, and ensure a
--          clean, deduplicated spine of campaigns from Google Ads.
-- 
-- NOTES:
-- - In a production environment with Fivetran/Airbyte, API extraction errors
--   often cause duplicate rows during backfills. We use ROW_NUMBER() over
--   _loaded_at to fetch the most recent state of the campaign.
-- - 'campaign_type' and 'status' are explicit dimensions used for filtering.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('google_ads', 'google_ads_campaigns') }}
),

renamed_and_typed AS (
    SELECT
        -- Primary Key
        CAST(campaign_id AS VARCHAR) AS source_campaign_id,

        -- Attributes
        CAST(campaign_name AS VARCHAR) AS campaign_name,
        CAST(campaign_type AS VARCHAR) AS campaign_type,
        CAST(status AS VARCHAR) AS campaign_status,
        CAST(currency_code AS VARCHAR) AS currency_code,

        -- System / Metadata
        CAST(_loaded_at AS TIMESTAMP) AS _loaded_at

    FROM source
),

-- Defensive dedup: keep the latest record per campaign_id.
-- Using a subquery + rank instead of QUALIFY to avoid a DuckDB planner
-- assertion bug that fires when QUALIFY appears inside chained view definitions.
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
    campaign_type,
    campaign_status,
    currency_code,
    _loaded_at
FROM ranked
WHERE _row_num = 1

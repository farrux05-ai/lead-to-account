-- ============================================================================
-- MODEL: int_unified_campaigns
-- ============================================================================
-- GRAIN: One row per campaign across ALL paid channels.
-- PURPOSE: Build a single canonical campaign spine by unioning Google Ads,
--          Meta Ads, and LinkedIn Ads campaigns into one table with a
--          channel-aware surrogate key.
--
-- WHY THIS MATTERS:
-- - Marketing teams think in campaigns, not in platforms. A CMO asking
--   "which campaign drove the most pipeline?" expects a single answer
--   across all channels. This model makes that possible.
-- - The surrogate key pattern (channel || source_id) prevents collisions.
--   Without this, "GC001" from Google and "LC001" from LinkedIn would be
--   indistinguishable in downstream JOINs.
--
-- EDGE CASES HANDLED:
-- - Paused/Removed campaigns are preserved with status flags so historical
--   performance data can still reference them without orphaned foreign keys.
-- - Currency differences are carried forward (USD vs EUR) — normalization
--   happens at the performance layer, not here.
-- ============================================================================

WITH google_campaigns AS (
    SELECT
        'google_ads' AS channel,
        source_campaign_id,
        campaign_name,
        campaign_type AS campaign_type_raw,
        campaign_status,
        currency_code,

        -- Standardize campaign type across channels
        CASE campaign_type
            WHEN 'SEARCH' THEN 'paid_search'
            WHEN 'DISPLAY' THEN 'display'
            WHEN 'PERFORMANCE_MAX' THEN 'performance_max'
            ELSE LOWER(campaign_type)
        END AS campaign_type_standardized

    FROM {{ ref('stg_google_ads__campaigns') }}
),

meta_campaigns AS (
    SELECT
        'meta_ads' AS channel,
        source_campaign_id,
        campaign_name,
        campaign_objective AS campaign_type_raw,
        campaign_status,
        currency_code,

        CASE campaign_objective
            WHEN 'LEAD_GENERATION' THEN 'lead_generation'
            WHEN 'BRAND_AWARENESS' THEN 'brand_awareness'
            WHEN 'CONVERSIONS' THEN 'conversions'
            ELSE LOWER(campaign_objective)
        END AS campaign_type_standardized

    FROM {{ ref('stg_meta_ads__campaigns') }}
),

linkedin_campaigns AS (
    SELECT
        'linkedin_ads' AS channel,
        source_campaign_id,
        campaign_name,
        'SPONSORED' AS campaign_type_raw,
        campaign_status,
        currency_code,

        'paid_social' AS campaign_type_standardized

    FROM {{ ref('stg_linkedin_ads__campaigns') }}
),

-- Stack all channels into one unified campaign spine
unioned AS (
    SELECT * FROM google_campaigns
    UNION ALL
    SELECT * FROM meta_campaigns
    UNION ALL
    SELECT * FROM linkedin_campaigns
),

-- Add the cross-channel surrogate key and activity flag
final AS (
    SELECT
        -- Surrogate key: collision-safe across channels
        {{ dbt_utils.generate_surrogate_key(['channel', 'source_campaign_id']) }} AS campaign_key,

        channel,
        source_campaign_id,
        campaign_name,
        campaign_type_raw,
        campaign_type_standardized,
        campaign_status,
        currency_code,

        -- Soft-delete / activity flag for downstream filtering
        -- Metabase filters can use this to exclude dead campaigns by default
        CASE
            WHEN campaign_status IN ('ENABLED', 'ACTIVE') THEN TRUE
            ELSE FALSE
        END AS is_active

    FROM unioned
)

SELECT * FROM final

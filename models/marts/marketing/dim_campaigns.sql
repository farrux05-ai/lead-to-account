-- ============================================================================
-- MODEL: dim_campaigns
-- ============================================================================
-- GRAIN: One row per unique campaign across all paid channels.
-- PURPOSE: The canonical campaign dimension for every marketing dashboard.
--          Wraps int_unified_campaigns as a materialized table so Metabase
--          JOIN performance remains fast even as historical campaigns grow.
--
-- DESIGN DECISION:
-- - This is a thin wrapper over the intermediate model. The heavy lifting
--   (UNION, surrogate keys, type standardization) lives in int_unified_campaigns.
-- - We materialize as a table here because Metabase runs ad-hoc JOINs against
--   this dimension for every campaign-level question — a view would force
--   re-execution of the UNION on every dashboard load.
-- ============================================================================

{{ config(materialized='table') }}

SELECT
    -- Primary key (surrogate, collision-safe across channels)
    campaign_key,

    -- Channel identification
    channel,
    source_campaign_id,

    -- Campaign attributes
    campaign_name,
    campaign_type_raw,
    campaign_type_standardized,
    campaign_status,
    currency_code,

    -- Activity flag for default Metabase filters
    is_active

FROM {{ ref('int_unified_campaigns') }}

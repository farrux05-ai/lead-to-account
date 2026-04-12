-- ============================================================================
-- MODEL: int_unified_ad_performance
-- ============================================================================
-- GRAIN: One row per channel + campaign + day.
-- PURPOSE: Merge daily ad performance from all three paid channels into a
--          single cross-channel performance table with normalized metrics.
--
-- WHY THIS MATTERS:
-- - Single source of truth for "How much did we spend across all paid channels?"
-- - Currency normalized to USD at this layer so all downstream marts can
--   safely SUM/AVG without worrying about mixed currencies.
--
-- DESIGN NOTE — Metric calculation placement:
-- - CTR/CPC are NOT calculated here. They are computed in the mart layer
--   (fct_ad_performance_daily) where types are well-defined after a JOIN.
-- - Computing divisions directly over a UNION ALL in DuckDB 1.5 can trigger
--   an internal assertion failure due to ambiguous type resolution on the
--   UNION output columns. Moving computation downstream avoids this entirely.
--
-- EDGE CASES HANDLED:
-- - Google spend in micros → converted in staging via micros_to_dollars.
-- - Meta spend in cents → converted in staging via cents_to_dollars.
-- - LinkedIn reports decimal USD natively.
-- - EUR campaigns multiplied by fixed 1.08 rate (production: exchange rate dim).
-- - Anomaly flag: spend > 0 but impressions = 0 → API billing artifact.
-- ============================================================================

WITH google_performance AS (
    SELECT
        'google_ads'                            AS channel,
        source_campaign_id,
        metric_date,
        CAST(daily_impressions AS INTEGER)      AS daily_impressions,
        CAST(daily_clicks AS INTEGER)           AS daily_clicks,
        CAST(daily_spend_original_currency AS DOUBLE) AS daily_spend_local,
        currency_code
    FROM {{ ref('stg_google_ads__performance_daily') }}
),

meta_performance AS (
    SELECT
        'meta_ads'                              AS channel,
        source_campaign_id,
        metric_date,
        CAST(daily_impressions AS INTEGER)      AS daily_impressions,
        CAST(daily_clicks AS INTEGER)           AS daily_clicks,
        CAST(daily_spend_original_currency AS DOUBLE) AS daily_spend_local,
        currency_code
    FROM {{ ref('stg_meta_ads__performance_daily') }}
),

linkedin_performance AS (
    SELECT
        'linkedin_ads'                          AS channel,
        source_campaign_id,
        metric_date,
        CAST(daily_impressions AS INTEGER)      AS daily_impressions,
        CAST(daily_clicks AS INTEGER)           AS daily_clicks,
        CAST(daily_spend_usd AS DOUBLE)         AS daily_spend_local,
        currency_code
    FROM {{ ref('stg_linkedin_ads__performance_daily') }}
),

-- Explicit casts on every branch of the UNION prevents DuckDB from entering
-- ambiguous type resolution during the physical planning stage.
unioned AS (
    SELECT * FROM google_performance
    UNION ALL
    SELECT * FROM meta_performance
    UNION ALL
    SELECT * FROM linkedin_performance
),

final AS (
    SELECT
        -- Cross-channel surrogate key (collision-safe across channels)
        {{ dbt_utils.generate_surrogate_key(['channel', 'source_campaign_id']) }} AS campaign_key,

        channel,
        source_campaign_id,
        metric_date,

        -- Core volume metrics (INTEGER types — safe for SUM in Metabase)
        daily_impressions,
        daily_clicks,

        -- Normalized spend in USD (DOUBLE type, consistent across all rows)
        -- Production: replace the 1.08 constant with a JOIN to dim_exchange_rates
        CASE
            WHEN currency_code = 'EUR' THEN ROUND(daily_spend_local * 1.08, 6)
            ELSE ROUND(daily_spend_local, 6)
        END AS daily_spend_usd,

        -- Preserve original local spend and currency for auditability
        daily_spend_local,
        currency_code,

        -- Anomaly flag: billing events with no impressions indicate API glitches.
        -- Downstream dashboards should default-filter is_anomaly = FALSE.
        CASE
            WHEN daily_spend_local > 0.0 AND daily_impressions = 0 THEN TRUE
            ELSE FALSE
        END AS is_anomaly

    FROM unioned
)

SELECT * FROM final

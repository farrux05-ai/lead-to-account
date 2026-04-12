-- ============================================================================
-- MODEL: fct_ad_performance_daily
-- ============================================================================
-- GRAIN: One row per campaign per day (across all paid channels).
-- PURPOSE: The single, definitive table for all paid media reporting.
--          This is where Metabase connects for spend, CTR, CPC, and CPM.
--
-- METABASE DESIGN:
-- - Pre-calculated metrics (CTR, CPC, CPM) eliminate the need for
--   Metabase custom expressions, reducing query complexity and errors.
-- - Fully denormalized: campaign and date attributes are joined inline
--   so Metabase users never need to configure table relationships.
--
-- EDGE CASES:
-- - Anomalous rows (spend > 0, impressions = 0) are flagged as is_anomaly.
--   Metabase dashboard default filter: is_anomaly = FALSE.
-- - Missing days (API gaps or zero-spend weekends) are NOT filled here.
--   Use Metabase's "Replace missing values with zero" at visualization layer.
--
-- DESIGN NOTE — CTR/CPC/CPM computed here, not in intermediate:
-- - DuckDB 1.5 has a known planner assertion failure when complex division
--   expressions (involving CAST) are evaluated directly over a UNION ALL CTE.
-- - By moving metric computation to this layer (post-JOIN, post-UNION),
--   all column types are fully resolved before division occurs.
-- ============================================================================

{{ config(materialized='table') }}

WITH performance AS (
    SELECT * FROM {{ ref('int_unified_ad_performance') }}
),

campaigns AS (
    SELECT * FROM {{ ref('dim_campaigns') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        -- ── Primary Keys ─────────────────────────────────────────
        perf.campaign_key,
        perf.metric_date AS date_day,

        -- ── Campaign dimensions (denormalized for Metabase) ──────
        camp.channel,
        camp.campaign_name,
        camp.campaign_type_standardized,
        camp.campaign_status,
        camp.is_active,

        -- ── Date dimensions (denormalized for Metabase) ──────────
        dd.year_number,
        dd.quarter_number,
        dd.month_number,
        dd.month_name,
        dd.day_name,
        dd.is_weekend,

        -- ── Core volume metrics ───────────────────────────────────
        perf.daily_impressions,
        perf.daily_clicks,
        perf.daily_spend_usd,

        -- ── Pre-calculated efficiency metrics ────────────────────
        -- Computed here where all types are fully resolved columns
        -- (avoids DuckDB planner crash from division over UNION ALL CTEs)
        CASE
            WHEN perf.daily_impressions = 0 OR perf.daily_impressions IS NULL THEN NULL
            ELSE ROUND(
                CAST(perf.daily_clicks AS DOUBLE) / CAST(perf.daily_impressions AS DOUBLE),
                6
            )
        END AS daily_ctr,

        CASE
            WHEN perf.daily_clicks = 0 OR perf.daily_clicks IS NULL THEN NULL
            ELSE ROUND(
                CAST(perf.daily_spend_usd AS DOUBLE) / CAST(perf.daily_clicks AS DOUBLE),
                4
            )
        END AS daily_cpc_usd,

        CASE
            WHEN perf.daily_impressions = 0 OR perf.daily_impressions IS NULL THEN NULL
            ELSE ROUND(
                (CAST(perf.daily_spend_usd AS DOUBLE) * 1000.0) / CAST(perf.daily_impressions AS DOUBLE),
                4
            )
        END AS daily_cpm_usd,

        -- ── Data quality flag ─────────────────────────────────────
        perf.is_anomaly

    FROM performance AS perf

    LEFT JOIN campaigns AS camp
        ON perf.campaign_key = camp.campaign_key

    LEFT JOIN date_dim AS dd
        ON perf.metric_date = dd.date_day
)

SELECT * FROM final

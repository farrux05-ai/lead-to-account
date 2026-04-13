-- ============================================================================
-- MODEL: fct_web_sessions_daily
-- ============================================================================
-- GRAIN: One row per date + source + medium + campaign.
-- PURPOSE: Website traffic analysis by acquisition channel. This table
--          answers: "Where are our website visitors coming from, and are
--          they engaged?"
--
-- METABASE DASHBOARDS POWERED:
-- - Website Traffic Overview (sessions, bounce rate by channel)
-- - Paid vs Organic Traffic Comparison
-- - Campaign Landing Page Performance
--
-- NOTES:
-- - GA4 sessions are pre-aggregated by source/medium in the staging layer.
-- - Engagement rate = engaged_sessions / sessions. This is the inverse of
--   bounce rate and is the GA4-native metric (bounce rate is deprecated).
-- ============================================================================

{{ config(
    materialized='incremental',
    unique_key=['date_day', 'traffic_source', 'traffic_medium', 'traffic_campaign']
) }}

WITH sessions AS (
    SELECT * FROM {{ ref('stg_ga4__sessions_daily') }}
    {% if is_incremental() %}
    WHERE metric_date >= (SELECT MAX(date_day) FROM {{ this }})
    {% endif %}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        -- ── Keys ────────────────────────────────────────
        sess.metric_date AS date_day,

        -- ── Traffic source dimensions ───────────────────
        COALESCE(sess.utm_source, '(direct)') AS traffic_source,
        COALESCE(sess.utm_medium, '(none)') AS traffic_medium,
        COALESCE(sess.utm_campaign, '(not set)') AS traffic_campaign,

        -- Classify into high-level channel buckets for Metabase grouping
        CASE
            WHEN sess.utm_medium IN ('cpc', 'ppc', 'paid_search') THEN 'Paid Search'
            WHEN sess.utm_medium IN ('paid_social', 'paidsocial') THEN 'Paid Social'
            WHEN sess.utm_medium = 'organic' THEN 'Organic Search'
            WHEN sess.utm_medium = 'email' THEN 'Email'
            WHEN sess.utm_medium = 'referral' THEN 'Referral'
            WHEN sess.utm_medium = 'social' THEN 'Organic Social'
            WHEN sess.utm_medium = '(none)' AND sess.utm_source = '(direct)' THEN 'Direct'
            ELSE 'Other'
        END AS channel_grouping,

        -- ── Date dimensions ─────────────────────────────
        dd.year_number,
        dd.quarter_number,
        dd.month_number,
        dd.month_name,
        dd.is_weekend,

        -- ── Core metrics ────────────────────────────────
        sess.daily_sessions,
        sess.daily_engaged_sessions,
        sess.daily_bounces,

        -- ── Pre-calculated rates ────────────────────────
        {{ safe_divide('sess.daily_engaged_sessions', 'sess.daily_sessions') }} AS engagement_rate,
        {{ safe_divide('sess.daily_bounces', 'sess.daily_sessions') }} AS bounce_rate

    FROM sessions AS sess

    LEFT JOIN date_dim AS dd
        ON sess.metric_date = dd.date_day
)

SELECT * FROM final

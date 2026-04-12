-- ============================================================================
-- MODEL: stg_ga4__sessions_daily
-- GRAIN: One row per date + source + medium + campaign.
-- NOTE: Safe ROW_NUMBER dedup instead of QUALIFY — avoids DuckDB bug.
-- "(not set)" is nullified so downstream COALESCE logic stays clean.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('ga4', 'ga4_sessions_daily') }}
),

renamed_and_typed AS (
    SELECT
        CAST(date AS DATE)                                    AS metric_date,
        NULLIF(CAST(source   AS VARCHAR), '(not set)')        AS utm_source,
        NULLIF(CAST(medium   AS VARCHAR), '(not set)')        AS utm_medium,
        NULLIF(CAST(campaign AS VARCHAR), '(not set)')        AS utm_campaign,
        COALESCE(CAST(sessions          AS INTEGER), 0)       AS daily_sessions,
        COALESCE(CAST(engaged_sessions  AS INTEGER), 0)       AS daily_engaged_sessions,
        COALESCE(CAST(bounces           AS INTEGER), 0)       AS daily_bounces,
        CAST(_loaded_at AS TIMESTAMP)                         AS _loaded_at
    FROM source
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY metric_date, utm_source, utm_medium, utm_campaign
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM renamed_and_typed
)

SELECT
    metric_date,
    utm_source,
    utm_medium,
    utm_campaign,
    daily_sessions,
    daily_engaged_sessions,
    daily_bounces,
    _loaded_at
FROM ranked
WHERE _row_num = 1

-- ============================================================================
-- MODEL: dim_date
-- ============================================================================
-- GRAIN: One row per calendar day.
-- PURPOSE: Universal date dimension that every fact table joins against.
--          Metabase's time-series charts rely on consistent date attributes
--          for grouping, filtering, and drill-down (week, month, quarter).
--
-- WHY A DATE DIMENSION?
-- - Without this, Metabase users must write SQL functions like
--   EXTRACT(MONTH FROM date) in every question — error-prone and slow.
-- - With dim_date, they simply filter/group by "month_name" or "quarter"
--   as a drag-and-drop dimension.
--
-- RANGE: Covers Jan 1, 2025 → Dec 31, 2026 to allow YoY comparisons
--        even though seed data only spans Q1 2026.
-- ============================================================================

{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2025-01-01' AS DATE)",
        end_date="CAST('2026-12-31' AS DATE)"
    ) }}
),

final AS (
    SELECT
        CAST(date_day AS DATE) AS date_day,

        -- ISO calendar attributes
        EXTRACT(YEAR FROM date_day)       AS year_number,
        EXTRACT(QUARTER FROM date_day)    AS quarter_number,
        EXTRACT(MONTH FROM date_day)      AS month_number,
        EXTRACT(WEEK FROM date_day)       AS iso_week_number,
        EXTRACT(DOW FROM date_day)        AS day_of_week_number,
        EXTRACT(DOY FROM date_day)        AS day_of_year,

        -- Human-readable labels (critical for Metabase filter UI)
        CAST(EXTRACT(YEAR FROM date_day) AS VARCHAR) || '-Q' || CAST(EXTRACT(QUARTER FROM date_day) AS VARCHAR) AS year_quarter,
        strftime(date_day, '%Y-%m')       AS year_month,
        monthname(date_day)               AS month_name,
        dayname(date_day)                 AS day_name,

        -- Business flags
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- Relative flags (useful for default Metabase filters)
        CASE
            WHEN date_day = CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_today

    FROM date_spine
)

SELECT * FROM final

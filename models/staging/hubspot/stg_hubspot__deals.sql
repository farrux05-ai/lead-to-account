-- ============================================================================
-- MODEL: stg_hubspot__deals
-- ============================================================================
-- GRAIN: One row per unique deal_id.
-- DEDUP STRATEGY: If the same deal_id exists in both the historical seed and
--   the dlt incremental source, the most recently loaded version wins.
--   This prevents double-counting revenue on the dashboard.
-- ============================================================================

WITH seed_source AS (
    SELECT * FROM {{ source('hubspot', 'hubspot_deals') }}
),

dlt_source AS (
    SELECT * FROM {{ source('hubspot', 'hubspot_deals_incremental') }}
),

unioned AS (
    SELECT 
        deal_id,
        contact_id,
        deal_name,
        deal_stage,
        amount,
        created_at,
        closed_at,
        _loaded_at
    FROM seed_source
    
    UNION ALL
    
    SELECT 
        deal_id,
        contact_id,
        deal_name,
        deal_stage,
        amount,
        created_at,
        closed_at,
        _loaded_at
    FROM dlt_source
),

-- Deduplication: Keep only the most recent version of each deal.
-- DLT records will naturally win as they have a later _loaded_at timestamp.
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY deal_id
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM unioned
)

SELECT
    CAST(deal_id AS VARCHAR)            AS source_deal_id,
    CAST(contact_id AS VARCHAR)         AS source_contact_id,
    CAST(deal_name AS VARCHAR)          AS deal_name,
    CAST(deal_stage AS VARCHAR)         AS deal_stage,
    TRY_CAST(amount AS DOUBLE)          AS amount_usd,
    TRY_CAST(created_at AS TIMESTAMP)   AS created_at,
    TRY_CAST(closed_at AS TIMESTAMP)    AS closed_at,
    TRY_CAST(_loaded_at AS TIMESTAMP)   AS _loaded_at
FROM deduped
WHERE _row_num = 1

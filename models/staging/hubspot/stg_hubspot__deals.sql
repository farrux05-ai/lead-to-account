-- ============================================================================
-- MODEL: stg_hubspot__deals
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
FROM unioned

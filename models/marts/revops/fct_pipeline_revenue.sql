-- ============================================================================
-- MODEL: fct_pipeline_revenue
-- ============================================================================
-- GRAIN: One row per Deal/Opportunity.
-- PURPOSE: The ultimate Revenue Attribution table. Maps closed won revenue
--          back to the original marketing channel that acquired the account.
--          This answers: "Which campaigns generate the most actual money?"
-- ============================================================================

{{ config(materialized='table') }}

WITH deals AS (
    SELECT * FROM {{ ref('stg_hubspot__deals') }}
),

contacts AS (
    SELECT * FROM {{ ref('dim_contacts') }}
),

accounts AS (
    SELECT * FROM {{ ref('dim_accounts') }}
),

deal_attribution AS (
    SELECT
        -- Deal details
        d.source_deal_id AS deal_key,
        d.deal_name,
        d.deal_stage,
        d.amount_usd,
        d.created_at AS deal_created_at,
        d.closed_at AS deal_closed_at,
        d._loaded_at,
        
        -- Entity linkages
        c.contact_key AS associated_contact_key,
        a.account_key,
        a.email_domain AS company_domain,
        
        -- Account-Based Attribution (The Crown Jewel)
        CAST(COALESCE(a.account_original_source, c.original_source, 'UNKNOWN') AS VARCHAR) AS attribution_channel,
        
        -- Flags for easy Metabase filtering
        CAST(CASE WHEN d.deal_stage = 'closed_won' THEN TRUE ELSE FALSE END AS BOOLEAN) AS is_closed_won,
        CAST(CASE WHEN d.deal_stage = 'closed_lost' THEN TRUE ELSE FALSE END AS BOOLEAN) AS is_closed_lost

    FROM deals d
    LEFT JOIN contacts c 
        ON CAST(d.source_contact_id AS VARCHAR) = CAST(c.contact_key AS VARCHAR)
    LEFT JOIN accounts a 
        ON CAST(c.account_key AS VARCHAR) = CAST(a.account_key AS VARCHAR)
)

SELECT * FROM deal_attribution

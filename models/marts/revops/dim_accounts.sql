-- ============================================================================
-- MODEL: dim_accounts
-- ============================================================================
-- GRAIN: One row per Account (B2B Domain).
-- PURPOSE: Virtual B2B Account dimension for Account-Based Marketing (ABM)
--          and B2B Pipeline tracking. Groups individual contacts by their
--          employer (domain) ignoring free email addresses.
-- ============================================================================

{{ config(materialized='table') }}

WITH identity_resolution AS (
    SELECT * FROM {{ ref('int_identity_resolution') }}
    WHERE virtual_account_id IS NOT NULL -- Only B2B accounts
),

account_aggregations AS (
    SELECT
        virtual_account_id AS account_key,
        email_domain,
        account_original_source,
        account_champion_contact_id,
        MIN(created_at) AS account_first_touch_at,
        COUNT(source_contact_id) AS total_contacts_in_account
    FROM identity_resolution
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM account_aggregations

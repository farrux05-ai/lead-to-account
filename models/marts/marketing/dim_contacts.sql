-- ============================================================================
-- MODEL: dim_contacts
-- ============================================================================
-- GRAIN: One row per surviving (non-merged) CRM contact.
-- PURPOSE: The canonical contact/lead dimension for funnel analysis.
--          Updated for B2B RevOps: Includes virtual_account_id link!
-- ============================================================================

{{ config(materialized='table') }}

SELECT
    -- Primary key
    source_contact_id AS contact_key,

    -- Foreign Key to B2B Account
    virtual_account_id AS account_key,

    -- Contact identity
    email,
    first_name,
    last_name,

    -- B2B Classification
    is_free_email,

    -- Funnel stage (cleaned — no NULLs)
    lifecycle_stage,
    account_original_source AS original_source, -- Inherited from Account First Touch!

    -- Timestamps
    created_at

FROM {{ ref('int_identity_resolution') }}

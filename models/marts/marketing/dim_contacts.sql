-- ============================================================================
-- MODEL: dim_contacts
-- ============================================================================
-- GRAIN: One row per surviving (non-merged) CRM contact.
-- PURPOSE: The canonical contact/lead dimension for funnel analysis,
--          lead source attribution, and pipeline reporting.
--
-- WHY CONTACTS MATTER FOR MARKETING:
-- - Every marketing dollar ultimately maps to a person entering the funnel.
--   Without a clean contact dimension, attribution breaks — you can't answer
--   "how many MQLs did Google Ads generate this quarter?"
-- - The merge resolution in int_contacts_resolved ensures we don't inflate
--   lead counts when HubSpot deduplicates records.
-- ============================================================================

{{ config(materialized='table') }}

SELECT
    -- Primary key
    source_contact_id AS contact_key,

    -- Contact identity
    email,
    first_name,
    last_name,

    -- Funnel stage (cleaned — no NULLs)
    lifecycle_stage,
    original_source,

    -- Audit/quality flag
    has_merged_duplicates,

    -- Timestamps
    created_at

FROM {{ ref('int_contacts_resolved') }}

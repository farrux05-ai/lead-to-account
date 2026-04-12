-- ============================================================================
-- MODEL: int_contacts_resolved
-- ============================================================================
-- GRAIN: One row per surviving (non-merged) HubSpot contact.
-- PURPOSE: Resolve merged duplicate contacts so downstream models reference
--          only the canonical "parent" contact record.
--
-- WHY THIS MATTERS:
-- - HubSpot merges duplicate contacts internally but preserves the old
--   record with a pointer (merged_into_contact_id). If we naively count
--   contacts, we double-count every merge — inflating lead volume metrics
--   by 3-5% in a typical B2B SaaS environment.
-- - This model filters out merged children and keeps only the surviving
--   parent, ensuring accurate funnel stage counts in Metabase.
--
-- EDGE CASES HANDLED:
-- - Contacts where merged_into_contact_id IS NOT NULL are excluded.
-- - NULL lifecycle stages are coalesced to 'unknown' to avoid broken
--   GROUP BY in Metabase funnel visualizations.
-- ============================================================================

WITH contacts AS (
    SELECT * FROM {{ ref('stg_hubspot__contacts') }}
),

-- Filter: remove contacts that were merged into another record
-- These are effectively "soft-deleted" duplicates in HubSpot
resolved AS (
    SELECT
        source_contact_id,
        email,
        first_name,
        last_name,

        -- Defensive coalesce: NULL stages break Metabase funnel charts
        COALESCE(lifecycle_stage, 'unknown') AS lifecycle_stage,
        COALESCE(original_source, 'unknown') AS original_source,
        created_at,

        -- Flag: was this contact ever the target of a merge?
        -- Useful for data quality audits and merge rate monitoring
        CASE
            WHEN source_contact_id IN (
                SELECT merged_into_contact_id
                FROM contacts
                WHERE merged_into_contact_id IS NOT NULL
            ) THEN TRUE
            ELSE FALSE
        END AS has_merged_duplicates

    FROM contacts
    WHERE merged_into_contact_id IS NULL
)

SELECT * FROM resolved

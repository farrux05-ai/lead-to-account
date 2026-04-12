-- ============================================================================
-- MODEL: stg_hubspot__contacts
-- GRAIN: One row per HubSpot contact.
-- NOTE: Safe ROW_NUMBER dedup instead of QUALIFY — avoids DuckDB bug.
-- merged_into_contact_id is preserved for merge resolution in int layer.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('hubspot', 'hubspot_contacts') }}
),

renamed_and_typed AS (
    SELECT
        CAST(contact_id AS VARCHAR)              AS source_contact_id,
        CAST(email AS VARCHAR)                   AS email,
        CAST(merged_into_contact_id AS VARCHAR)  AS merged_into_contact_id,
        CAST(first_name AS VARCHAR)              AS first_name,
        CAST(last_name AS VARCHAR)               AS last_name,
        CAST(lifecycle_stage AS VARCHAR)         AS lifecycle_stage,
        CAST(original_source AS VARCHAR)         AS original_source,
        CAST(created_at AS TIMESTAMP)            AS created_at
    FROM source
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_contact_id
            ORDER BY created_at DESC
        ) AS _row_num
    FROM renamed_and_typed
)

SELECT
    source_contact_id,
    email,
    merged_into_contact_id,
    first_name,
    last_name,
    lifecycle_stage,
    original_source,
    created_at
FROM ranked
WHERE _row_num = 1

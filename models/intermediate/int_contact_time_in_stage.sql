-- ============================================================================
-- MODEL: int_contact_time_in_stage
-- ============================================================================
-- GRAIN: One row per contact per funnel stage they have been in.
-- PURPOSE: Analyzes the `snap_hubspot_contacts_lifecycle` snapshot to calculate
--          Pipeline Velocity (how many days a contact spent in MQL, SQL, etc).
-- ============================================================================

{{ config(materialized='table') }}

WITH lifecycle_snapshots AS (
    SELECT * FROM {{ ref('snap_hubspot_contacts_lifecycle') }}
),

stage_durations AS (
    SELECT
        contact_id AS source_contact_id,
        lifecycle_stage,
        
        -- Target timestamps created by dbt snapshot
        dbt_valid_from AS entered_stage_at,
        -- If dbt_valid_to is NULL, it means the contact is CURRENTLY in this stage.
        COALESCE(dbt_valid_to, CURRENT_TIMESTAMP) AS exited_stage_at,
        
        -- Calculate time spent in this stage
        DATE_DIFF('day', CAST(dbt_valid_from AS DATE), CAST(COALESCE(dbt_valid_to, CURRENT_TIMESTAMP) AS DATE)) AS days_in_stage
        
    FROM lifecycle_snapshots
)

SELECT * FROM stage_durations

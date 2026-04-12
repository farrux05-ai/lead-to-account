-- ============================================================================
-- MODEL: stg_segment__tracks
-- GRAIN: One row per unique Segment message_id.
-- NOTE: Safe ROW_NUMBER dedup instead of QUALIFY — avoids DuckDB bug.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('segment', 'segment_tracks') }}
),

renamed_and_typed AS (
    SELECT
        CAST(message_id AS VARCHAR)        AS message_id,
        CAST(user_id AS VARCHAR)           AS user_id,
        CAST(event AS VARCHAR)             AS event_name,
        CAST(timestamp AS TIMESTAMP)       AS event_timestamp,
        CAST(timestamp AS DATE)            AS event_date,
        CAST(_loaded_at AS TIMESTAMP)      AS _loaded_at
    FROM source
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY message_id
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM renamed_and_typed
)

SELECT
    message_id,
    user_id,
    event_name,
    event_timestamp,
    event_date,
    _loaded_at
FROM ranked
WHERE _row_num = 1

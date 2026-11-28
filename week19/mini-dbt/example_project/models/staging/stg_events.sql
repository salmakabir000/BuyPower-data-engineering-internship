-- @column event_sk: Warehouse surrogate key for the event
-- @column event_id: Original GitHub event identifier
-- @column created_at: Timestamp when the event occurred

SELECT
    event_sk,
    event_id,
    event_type_sk,
    actor_sk,
    repo_sk,
    date_sk,
    created_at
FROM {{ source('warehouse', 'fact_events') }}

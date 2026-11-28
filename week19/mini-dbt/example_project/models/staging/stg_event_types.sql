-- @column event_type_sk: Warehouse surrogate key for the event type
-- @column event_type_name: Name of the GitHub event type

SELECT
    event_type_sk,
    event_type_name
FROM {{ source('warehouse', 'dim_event_type') }}

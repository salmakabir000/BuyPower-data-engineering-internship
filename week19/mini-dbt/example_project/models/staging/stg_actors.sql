-- @column actor_sk: Warehouse surrogate key for the actor
-- @column actor_id: Original GitHub actor identifier
-- @column login: GitHub username

SELECT
    actor_sk,
    actor_id,
    login,
    display_name,
    url
FROM {{ source('warehouse', 'dim_actor') }}

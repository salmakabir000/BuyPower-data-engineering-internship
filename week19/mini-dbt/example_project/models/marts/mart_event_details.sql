-- @column event_id: Original GitHub event identifier
-- @column actor_login: Username of the actor
-- @column repo_name: Name of the repository
-- @column event_type: Type of GitHub event
-- @column created_at: Timestamp when the event occurred

SELECT
    e.event_id,
    a.login AS actor_login,
    r.repo_name,
    t.event_type_name AS event_type,
    d.date,
    d.year,
    d.month,
    e.created_at
FROM {{ ref('stg_events') }} e
JOIN {{ ref('stg_actors') }} a
    ON e.actor_sk = a.actor_sk
JOIN {{ ref('stg_repos') }} r
    ON e.repo_sk = r.repo_sk
JOIN {{ ref('stg_event_types') }} t
    ON e.event_type_sk = t.event_type_sk
JOIN {{ ref('stg_dates') }} d
    ON e.date_sk = d.date_sk

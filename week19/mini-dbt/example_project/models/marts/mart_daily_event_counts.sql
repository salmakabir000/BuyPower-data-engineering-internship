-- @column date: Calendar date
-- @column event_count: Number of GitHub events on that date

SELECT
    d.date,
    COUNT(*) AS event_count
FROM {{ ref('stg_events') }} e
JOIN {{ ref('stg_dates') }} d
    ON e.date_sk = d.date_sk
GROUP BY d.date

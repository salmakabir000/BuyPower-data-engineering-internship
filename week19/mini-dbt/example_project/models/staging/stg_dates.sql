-- @column date_sk: Warehouse date key
-- @column date: Calendar date
-- @column is_weekend: Indicates whether the date falls on a weekend

SELECT
    date_sk,
    date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    is_weekend
FROM {{ source('warehouse', 'dim_date') }}

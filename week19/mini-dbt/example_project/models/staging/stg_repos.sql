-- @column repo_sk: Warehouse surrogate key for the repository
-- @column repo_id: Original GitHub repository identifier
-- @column repo_name: Repository name

SELECT
    repo_sk,
    repo_id,
    repo_name,
    repo_url
FROM {{ source('warehouse', 'dim_repo') }}

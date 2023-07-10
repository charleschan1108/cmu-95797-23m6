WITH SOURCE AS (
    SELECT
        *
    FROM 
        {{ source('main', 'fhv_bases') }}
),

RENAMED AS (
    SELECT
        base_number,
        base_name,
        dba,
        dba_category,
        filename
    FROM
        SOURCE
)

SELECT * FROM RENAMED
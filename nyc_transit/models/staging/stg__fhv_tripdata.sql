WITH SOURCE AS (
    SELECT
        *
    FROM 
        {{ source('main', 'fhv_tripdata') }}
),

/* RAW data in PARQUET format, putting data types here for reference only 
    as the data type information is preserved */

RENAMED AS (
    SELECT
        dispatching_base_num,
        pickup_datetime::datetime AS pickup_datetime,
        dropOff_datetime::datetime AS dropOff_datetime,
        PUlocationID::double AS PUlocationID,
        DOlocationID::double AS DOlocationID,
        -- SR_FLAG, -- Exclude COLUMN since all null values
        Affiliated_base_number,
        filename
    FROM 
        SOURCE
)

SELECT * FROM RENAMED
WITH SOURCE AS (
    SELECT
        *
    FROM 
        {{ source('main', 'central_park_weather') }}
),

RENAMED AS (
    SELECT
        station,
        name,
        date::date AS date,
        awnd::double AS awnd,
        prcp::double AS prcp,
        snow::double AS snow,
        snwd::double AS snwd,
        tmax::int AS tmax,
        tmin::int AS tmin,
        filename
    FROM 
        SOURCE
)

SELECT * FROM RENAMED

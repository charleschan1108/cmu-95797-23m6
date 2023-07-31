{%- set temp_directory = './tmp.tmp' -%} -- set temp_directory to avoid out of memory error

-- Methodology: for each zone, count the number of trips when it is the origin and destination zone
-- and sum the two to get the total number of trips

WITH all_zone_trips AS (
    SELECT
        puzone AS zone,
        SUM(trips) AS trips
    FROM {{ ref('mart__fact_trips_by_borough') }}
    GROUP BY ALL
    UNION ALL
    SELECT 
        dozone AS zone,
        SUM(trips) AS trips
    FROM {{ ref('mart__fact_trips_by_borough') }}
    GROUP BY ALL
)

SELECT 
    zone,
    SUM(trips) AS trips
FROM all_zone_trips
GROUP BY ALL
HAVING trips < 100000



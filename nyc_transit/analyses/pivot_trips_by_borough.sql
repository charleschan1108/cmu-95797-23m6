/** 
Outout:

Create pivot table with:
index being pick up borough
columns being drop off borough
values being number of trips
**/

-- Implementation 1: duckdb native function
-- PIVOT {{ ref('mart__fact_trips_by_borough') }} 
-- ON doborough USING SUM(trips) -- column will be drop off borough 
-- GROUP BY puborough -- rows will be pick up borough

-- Implementation 2: dbt utils method
SELECT
      puborough,
      {{ dbt_utils.pivot('doborough', 
                        dbt_utils.get_column_values(ref('mart__fact_trips_by_borough'),
                                                    'doborough'),
                        agg='sum',
                        then_value='trips',
        ) }}
    from {{ ref('mart__fact_trips_by_borough') }} 
    group by puborough
SELECT 
    station,
    name,
    date,
    min(prcp) OVER seven_days as prcpma_7d_min,
    max(prcp) OVER seven_days as prcpma_7d_max,
    avg(prcp) OVER seven_days as prcpma_7d_avg,
    sum(prcp) OVER seven_days as prcpma_7d_sum,
    min(snow) OVER seven_days as snow_ma_7d_min,
    max(snow) OVER seven_days as snow_ma_7d_max,
    avg(snow) OVER seven_days as snow_ma_7d_avg,
    sum(snow) OVER seven_days as snow_ma_7d_sum
FROM {{ ref('stg__central_park_weather') }}
WINDOW seven_days AS (PARTITION BY station ORDER BY date ASC
                    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) -- calculate the 7 day moving average centre on current date
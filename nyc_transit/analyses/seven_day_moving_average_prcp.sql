SELECT 
    station,
    name,
    date,
    AVG(prcp) OVER (PARTITION BY station ORDER BY date 
                    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS ma_7d -- calculate the 7 day moving average centre on current date
FROM {{ ref('stg__central_park_weather') }}
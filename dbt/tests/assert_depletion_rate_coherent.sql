-- Assert that depletion_rate_per_minute is physically coherent
-- It is impossible to lose more bikes in one minute than the station's total capacity

SELECT station_code, station_name, depletion_rate_per_minute, capacity, last_reported
FROM {{ ref('velib_silver') }}
WHERE depletion_rate_per_minute IS NOT NULL
  AND ABS(depletion_rate_per_minute) > capacity

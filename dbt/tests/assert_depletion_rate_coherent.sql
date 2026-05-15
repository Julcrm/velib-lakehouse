-- Vérifie que depletion_rate_per_minute est physiquement cohérent
-- Impossible de vider plus de vélos en 1 minute que la capacité totale de la station

SELECT station_code, station_name, depletion_rate_per_minute, capacity, last_reported
FROM {{ ref('velib_silver') }}
WHERE depletion_rate_per_minute IS NOT NULL
  AND ABS(depletion_rate_per_minute) > capacity

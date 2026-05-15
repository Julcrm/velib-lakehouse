-- Vérifie que capacity est toujours > 0
-- Une station avec capacité zéro est suspecte

SELECT station_code, station_name, capacity, last_reported
FROM {{ ref('velib_silver') }}
WHERE capacity <= 0

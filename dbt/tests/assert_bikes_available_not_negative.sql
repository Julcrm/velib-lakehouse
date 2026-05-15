-- Vérifie que bikes_available est toujours >= 0
-- Une valeur négative serait une anomalie de l'API Vélib

SELECT station_code, station_name, bikes_available, last_reported
FROM {{ ref('velib_silver') }}
WHERE bikes_available < 0

-- Assert that bikes_available is never negative
-- A negative value would indicate a data anomaly from the Vélib API

SELECT station_code, station_name, bikes_available, last_reported
FROM {{ ref('velib_silver') }}
WHERE bikes_available < 0

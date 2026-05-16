-- Assert that capacity is always greater than zero
-- A station with zero capacity is suspicious and likely a data quality issue

SELECT station_code, station_name, capacity, last_reported
FROM {{ ref('velib_silver') }}
WHERE capacity <= 0

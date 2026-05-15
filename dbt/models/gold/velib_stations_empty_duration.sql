-- Stations actives vides depuis plus de 60 minutes sans réapprovisionnement

{{ config(
    materialized='external',
    location='s3://velib-lakehouse/gold/velib/velib_stations_empty_duration.parquet'
) }}


WITH silver AS (
    SELECT * FROM read_parquet('s3://velib-lakehouse/silver/velib/velib_silver.parquet')
    WHERE date = current_date
      AND is_renting = true
),

last_with_bikes AS (
    SELECT
        station_code,
        MAX(last_reported) as last_seen_with_bikes
    FROM silver
    WHERE bikes_available > 0
    GROUP BY station_code
),

currently_empty AS (
    SELECT station_code, station_name, arrondissement, last_reported, bikes_available
    FROM silver
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_code
        ORDER BY last_reported DESC
    ) = 1
),

currently_empty_filtered AS (
    SELECT * FROM currently_empty
    WHERE bikes_available = 0
),

final AS (
    SELECT
        e.station_code,
        e.station_name,
        e.arrondissement,
        l.last_seen_with_bikes,
        e.last_reported as checked_at,
        DATEDIFF('minute', l.last_seen_with_bikes, e.last_reported) as minutes_empty
    FROM currently_empty_filtered e
    LEFT JOIN last_with_bikes l ON e.station_code = l.station_code
    WHERE l.last_seen_with_bikes IS NOT NULL
      AND DATEDIFF('minute', l.last_seen_with_bikes, e.last_reported) > 60
)

SELECT * FROM final
ORDER BY minutes_empty DESC
-- Couche Gold — statistiques agrégées par arrondissement
-- Snapshot le plus récent de chaque station, agrégé par arrondissement

{{ config(
    materialized='external',
    location='s3://velib-lakehouse/gold/velib/velib_stats_arrondissement.parquet'
) }}

WITH silver AS (
    SELECT * FROM read_parquet('s3://velib-lakehouse/silver/velib/velib_silver.parquet')
    WHERE date = current_date
),

latest AS (
    SELECT *
    FROM silver
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_code
        ORDER BY last_reported DESC
    ) = 1
),

aggregated AS (
    SELECT
        arrondissement,
        COUNT(*)                                        AS total_stations,
        SUM(bikes_available)                            AS total_bikes,
        SUM(bikes_mechanical)                           AS total_mechanical,
        SUM(bikes_electric)                             AS total_electric,
        SUM(docks_available)                            AS total_docks_available,
        SUM(capacity)                                   AS total_capacity,
        ROUND(
            100.0 * SUM(bikes_available) / NULLIF(SUM(capacity), 0),
            1
        )                                               AS fill_rate_pct,
        COUNT(*) FILTER (WHERE bikes_available = 0)     AS empty_stations,
        COUNT(*) FILTER (WHERE docks_available = 0)     AS full_stations
    FROM latest
    GROUP BY arrondissement
)

SELECT * FROM aggregated
ORDER BY arrondissement

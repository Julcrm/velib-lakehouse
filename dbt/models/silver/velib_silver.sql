{{ config(
    materialized='external',
    location='s3://velib-lakehouse/silver/velib/velib_silver.parquet'
) }}

-- =============================================================================
-- Model       : velib_silver
-- Description : Cleans and enriches raw Vélib snapshots from the Bronze layer.
--               Filters out ghost stations (NULL stationcode, zero capacity).
--               Computes bikes_delta and depletion_rate_per_minute using LAG
--               window function partitioned by station_code.
-- Source      : s3://velib-lakehouse/bronze/velib/**/*.parquet
-- Output      : s3://velib-lakehouse/silver/velib/velib_silver.parquet
-- =============================================================================

WITH source AS (
    SELECT * FROM read_parquet('s3://velib-lakehouse/bronze/velib/**/*.parquet')
),

cleaned AS (
    SELECT
        -- Identifiers
        stationcode                                     AS station_code,
        name                                            AS station_name,
        nom_arrondissement_communes                     AS arrondissement,
        code_insee_commune                              AS code_insee,

        -- Availability
        numbikesavailable                               AS bikes_available,
        mechanical                                      AS bikes_mechanical,
        ebike                                           AS bikes_electric,
        numdocksavailable                               AS docks_available,
        capacity,

        -- Status — convert OUI/NON strings to booleans
        (is_installed = 'OUI')                          AS is_installed,
        (is_renting = 'OUI')                            AS is_renting,
        (is_returning = 'OUI')                          AS is_returning,

        -- Coordinates
        "coordonnees_geo.lat"                           AS latitude,
        "coordonnees_geo.lon"                           AS longitude,

        -- Timestamps
        CAST(duedate AS TIMESTAMP)                      AS last_reported,
        ingested_at,
        date

    FROM source
    WHERE is_installed = 'OUI'
    AND stationcode IS NOT NULL
    AND name IS NOT NULL
    AND capacity > 0
),

with_delta AS (
    SELECT
        *,
        -- Previous snapshot bike count for this station
        LAG(bikes_available) OVER (
            PARTITION BY station_code
            ORDER BY last_reported
        )                                               AS prev_bikes_available,

        -- Bike count change since the previous snapshot
        bikes_available - LAG(bikes_available) OVER (
            PARTITION BY station_code
            ORDER BY last_reported
        )                                               AS bikes_delta,

        -- Minutes elapsed since the previous snapshot
        DATEDIFF('minute',
            LAG(last_reported) OVER (
                PARTITION BY station_code
                ORDER BY last_reported
            ),
            last_reported
        )                                               AS minutes_since_last_snapshot

    FROM cleaned
),

final AS (
    SELECT
        *,
        -- Depletion rate: bikes lost per minute (negative = depleting, positive = refilling)
        CASE
            WHEN minutes_since_last_snapshot > 0
            THEN ROUND(
                CAST(bikes_delta AS DOUBLE) / minutes_since_last_snapshot,
                4
            )
            ELSE NULL
        END                                             AS depletion_rate_per_minute

    FROM with_delta
)

SELECT * FROM final

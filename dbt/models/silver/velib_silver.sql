-- Couche Silver — nettoyage, typage et enrichissement des données Vélib
-- Calcule la vitesse de vidage par station via window function LAG

{{ config(
    materialized='external',
    location='s3://velib-lakehouse/silver/velib/velib_silver.parquet'
) }}

WITH source AS (
    SELECT * FROM read_parquet('s3://velib-lakehouse/bronze/velib/**/*.parquet')
),

cleaned AS (
    SELECT
        -- Identifiants
        stationcode                                     AS station_code,
        name                                            AS station_name,
        nom_arrondissement_communes                     AS arrondissement,
        code_insee_commune                              AS code_insee,

        -- Disponibilité
        numbikesavailable                               AS bikes_available,
        mechanical                                      AS bikes_mechanical,
        ebike                                           AS bikes_electric,
        numdocksavailable                               AS docks_available,
        capacity,

        -- Statuts — conversion OUI/NON → boolean
        (is_installed = 'OUI')                          AS is_installed,
        (is_renting = 'OUI')                            AS is_renting,
        (is_returning = 'OUI')                          AS is_returning,

        -- Coordonnées
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
),

with_delta AS (
    SELECT
        *,
        -- Snapshot précédent par station
        LAG(bikes_available) OVER (
            PARTITION BY station_code
            ORDER BY last_reported
        )                                               AS prev_bikes_available,

        -- Delta de vélos entre deux snapshots
        bikes_available - LAG(bikes_available) OVER (
            PARTITION BY station_code
            ORDER BY last_reported
        )                                               AS bikes_delta,

        -- Durée en minutes entre deux snapshots
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
        -- Vitesse de vidage : vélos perdus par minute (négatif = vidage, positif = remplissage)
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

-- Couche Gold — stations à risque de vidage imminent
-- Une station est à risque si son taux de vidage prédit un stock nul dans moins de 30 minutes

WITH silver AS (
    SELECT * FROM read_parquet('s3://velib-lakehouse/silver/velib/velib_silver.parquet')
    WHERE date = current_date
),

latest_snapshot AS (
    -- Garde uniquement le dernier snapshot de chaque station
    SELECT *
    FROM silver
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_code
        ORDER BY last_reported DESC
    ) = 1
),

at_risk AS (
    SELECT
        station_code,
        station_name,
        arrondissement,
        latitude,
        longitude,
        bikes_available,
        depletion_rate_per_minute,
        -- Minutes avant vidage estimé
        CASE
            WHEN depletion_rate_per_minute < 0
            THEN ROUND(bikes_available / ABS(depletion_rate_per_minute), 0)
            ELSE NULL
        END                             AS minutes_until_empty,
        last_reported
    FROM latest_snapshot
    WHERE
        bikes_available > 0
        AND depletion_rate_per_minute < 0
        AND (bikes_available / ABS(depletion_rate_per_minute)) < 30
)

SELECT * FROM at_risk
ORDER BY minutes_until_empty ASC

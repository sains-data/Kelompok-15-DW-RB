/*
===============================================================================
Insert Script: Silver Layer Data Population
===============================================================================
Script Purpose:
    Script ini mengisi data dimensi di silver layer berdasarkan data dari 
    bronze.raw_ship_data. Data akan dibersihkan dan dinormalisasi sebelum 
    dimasukkan ke dalam tabel dimensi.
===============================================================================
*/

USE ShipDataWarehouse;
GO

PRINT 'Starting Silver Layer Data Population...';
GO

-- =========================
-- POPULATE DIM_VESSEL
-- =========================
PRINT 'Populating dim_vessel...';
GO

INSERT INTO silver.dim_vessel (
    vessel_id, vessel_name, mmsi, imo_number, vessel_type, flag, 
    year_built, gross_tonnage, deadweight, width_meters, length_meters, draught_meters
)
SELECT DISTINCT
    COALESCE(NULLIF(TRIM(mmsi), ''), 'UNKNOWN') as vessel_id,
    COALESCE(NULLIF(TRIM(vessel_name), ''), 'Unknown Vessel') as vessel_name,
    COALESCE(NULLIF(TRIM(mmsi), ''), 'UNKNOWN') as mmsi,
    COALESCE(NULLIF(TRIM(imo_number), ''), 'UNKNOWN') as imo_number,
    COALESCE(NULLIF(TRIM(vessel_type), ''), 'Unknown') as vessel_type,
    COALESCE(NULLIF(TRIM(flag), ''), 'Unknown') as flag,
    CASE 
        WHEN ISNUMERIC(year_built) = 1 AND CAST(year_built AS INT) BETWEEN 1900 AND YEAR(GETDATE())
        THEN CAST(year_built AS INT)
        ELSE NULL 
    END as year_built,
    CASE 
        WHEN ISNUMERIC(gross_tonnage) = 1 AND CAST(gross_tonnage AS FLOAT) > 0
        THEN CAST(gross_tonnage AS INT)
        ELSE NULL 
    END as gross_tonnage,
    CASE 
        WHEN ISNUMERIC(deadweight) = 1 AND CAST(deadweight AS FLOAT) > 0
        THEN CAST(deadweight AS INT)
        ELSE NULL 
    END as deadweight,
    CASE 
        WHEN ISNUMERIC(width) = 1 AND CAST(width AS FLOAT) > 0
        THEN CAST(width AS DECIMAL(10,2))
        ELSE NULL 
    END as width_meters,
    CASE 
        WHEN ISNUMERIC(length) = 1 AND CAST(length AS FLOAT) > 0
        THEN CAST(length AS DECIMAL(10,2))
        ELSE NULL 
    END as length_meters,
    CASE 
        WHEN ISNUMERIC(draught) = 1 AND CAST(draught AS FLOAT) > 0
        THEN CAST(draught AS DECIMAL(10,2))
        ELSE NULL 
    END as draught_meters
FROM bronze.raw_ship_data
WHERE TRIM(COALESCE(mmsi, '')) <> ''
    AND NOT EXISTS (
        SELECT 1 FROM silver.dim_vessel 
        WHERE vessel_id = COALESCE(NULLIF(TRIM(bronze.raw_ship_data.mmsi), ''), 'UNKNOWN')
            AND is_current = 1
    );
GO

-- =========================
-- POPULATE DIM_TIME
-- =========================
PRINT 'Populating dim_time...';
GO

WITH time_data AS (
    SELECT DISTINCT
        CAST(TRY_CAST(datetime_recorded AS DATETIME) AS DATE) as date_value,
        DATEPART(HOUR, TRY_CAST(datetime_recorded AS DATETIME)) as hour_value,
        DATEPART(MINUTE, TRY_CAST(datetime_recorded AS DATETIME)) as minute_value
    FROM bronze.raw_ship_data
    WHERE TRY_CAST(datetime_recorded AS DATETIME) IS NOT NULL
)
INSERT INTO silver.dim_time (
    date_key, full_date, year, quarter, month, month_name, day, 
    day_of_week, day_name, week_of_year, is_weekend, hour, minute, time_period
)
SELECT DISTINCT
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT) as date_key,
    date_value as full_date,
    YEAR(date_value) as year,
    DATEPART(QUARTER, date_value) as quarter,
    MONTH(date_value) as month,
    DATENAME(MONTH, date_value) as month_name,
    DAY(date_value) as day,
    DATEPART(WEEKDAY, date_value) as day_of_week,
    DATENAME(WEEKDAY, date_value) as day_name,
    DATEPART(WEEK, date_value) as week_of_year,
    CASE WHEN DATEPART(WEEKDAY, date_value) IN (1, 7) THEN 1 ELSE 0 END as is_weekend,
    hour_value as hour,
    minute_value as minute,
    CASE 
        WHEN hour_value BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour_value BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour_value BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END as time_period
FROM time_data
WHERE NOT EXISTS (
    SELECT 1 FROM silver.dim_time 
    WHERE date_key = CAST(FORMAT(time_data.date_value, 'yyyyMMdd') AS INT)
        AND hour = time_data.hour_value 
        AND minute = time_data.minute_value
);
GO

-- =========================
-- POPULATE DIM_ROUTE
-- =========================
PRINT 'Populating dim_route...';
GO

INSERT INTO silver.dim_route (
    route_id, route_name, departure_port, arrival_port, route_category,
    port_country_dep, port_country_arr
)
SELECT DISTINCT
    COALESCE(NULLIF(TRIM(route_name), ''), 
             TRIM(departure_port) + ' to ' + TRIM(arrival_port)) as route_id,
    COALESCE(NULLIF(TRIM(route_name), ''), 
             TRIM(departure_port) + ' to ' + TRIM(arrival_port)) as route_name,
    COALESCE(NULLIF(TRIM(departure_port), ''), 'Unknown') as departure_port,
    COALESCE(NULLIF(TRIM(arrival_port), ''), 'Unknown') as arrival_port,
    CASE 
        WHEN TRIM(departure_port) LIKE '%Indonesia%' OR TRIM(arrival_port) LIKE '%Indonesia%'
            OR TRIM(departure_port) IN ('Surabaya', 'Jakarta', 'Makassar', 'Medan')
            OR TRIM(arrival_port) IN ('Surabaya', 'Jakarta', 'Makassar', 'Medan')
        THEN 'Domestic'
        ELSE 'International'
    END as route_category,
    CASE 
        WHEN TRIM(departure_port) IN ('Surabaya', 'Jakarta', 'Makassar', 'Medan') THEN 'Indonesia'
        WHEN TRIM(departure_port) LIKE '%Klang%' THEN 'Malaysia'
        WHEN TRIM(departure_port) LIKE '%Singapore%' THEN 'Singapore'
        ELSE 'Unknown'
    END as port_country_dep,
    CASE 
        WHEN TRIM(arrival_port) IN ('Surabaya', 'Jakarta', 'Makassar', 'Medan', 'Belawan', 'Tanjung Priok') THEN 'Indonesia'
        WHEN TRIM(arrival_port) LIKE '%Klang%' THEN 'Malaysia'
        WHEN TRIM(arrival_port) LIKE '%Singapore%' THEN 'Singapore'
        ELSE 'Unknown'
    END as port_country_arr
FROM bronze.raw_ship_data
WHERE TRIM(COALESCE(departure_port, '')) <> '' 
    AND TRIM(COALESCE(arrival_port, '')) <> ''
    AND NOT EXISTS (
        SELECT 1 FROM silver.dim_route 
        WHERE route_id = COALESCE(NULLIF(TRIM(bronze.raw_ship_data.route_name), ''), 
                                  TRIM(bronze.raw_ship_data.departure_port) + ' to ' + TRIM(bronze.raw_ship_data.arrival_port))
    );
GO

-- =========================
-- POPULATE DIM_WEATHER
-- =========================
PRINT 'Populating dim_weather...';
GO

INSERT INTO silver.dim_weather (
    weather_id, wind_direction, wind_speed_knots, wind_category, 
    wave_height_meters, wave_category, air_temperature_c, temp_category, weather_condition
)
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY 
        COALESCE(TRY_CAST(wind_direction AS DECIMAL(6,2)), 0),
        COALESCE(TRY_CAST(wind_speed_knots AS DECIMAL(6,2)), 0),
        COALESCE(TRY_CAST(wave_height_meters AS DECIMAL(6,2)), 0),
        COALESCE(TRY_CAST(air_temperature_c AS DECIMAL(6,2)), 0)
    ) as weather_id,
    CASE 
        WHEN ISNUMERIC(wind_direction) = 1 AND CAST(wind_direction AS FLOAT) BETWEEN 0 AND 360
        THEN CAST(wind_direction AS DECIMAL(6,2))
        ELSE NULL 
    END as wind_direction,
    CASE 
        WHEN ISNUMERIC(wind_speed_knots) = 1 AND CAST(wind_speed_knots AS FLOAT) >= 0
        THEN CAST(wind_speed_knots AS DECIMAL(6,2))
        ELSE NULL 
    END as wind_speed_knots,
    CASE 
        WHEN ISNUMERIC(wind_speed_knots) = 1 THEN
            CASE 
                WHEN CAST(wind_speed_knots AS FLOAT) < 1 THEN 'Calm'
                WHEN CAST(wind_speed_knots AS FLOAT) < 7 THEN 'Light'
                WHEN CAST(wind_speed_knots AS FLOAT) < 17 THEN 'Moderate'
                WHEN CAST(wind_speed_knots AS FLOAT) < 28 THEN 'Strong'
                ELSE 'Gale'
            END
        ELSE 'Unknown'
    END as wind_category,
    CASE 
        WHEN ISNUMERIC(wave_height_meters) = 1 AND CAST(wave_height_meters AS FLOAT) >= 0
        THEN CAST(wave_height_meters AS DECIMAL(6,2))
        ELSE NULL 
    END as wave_height_meters,
    CASE 
        WHEN ISNUMERIC(wave_height_meters) = 1 THEN
            CASE 
                WHEN CAST(wave_height_meters AS FLOAT) < 0.5 THEN 'Calm'
                WHEN CAST(wave_height_meters AS FLOAT) < 1.5 THEN 'Moderate'
                WHEN CAST(wave_height_meters AS FLOAT) < 3.0 THEN 'Rough'
                ELSE 'Very Rough'
            END
        ELSE 'Unknown'
    END as wave_category,
    CASE 
        WHEN ISNUMERIC(air_temperature_c) = 1 AND CAST(air_temperature_c AS FLOAT) BETWEEN -50 AND 60
        THEN CAST(air_temperature_c AS DECIMAL(6,2))
        ELSE NULL 
    END as air_temperature_c,
    CASE 
        WHEN ISNUMERIC(air_temperature_c) = 1 THEN
            CASE 
                WHEN CAST(air_temperature_c AS FLOAT) < 10 THEN 'Cold'
                WHEN CAST(air_temperature_c AS FLOAT) < 20 THEN 'Cool'
                WHEN CAST(air_temperature_c AS FLOAT) < 30 THEN 'Moderate'
                WHEN CAST(air_temperature_c AS FLOAT) < 35 THEN 'Warm'
                ELSE 'Hot'
            END
        ELSE 'Unknown'
    END as temp_category,
    'Clear' as weather_condition  -- Default, bisa diperluas berdasarkan kombinasi faktor cuaca
FROM bronze.raw_ship_data
WHERE (ISNUMERIC(wind_direction) = 1 OR ISNUMERIC(wind_speed_knots) = 1 
       OR ISNUMERIC(wave_height_meters) = 1 OR ISNUMERIC(air_temperature_c) = 1);
GO

-- =========================
-- POPULATE DIM_POSITION
-- =========================
PRINT 'Populating dim_position...';
GO

INSERT INTO silver.dim_position (
    position_id, latitude, longitude, region, water_body, 
    proximity_to_port, country_waters
)
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY 
        COALESCE(TRY_CAST(latitude AS DECIMAL(10,6)), 0),
        COALESCE(TRY_CAST(longitude AS DECIMAL(10,6)), 0)
    ) as position_id,
    CASE 
        WHEN ISNUMERIC(latitude) = 1 AND CAST(latitude AS FLOAT) BETWEEN -90 AND 90
        THEN CAST(latitude AS DECIMAL(10,6))
        ELSE NULL 
    END as latitude,
    CASE 
        WHEN ISNUMERIC(longitude) = 1 AND CAST(longitude AS FLOAT) BETWEEN -180 AND 180
        THEN CAST(longitude AS DECIMAL(10,6))
        ELSE NULL 
    END as longitude,
    CASE 
        WHEN ISNUMERIC(latitude) = 1 AND ISNUMERIC(longitude) = 1 THEN
            CASE 
                WHEN CAST(latitude AS FLOAT) BETWEEN -10 AND 10 
                     AND CAST(longitude AS FLOAT) BETWEEN 95 AND 140
                THEN 'Southeast Asia'
                WHEN CAST(latitude AS FLOAT) BETWEEN -6 AND 6 
                     AND CAST(longitude AS FLOAT) BETWEEN 95 AND 141
                THEN 'Indonesian Waters'
                ELSE 'Other'
            END
        ELSE 'Unknown'
    END as region,
    CASE 
        WHEN ISNUMERIC(latitude) = 1 AND ISNUMERIC(longitude) = 1 THEN
            CASE 
                WHEN CAST(latitude AS FLOAT) BETWEEN -6 AND -5 
                     AND CAST(longitude AS FLOAT) BETWEEN 105 AND 107
                THEN 'Java Sea'
                WHEN CAST(latitude AS FLOAT) BETWEEN 1 AND 6 
                     AND CAST(longitude AS FLOAT) BETWEEN 100 AND 104
                THEN 'Malacca Strait'
                WHEN CAST(latitude AS FLOAT) BETWEEN -5 AND 0 
                     AND CAST(longitude AS FLOAT) BETWEEN 119 AND 125
                THEN 'Celebes Sea'
                ELSE 'Open Waters'
            END
        ELSE 'Unknown'
    END as water_body,
    CASE 
        WHEN departure_port IN ('Surabaya', 'Belawan', 'Tanjung Priok', 'Makassar', 'Port Klang')
             OR arrival_port IN ('Surabaya', 'Belawan', 'Tanjung Priok', 'Makassar', 'Port Klang')
        THEN 'Near Port'
        ELSE 'Open Sea'
    END as proximity_to_port,
    CASE 
        WHEN ISNUMERIC(latitude) = 1 AND ISNUMERIC(longitude) = 1 THEN
            CASE 
                WHEN CAST(latitude AS FLOAT) BETWEEN -11 AND 6 
                     AND CAST(longitude AS FLOAT) BETWEEN 95 AND 141
                THEN 'Indonesia'
                WHEN CAST(latitude AS FLOAT) BETWEEN 1 AND 7 
                     AND CAST(longitude AS FLOAT) BETWEEN 99 AND 119
                THEN 'Malaysia'
                WHEN CAST(latitude AS FLOAT) BETWEEN 1 AND 2 
                     AND CAST(longitude AS FLOAT) BETWEEN 103 AND 104
                THEN 'Singapore'
                ELSE 'International Waters'
            END
        ELSE 'Unknown'
    END as country_waters
FROM bronze.raw_ship_data
WHERE (ISNUMERIC(latitude) = 1 AND ISNUMERIC(longitude) = 1);
GO

-- =========================
-- POPULATE DIM_SENSOR
-- =========================
PRINT 'Populating dim_sensor...';
GO

INSERT INTO silver.dim_sensor (
    sensor_id, sensor_type, sensor_status, last_calibration_date, calibration_interval_days
)
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY 
        COALESCE(NULLIF(TRIM(sensor_type), ''), 'Unknown'),
        COALESCE(NULLIF(TRIM(sensor_status), ''), 'Unknown')
    ) as sensor_id,
    COALESCE(NULLIF(TRIM(sensor_type), ''), 'Unknown Sensor') as sensor_type,
    COALESCE(NULLIF(TRIM(sensor_status), ''), 'Unknown') as sensor_status,
    TRY_CAST(last_calibration_date AS DATE) as last_calibration_date,
    CASE 
        WHEN TRIM(sensor_type) LIKE '%Engine%' THEN 30
        WHEN TRIM(sensor_type) LIKE '%Fuel%' THEN 90
        WHEN TRIM(sensor_type) LIKE '%AIS%' THEN 365
        WHEN TRIM(sensor_type) LIKE '%Gyroscope%' THEN 180
        WHEN TRIM(sensor_type) LIKE '%Anemometer%' THEN 180
        ELSE 365
    END as calibration_interval_days
FROM bronze.raw_ship_data
WHERE TRIM(COALESCE(sensor_type, '')) <> '';
GO

-- =========================
-- POPULATE DIM_NAVIGATION
-- =========================
PRINT 'Populating dim_navigation...';
GO

INSERT INTO silver.dim_navigation (
    navigation_id, navigational_status, sog, cog, heading, rotation,
    speed_category, heading_category, maneuver_type
)
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY 
        COALESCE(NULLIF(TRIM(navigational_status), ''), 'Unknown'),
        COALESCE(TRY_CAST(sog AS DECIMAL(6,2)), 0),
        COALESCE(TRY_CAST(cog AS DECIMAL(6,2)), 0)
    ) as navigation_id,
    COALESCE(NULLIF(TRIM(navigational_status), ''), 'Unknown') as navigational_status,
    CASE 
        WHEN ISNUMERIC(sog) = 1 AND CAST(sog AS FLOAT) >= 0
        THEN CAST(sog AS DECIMAL(6,2))
        ELSE NULL 
    END as sog,
    CASE 
        WHEN ISNUMERIC(cog) = 1 AND CAST(cog AS FLOAT) BETWEEN 0 AND 360
        THEN CAST(cog AS DECIMAL(6,2))
        ELSE NULL 
    END as cog,
    CASE 
        WHEN ISNUMERIC(heading) = 1 AND CAST(heading AS FLOAT) BETWEEN 0 AND 360
        THEN CAST(heading AS DECIMAL(6,2))
        ELSE NULL 
    END as heading,
    CASE 
        WHEN ISNUMERIC(rotation) = 1 
        THEN CAST(rotation AS DECIMAL(8,2))
        ELSE NULL 
    END as rotation,
    CASE 
        WHEN ISNUMERIC(sog) = 1 THEN
            CASE 
                WHEN CAST(sog AS FLOAT) < 5 THEN 'Slow'
                WHEN CAST(sog AS FLOAT) < 15 THEN 'Normal'
                ELSE 'Fast'
            END
        ELSE 'Unknown'
    END as speed_category,
    CASE 
        WHEN ISNUMERIC(heading) = 1 THEN
            CASE 
                WHEN CAST(heading AS FLOAT) BETWEEN 315 AND 360 OR CAST(heading AS FLOAT) BETWEEN 0 AND 45 THEN 'North'
                WHEN CAST(heading AS FLOAT) BETWEEN 45 AND 135 THEN 'East'
                WHEN CAST(heading AS FLOAT) BETWEEN 135 AND 225 THEN 'South'
                WHEN CAST(heading AS FLOAT) BETWEEN 225 AND 315 THEN 'West'
                ELSE 'Unknown'
            END
        ELSE 'Unknown'
    END as heading_category,
    CASE 
        WHEN TRIM(navigational_status) LIKE '%anchor%' THEN 'Anchored'
        WHEN TRIM(navigational_status) LIKE '%Moored%' THEN 'Moored'
        WHEN ISNUMERIC(rotation) = 1 AND ABS(CAST(rotation AS FLOAT)) > 10 THEN 'Turning'
        WHEN ISNUMERIC(sog) = 1 AND CAST(sog AS FLOAT) > 1 THEN 'Underway'
        ELSE 'Stationary'
    END as maneuver_type
FROM bronze.raw_ship_data
WHERE TRIM(COALESCE(navigational_status, '')) <> '';
GO

-- =========================
-- DATA QUALITY SUMMARY
-- =========================
PRINT 'Silver layer data population completed!';
PRINT '========================================';
GO

SELECT 'dim_vessel' as table_name, COUNT(*) as record_count FROM silver.dim_vessel
UNION ALL
SELECT 'dim_time', COUNT(*) FROM silver.dim_time
UNION ALL
SELECT 'dim_route', COUNT(*) FROM silver.dim_route
UNION ALL
SELECT 'dim_weather', COUNT(*) FROM silver.dim_weather
UNION ALL
SELECT 'dim_position', COUNT(*) FROM silver.dim_position
UNION ALL
SELECT 'dim_sensor', COUNT(*) FROM silver.dim_sensor
UNION ALL
SELECT 'dim_navigation', COUNT(*) FROM silver.dim_navigation
ORDER BY table_name;
GO

PRINT 'Silver Layer ETL Process Completed Successfully!';
GO
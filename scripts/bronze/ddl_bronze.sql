/*
===============================================================================
DDL Script: Membuat Bronze Tables untuk Data Warehouse Kapal
===============================================================================
Tujuan Script:
    Script ini membuat tabel dalam skema 'bronze' untuk proyek "Perancangan-
    Arsitektur-Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-Berbasis-Big-
    Data".
===============================================================================
*/

-- =========================
-- Raw Data Table
-- =========================

IF OBJECT_ID('bronze.raw_ship_data', 'U') IS NOT NULL
    DROP TABLE bronze.raw_ship_data;
GO

CREATE TABLE bronze.raw_ship_data (
    id                      INT,
    datetime_recorded       DATETIME,
    mmsi                    NVARCHAR(50),
    navigational_status     NVARCHAR(50),
    sog                     FLOAT,
    cog                     FLOAT,
    heading                 FLOAT,
    vessel_type             NVARCHAR(50),
    width                   FLOAT,
    length                  FLOAT,
    draught                 FLOAT,
    rotation                FLOAT,
    speed                   FLOAT,
    rudder                  FLOAT,
    wind_direction          FLOAT,
    wind_speed_knots        FLOAT,
    class_value             INT,
    vessel_sk               INT,
    time_sk                 INT,
    route_sk                INT,
    weather_sk              INT,
    position_sk             INT,
    sensor_sk               INT,
    navigation_sk           INT,
    fuel_consumption_liters FLOAT,
    ship_speed_knots        FLOAT,
    engine_temperature_c    FLOAT,
    travel_distance_miles   FLOAT,
    latitude                FLOAT,
    longitude               FLOAT,
    wave_height_meters      FLOAT,
    air_temperature_c       FLOAT,
    departure_port          NVARCHAR(100),
    arrival_port            NVARCHAR(100),
    route_name              NVARCHAR(100),
    vessel_name             NVARCHAR(100),
    imo_number              NVARCHAR(50),
    flag                    NVARCHAR(50),
    year_built              INT,
    gross_tonnage           INT,
    deadweight              INT,
    sensor_type             NVARCHAR(50),
    sensor_status           NVARCHAR(50),
    last_calibration_date   DATE
);
GO
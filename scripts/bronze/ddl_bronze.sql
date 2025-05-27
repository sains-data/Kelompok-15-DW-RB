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

USE ShipDataWarehouse;
GO

IF OBJECT_ID('bronze.raw_ship_data', 'U') IS NOT NULL
    DROP TABLE bronze.raw_ship_data;
GO

CREATE TABLE bronze.raw_ship_data (
    id                      INT,
    datetime_recorded       NVARCHAR(50),        -- Ubah dari DATETIME ke NVARCHAR dulu
    mmsi                    NVARCHAR(100),       -- Perbesar ukuran
    navigational_status     NVARCHAR(100),       -- Perbesar ukuran
    sog                     NVARCHAR(50),        -- Ubah ke NVARCHAR dulu
    cog                     NVARCHAR(50),
    heading                 NVARCHAR(50),
    vessel_type             NVARCHAR(100),
    width                   NVARCHAR(50),
    length                  NVARCHAR(50),
    draught                 NVARCHAR(50),
    rotation                NVARCHAR(50),
    speed                   NVARCHAR(50),
    rudder                  NVARCHAR(50),
    wind_direction          NVARCHAR(50),
    wind_speed_knots        NVARCHAR(50),
    class_value             NVARCHAR(50),
    vessel_sk               NVARCHAR(50),
    time_sk                 NVARCHAR(50),
    route_sk                NVARCHAR(50),
    weather_sk              NVARCHAR(50),
    position_sk             NVARCHAR(50),
    sensor_sk               NVARCHAR(50),
    navigation_sk           NVARCHAR(50),
    fuel_consumption_liters NVARCHAR(50),
    ship_speed_knots        NVARCHAR(50),
    engine_temperature_c    NVARCHAR(50),
    travel_distance_miles   NVARCHAR(50),
    latitude                NVARCHAR(50),
    longitude               NVARCHAR(50),
    wave_height_meters      NVARCHAR(50),
    air_temperature_c       NVARCHAR(50),
    departure_port          NVARCHAR(200),
    arrival_port            NVARCHAR(200),
    route_name              NVARCHAR(200),
    vessel_name             NVARCHAR(200),
    imo_number              NVARCHAR(100),
    flag                    NVARCHAR(100),
    year_built              NVARCHAR(50),
    gross_tonnage           NVARCHAR(50),
    deadweight              NVARCHAR(50),
    sensor_type             NVARCHAR(200),
    sensor_status           NVARCHAR(100),
    last_calibration_date   NVARCHAR(50)
);
GO
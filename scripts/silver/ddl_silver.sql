/*
===============================================================================
DDL Script: Silver Layer - Dimensional Tables
===============================================================================
Script Purpose:
    Script ini membuat tabel dimensi untuk silver layer dalam proyek 
    "Perancangan-Arsitektur-Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-
    Berbasis-Big-Data". Silver layer berisi data yang sudah dibersihkan dan 
    dinormalisasi dari bronze layer.
===============================================================================
*/

USE ShipDataWarehouse;
GO

-- =========================
-- DIMENSION TABLES
-- =========================

-- Drop existing tables jika ada
IF OBJECT_ID('silver.dim_vessel', 'U') IS NOT NULL DROP TABLE silver.dim_vessel;
IF OBJECT_ID('silver.dim_time', 'U') IS NOT NULL DROP TABLE silver.dim_time;
IF OBJECT_ID('silver.dim_route', 'U') IS NOT NULL DROP TABLE silver.dim_route;
IF OBJECT_ID('silver.dim_weather', 'U') IS NOT NULL DROP TABLE silver.dim_weather;
IF OBJECT_ID('silver.dim_position', 'U') IS NOT NULL DROP TABLE silver.dim_position;
IF OBJECT_ID('silver.dim_sensor', 'U') IS NOT NULL DROP TABLE silver.dim_sensor;
IF OBJECT_ID('silver.dim_navigation', 'U') IS NOT NULL DROP TABLE silver.dim_navigation;
GO

-- =========================
-- Dimension Vessel (Kapal)
-- =========================
CREATE TABLE silver.dim_vessel (
    vessel_sk           INT IDENTITY(1,1) PRIMARY KEY,
    vessel_id           NVARCHAR(50) NOT NULL,
    vessel_name         NVARCHAR(200),
    mmsi                NVARCHAR(100),
    imo_number          NVARCHAR(100),
    vessel_type         NVARCHAR(100),
    flag                NVARCHAR(100),
    year_built          INT,
    gross_tonnage       INT,
    deadweight          INT,
    width_meters        DECIMAL(10,2),
    length_meters       DECIMAL(10,2),
    draught_meters      DECIMAL(10,2),
    
    -- SCD Type 2 fields
    effective_date      DATE NOT NULL DEFAULT GETDATE(),
    expiry_date         DATE NULL,
    is_current          BIT NOT NULL DEFAULT 1,
    
    -- Audit fields
    created_date        DATETIME2 NOT NULL DEFAULT GETDATE(),
    modified_date       DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    UNIQUE (vessel_id, effective_date)
);
GO

-- =========================
-- Dimension Time (Waktu)
-- =========================
CREATE TABLE silver.dim_time (
    time_sk             INT IDENTITY(1,1) PRIMARY KEY,
    date_key            INT NOT NULL,              -- YYYYMMDD
    full_date           DATE NOT NULL,
    year                INT NOT NULL,
    quarter             INT NOT NULL,
    month               INT NOT NULL,
    month_name          NVARCHAR(50) NOT NULL,
    day                 INT NOT NULL,
    day_of_week         INT NOT NULL,
    day_name            NVARCHAR(50) NOT NULL,
    week_of_year        INT NOT NULL,
    is_weekend          BIT NOT NULL,
    is_holiday          BIT NOT NULL DEFAULT 0,
    
    -- Time specific fields
    hour                INT NOT NULL,
    minute              INT NOT NULL,
    time_period         NVARCHAR(20) NOT NULL,      -- Morning, Afternoon, Evening, Night
    
    UNIQUE (date_key, hour, minute)
);
GO

-- =========================
-- Dimension Route (Rute)
-- =========================
CREATE TABLE silver.dim_route (
    route_sk            INT IDENTITY(1,1) PRIMARY KEY,
    route_id            NVARCHAR(50) NOT NULL,
    route_name          NVARCHAR(200),
    departure_port      NVARCHAR(200),
    arrival_port        NVARCHAR(200),
    route_distance_km   DECIMAL(10,2),
    estimated_duration_hours DECIMAL(8,2),
    route_category      NVARCHAR(50),              -- Domestic, International
    port_country_dep    NVARCHAR(100),
    port_country_arr    NVARCHAR(100),
    
    -- Audit fields
    created_date        DATETIME2 NOT NULL DEFAULT GETDATE(),
    modified_date       DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    UNIQUE (route_id)
);
GO

-- =========================
-- Dimension Weather (Cuaca)
-- =========================
CREATE TABLE silver.dim_weather (
    weather_sk          INT IDENTITY(1,1) PRIMARY KEY,
    weather_id          NVARCHAR(50) NOT NULL,
    wind_direction      DECIMAL(6,2),
    wind_speed_knots    DECIMAL(6,2),
    wind_category       NVARCHAR(50),              -- Calm, Light, Moderate, Strong, Gale
    wave_height_meters  DECIMAL(6,2),
    wave_category       NVARCHAR(50),              -- Calm, Moderate, Rough, Very Rough
    air_temperature_c   DECIMAL(6,2),
    temp_category       NVARCHAR(50),              -- Cold, Cool, Moderate, Warm, Hot
    weather_condition   NVARCHAR(100),             -- Clear, Cloudy, Rainy, Storm
    visibility_km       DECIMAL(6,2),
    
    -- Audit fields
    created_date        DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    UNIQUE (weather_id)
);
GO

-- =========================
-- Dimension Position (Posisi)
-- =========================
CREATE TABLE silver.dim_position (
    position_sk         INT IDENTITY(1,1) PRIMARY KEY,
    position_id         NVARCHAR(50) NOT NULL,
    latitude            DECIMAL(10,6),
    longitude           DECIMAL(10,6),
    region              NVARCHAR(100),              -- Southeast Asia, etc.
    water_body          NVARCHAR(100),              -- Java Sea, Malacca Strait, etc.
    proximity_to_port   NVARCHAR(100),              -- Near Port, Open Sea, Coastal
    navigation_zone     NVARCHAR(100),              -- Shipping Lane, Anchorage, etc.
    country_waters      NVARCHAR(100),
    
    -- Audit fields
    created_date        DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    UNIQUE (position_id)
);
GO

-- =========================
-- Dimension Sensor (Sensor)
-- =========================
CREATE TABLE silver.dim_sensor (
    sensor_sk           INT IDENTITY(1,1) PRIMARY KEY,
    sensor_id           NVARCHAR(50) NOT NULL,
    sensor_type         NVARCHAR(200),
    sensor_status       NVARCHAR(100),
    last_calibration_date DATE,
    calibration_interval_days INT,
    sensor_accuracy     NVARCHAR(50),
    manufacturer        NVARCHAR(100),
    model               NVARCHAR(100),
    installation_date   DATE,
    
    -- SCD Type 2 fields
    effective_date      DATE NOT NULL DEFAULT GETDATE(),
    expiry_date         DATE NULL,
    is_current          BIT NOT NULL DEFAULT 1,
    
    -- Audit fields
    created_date        DATETIME2 NOT NULL DEFAULT GETDATE(),
    modified_date       DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    UNIQUE (sensor_id, effective_date)
);
GO

-- =========================
-- Dimension Navigation (Navigasi)
-- =========================
CREATE TABLE silver.dim_navigation (
    navigation_sk       INT IDENTITY(1,1) PRIMARY KEY,
    navigation_id       NVARCHAR(50) NOT NULL,
    navigational_status NVARCHAR(100),
    sog                 DECIMAL(6,2),               -- Speed Over Ground
    cog                 DECIMAL(6,2),               -- Course Over Ground
    heading             DECIMAL(6,2),
    rotation            DECIMAL(8,2),               -- Rate of Turn
    speed_category      NVARCHAR(50),              -- Slow, Normal, Fast
    heading_category    NVARCHAR(50),              -- North, South, East, West, etc.
    maneuver_type       NVARCHAR(100),             -- Straight, Turning, Anchored, etc.
    
    -- Audit fields
    created_date        DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    UNIQUE (navigation_id)
);
GO

-- =========================
-- CREATE INDEXES
-- =========================

-- Vessel dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_vessel_mmsi ON silver.dim_vessel (mmsi);
CREATE NONCLUSTERED INDEX IX_dim_vessel_type ON silver.dim_vessel (vessel_type);
CREATE NONCLUSTERED INDEX IX_dim_vessel_flag ON silver.dim_vessel (flag);
CREATE NONCLUSTERED INDEX IX_dim_vessel_current ON silver.dim_vessel (is_current);
GO

-- Time dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_time_date ON silver.dim_time (full_date);
CREATE NONCLUSTERED INDEX IX_dim_time_year_month ON silver.dim_time (year, month);
CREATE NONCLUSTERED INDEX IX_dim_time_day_of_week ON silver.dim_time (day_of_week);
GO

-- Route dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_route_ports ON silver.dim_route (departure_port, arrival_port);
CREATE NONCLUSTERED INDEX IX_dim_route_category ON silver.dim_route (route_category);
GO

-- Weather dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_weather_wind ON silver.dim_weather (wind_speed_knots);
CREATE NONCLUSTERED INDEX IX_dim_weather_wave ON silver.dim_weather (wave_height_meters);
CREATE NONCLUSTERED INDEX IX_dim_weather_temp ON silver.dim_weather (air_temperature_c);
GO

-- Position dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_position_coordinates ON silver.dim_position (latitude, longitude);
CREATE NONCLUSTERED INDEX IX_dim_position_region ON silver.dim_position (region);
GO

-- Sensor dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_sensor_type ON silver.dim_sensor (sensor_type);
CREATE NONCLUSTERED INDEX IX_dim_sensor_status ON silver.dim_sensor (sensor_status);
CREATE NONCLUSTERED INDEX IX_dim_sensor_current ON silver.dim_sensor (is_current);
GO

-- Navigation dimension indexes
CREATE NONCLUSTERED INDEX IX_dim_navigation_status ON silver.dim_navigation (navigational_status);
CREATE NONCLUSTERED INDEX IX_dim_navigation_speed ON silver.dim_navigation (sog);
GO

PRINT 'Silver layer dimension tables created successfully!';
GO

-- =========================
-- INSERT DEFAULT/UNKNOWN RECORDS
-- =========================

-- Unknown Vessel
INSERT INTO silver.dim_vessel (vessel_id, vessel_name, mmsi, vessel_type) 
VALUES ('UNKNOWN', 'Unknown Vessel', 'UNKNOWN', 'Unknown');

-- Unknown Route
INSERT INTO silver.dim_route (route_id, route_name, departure_port, arrival_port) 
VALUES ('UNKNOWN', 'Unknown Route', 'Unknown', 'Unknown');

-- Unknown Weather
INSERT INTO silver.dim_weather (weather_id, weather_condition) 
VALUES ('UNKNOWN', 'Unknown Weather');

-- Unknown Position
INSERT INTO silver.dim_position (position_id, region) 
VALUES ('UNKNOWN', 'Unknown Region');

-- Unknown Sensor
INSERT INTO silver.dim_sensor (sensor_id, sensor_type, sensor_status) 
VALUES ('UNKNOWN', 'Unknown Sensor', 'Unknown');

-- Unknown Navigation
INSERT INTO silver.dim_navigation (navigation_id, navigational_status) 
VALUES ('UNKNOWN', 'Unknown Status');

PRINT 'Default unknown records inserted successfully!';
GO
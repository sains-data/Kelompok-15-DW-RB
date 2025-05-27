/*
===============================================================================
DDL Script: Membuat Bronze Tables untuk Data Warehouse Kapal
===============================================================================
Tujuan Script:
    Script ini membuat tabel dalam skema 'bronze' untuk proyek “Perancangan-
    Arsitektur-Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-Berbasis-Big-
    Data”.
===============================================================================
*/

-- =========================
-- Dimension Tables
-- =========================

IF OBJECT_ID('bronze.dim_time', 'U') IS NOT NULL
    DROP TABLE bronze.dim_time;
GO

CREATE TABLE bronze.dim_time (
    Time_SK INT PRIMARY KEY,
    FullDate DATE,
    Hour INT,
    DayOfWeek VARCHAR(20),
    Month INT,
    MonthName VARCHAR(20),
    Quarter INT,
    Year INT,
    Season VARCHAR(20)
);
GO

IF OBJECT_ID('bronze.dim_vessel', 'U') IS NOT NULL
    DROP TABLE bronze.dim_vessel;
GO

CREATE TABLE bronze.dim_vessel (
    Vessel_SK INT PRIMARY KEY,
    IMO_Number VARCHAR(50),
    VesselName VARCHAR(100),
    VesselType VARCHAR(50),
    FleetName VARCHAR(50)
);
GO

IF OBJECT_ID('bronze.dim_route', 'U') IS NOT NULL
    DROP TABLE bronze.dim_route;
GO

CREATE TABLE bronze.dim_route (
    Route_SK INT PRIMARY KEY,
    RouteName VARCHAR(100),
    RouteType VARCHAR(50),
    ComplexityLevel VARCHAR(50)
);
GO

IF OBJECT_ID('bronze.dim_position', 'U') IS NOT NULL
    DROP TABLE bronze.dim_position;
GO

CREATE TABLE bronze.dim_position (
    Position_SK INT PRIMARY KEY,
    Latitude FLOAT,
    Longitude FLOAT,
    PortOfOrigin VARCHAR(100),
    PortOfDestination VARCHAR(100)
);
GO

IF OBJECT_ID('bronze.dim_sensor', 'U') IS NOT NULL
    DROP TABLE bronze.dim_sensor;
GO

CREATE TABLE bronze.dim_sensor (
    Sensor_SK INT PRIMARY KEY,
    SensorType VARCHAR(50),
    SensorStatus VARCHAR(50),
    OperationalCondition VARCHAR(100)
);
GO

IF OBJECT_ID('bronze.dim_weather', 'U') IS NOT NULL
    DROP TABLE bronze.dim_weather;
GO

CREATE TABLE bronze.dim_weather (
    Weather_SK INT PRIMARY KEY,
    WeatherCondition VARCHAR(50),
    WindSpeed_ms FLOAT,
    Temperature_C FLOAT,
    WaveHeight_m FLOAT
);
GO

IF OBJECT_ID('bronze.dim_navigation', 'U') IS NOT NULL
    DROP TABLE bronze.dim_navigation;
GO

CREATE TABLE bronze.dim_navigation (
    Navigation_SK INT PRIMARY KEY,
    NavigationSource VARCHAR(50),
    Course VARCHAR(50),
    SeaCondition VARCHAR(50)
);
GO
/*
===============================================================================
DDL Script: Gold Layer - Fact Tables (Aggregated)
===============================================================================
Script Purpose:
    Script ini membuat tabel fakta teraggregasi untuk gold layer dalam proyek 
    "Perancangan-Arsitektur-Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-
    Berbasis-Big-Data". Gold layer berisi data yang sudah diagregasi untuk 
    keperluan analisis dan reporting yang cepat.
===============================================================================
*/

USE ShipDataWarehouse;
GO

-- =========================
-- FACT TABLES (AGGREGATED)
-- =========================

-- Drop existing tables jika ada
IF OBJECT_ID('gold.fact_ship_performance_daily', 'U') IS NOT NULL DROP TABLE gold.fact_ship_performance_daily;
IF OBJECT_ID('gold.fact_route_analytics', 'U') IS NOT NULL DROP TABLE gold.fact_route_analytics;
IF OBJECT_ID('gold.fact_fuel_consumption_summary', 'U') IS NOT NULL DROP TABLE gold.fact_fuel_consumption_summary;
IF OBJECT_ID('gold.fact_weather_impact', 'U') IS NOT NULL DROP TABLE gold.fact_weather_impact;
IF OBJECT_ID('gold.fact_port_traffic', 'U') IS NOT NULL DROP TABLE gold.fact_port_traffic;
IF OBJECT_ID('gold.fact_vessel_utilization', 'U') IS NOT NULL DROP TABLE gold.fact_vessel_utilization;
IF OBJECT_ID('gold.fact_sensor_health', 'U') IS NOT NULL DROP TABLE gold.fact_sensor_health;
IF OBJECT_ID('gold.fact_navigation_efficiency', 'U') IS NOT NULL DROP TABLE gold.fact_navigation_efficiency;
GO

-- =========================
-- FACT SHIP PERFORMANCE DAILY
-- =========================
CREATE TABLE gold.fact_ship_performance_daily (
    performance_sk          BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    vessel_sk               INT NOT NULL,
    date_sk                 INT NOT NULL,
    route_sk                INT,
    
    -- Date Information
    fact_date               DATE NOT NULL,
    year                    INT NOT NULL,
    month                   INT NOT NULL,
    quarter                 INT NOT NULL,
    
    -- Performance Metrics
    total_distance_miles    DECIMAL(12,2),
    avg_speed_knots         DECIMAL(8,2),
    max_speed_knots         DECIMAL(8,2),
    min_speed_knots         DECIMAL(8,2),
    
    -- Fuel Consumption
    total_fuel_liters       DECIMAL(12,2),
    avg_fuel_per_mile       DECIMAL(10,4),
    fuel_efficiency_rating  NVARCHAR(20),      -- Excellent, Good, Average, Poor
    
    -- Engine Performance
    avg_engine_temp_c       DECIMAL(8,2),
    max_engine_temp_c       DECIMAL(8,2),
    engine_overheating_mins INT DEFAULT 0,
    engine_efficiency_pct   DECIMAL(5,2),
    
    -- Operational Metrics
    sailing_hours           DECIMAL(8,2),
    anchored_hours          DECIMAL(8,2),
    moored_hours            DECIMAL(8,2),
    operational_efficiency  DECIMAL(5,2),      -- Percentage of time in motion
    
    -- Weather Impact
    avg_wave_height_m       DECIMAL(6,2),
    avg_wind_speed_knots    DECIMAL(6,2),
    severe_weather_hours    DECIMAL(6,2),
    weather_delay_hours     DECIMAL(6,2),
    
    -- Quality Metrics
    data_quality_score      DECIMAL(5,2),      -- 0-100 based on sensor availability
    record_count            INT,
    missing_data_pct        DECIMAL(5,2),
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_ship_performance_daily UNIQUE (vessel_sk, fact_date)
);
GO

-- =========================
-- FACT ROUTE ANALYTICS
-- =========================
CREATE TABLE gold.fact_route_analytics (
    route_analytics_sk      BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    route_sk                INT NOT NULL,
    month_sk                INT NOT NULL,
    
    -- Time Period
    analysis_year           INT NOT NULL,
    analysis_month          INT NOT NULL,
    analysis_quarter        INT NOT NULL,
    
    -- Route Performance
    total_voyages           INT,
    total_vessels           INT,
    avg_voyage_duration_hrs DECIMAL(8,2),
    min_voyage_duration_hrs DECIMAL(8,2),
    max_voyage_duration_hrs DECIMAL(8,2),
    
    -- Distance & Speed
    avg_distance_miles      DECIMAL(10,2),
    avg_speed_knots         DECIMAL(8,2),
    optimal_speed_knots     DECIMAL(8,2),
    speed_variance          DECIMAL(8,4),
    
    -- Fuel Analytics
    avg_fuel_consumption    DECIMAL(12,2),
    fuel_efficiency_rating  NVARCHAR(20),
    best_fuel_efficiency    DECIMAL(10,4),
    worst_fuel_efficiency   DECIMAL(10,4),
    
    -- Weather Conditions
    avg_weather_conditions  NVARCHAR(100),
    weather_delays          INT,
    severe_weather_voyages  INT,
    
    -- Route Optimization
    on_time_arrivals        INT,
    delayed_arrivals        INT,
    early_arrivals          INT,
    on_time_percentage      DECIMAL(5,2),
    
    -- Cost Analysis
    estimated_fuel_cost_usd DECIMAL(15,2),
    avg_cost_per_mile       DECIMAL(10,4),
    cost_efficiency_rating  NVARCHAR(20),
    
    -- Capacity Utilization
    vessel_capacity_used    DECIMAL(5,2),      -- Percentage
    cargo_efficiency        DECIMAL(5,2),
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_route_analytics UNIQUE (route_sk, analysis_year, analysis_month)
);
GO

-- =========================
-- FACT FUEL CONSUMPTION SUMMARY
-- =========================
CREATE TABLE gold.fact_fuel_consumption_summary (
    fuel_summary_sk         BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    vessel_sk               INT NOT NULL,
    route_sk                INT,
    weather_sk              INT,
    month_sk                INT NOT NULL,
    
    -- Time Period
    summary_year            INT NOT NULL,
    summary_month           INT NOT NULL,
    summary_quarter         INT NOT NULL,
    
    -- Fuel Consumption Metrics
    total_fuel_liters       DECIMAL(15,2),
    avg_hourly_consumption  DECIMAL(10,2),
    peak_consumption_hour   DECIMAL(10,2),
    min_consumption_hour    DECIMAL(10,2),
    
    -- Efficiency Metrics
    fuel_per_mile           DECIMAL(10,4),
    fuel_per_hour           DECIMAL(10,4),
    efficiency_vs_baseline  DECIMAL(8,4),      -- Percentage difference from optimal
    efficiency_trend        NVARCHAR(20),      -- Improving, Stable, Declining
    
    -- Operating Conditions Impact
    fuel_in_calm_weather    DECIMAL(12,2),
    fuel_in_rough_weather   DECIMAL(12,2),
    weather_impact_pct      DECIMAL(8,4),
    
    -- Speed Impact
    fuel_at_low_speed       DECIMAL(12,2),      -- < 10 knots
    fuel_at_medium_speed    DECIMAL(12,2),      -- 10-15 knots
    fuel_at_high_speed      DECIMAL(12,2),      -- > 15 knots
    optimal_speed_range     NVARCHAR(50),
    
    -- Cost Analysis
    estimated_fuel_cost_usd DECIMAL(15,2),
    cost_per_mile           DECIMAL(10,4),
    monthly_budget_variance DECIMAL(10,2),     -- Positive = over budget
    
    -- Environmental Impact
    estimated_co2_tons      DECIMAL(10,2),
    emission_efficiency     DECIMAL(8,4),
    green_score             DECIMAL(5,2),       -- 0-100 environmental rating
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_fuel_summary UNIQUE (vessel_sk, summary_year, summary_month)
);
GO

-- =========================
-- FACT WEATHER IMPACT
-- =========================
CREATE TABLE gold.fact_weather_impact (
    weather_impact_sk       BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    weather_sk              INT NOT NULL,
    route_sk                INT NOT NULL,
    month_sk                INT NOT NULL,
    
    -- Time Period
    impact_year             INT NOT NULL,
    impact_month            INT NOT NULL,
    impact_quarter          INT NOT NULL,
    
    -- Weather Conditions
    avg_wind_speed          DECIMAL(6,2),
    max_wind_speed          DECIMAL(6,2),
    avg_wave_height         DECIMAL(6,2),
    max_wave_height         DECIMAL(6,2),
    avg_temperature         DECIMAL(6,2),
    
    -- Impact on Operations
    weather_delay_hours     DECIMAL(8,2),
    speed_reduction_pct     DECIMAL(5,2),
    fuel_increase_pct       DECIMAL(5,2),
    route_diversions        INT,
    emergency_stops         INT,
    
    -- Safety Metrics
    safety_incidents        INT,
    near_miss_events        INT,
    weather_warnings        INT,
    safety_score            DECIMAL(5,2),       -- 0-100
    
    -- Economic Impact
    delay_cost_usd          DECIMAL(12,2),
    extra_fuel_cost_usd     DECIMAL(12,2),
    total_weather_cost_usd  DECIMAL(12,2),
    
    -- Vessel Count
    affected_vessels        INT,
    total_vessel_hours      DECIMAL(10,2),
    severe_weather_hours    DECIMAL(8,2),
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_weather_impact UNIQUE (weather_sk, route_sk, impact_year, impact_month)
);
GO

-- =========================
-- FACT PORT TRAFFIC
-- =========================
CREATE TABLE gold.fact_port_traffic (
    port_traffic_sk         BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Port Information
    port_name               NVARCHAR(200) NOT NULL,
    port_country            NVARCHAR(100),
    port_type               NVARCHAR(50),       -- Departure, Arrival, Both
    
    -- Time Period
    traffic_date            DATE NOT NULL,
    year                    INT NOT NULL,
    month                   INT NOT NULL,
    quarter                 INT NOT NULL,
    day_of_week             INT NOT NULL,
    
    -- Traffic Metrics
    vessel_arrivals         INT DEFAULT 0,
    vessel_departures       INT DEFAULT 0,
    total_vessel_movements  INT DEFAULT 0,
    unique_vessels          INT DEFAULT 0,
    
    -- Vessel Types
    cargo_vessels           INT DEFAULT 0,
    fishing_vessels         INT DEFAULT 0,
    passenger_vessels       INT DEFAULT 0,
    military_vessels        INT DEFAULT 0,
    other_vessels           INT DEFAULT 0,
    
    -- Capacity Metrics
    total_gross_tonnage     BIGINT DEFAULT 0,
    avg_vessel_size         DECIMAL(10,2),
    max_vessel_size         INT,
    capacity_utilization    DECIMAL(5,2),
    
    -- Timing Analysis
    avg_port_time_hours     DECIMAL(8,2),
    min_port_time_hours     DECIMAL(8,2),
    max_port_time_hours     DECIMAL(8,2),
    peak_traffic_hour       INT,
    
    -- Efficiency Metrics
    on_schedule_arrivals    INT DEFAULT 0,
    delayed_arrivals        INT DEFAULT 0,
    early_arrivals          INT DEFAULT 0,
    port_efficiency_score   DECIMAL(5,2),
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_port_traffic UNIQUE (port_name, traffic_date)
);
GO

-- =========================
-- FACT VESSEL UTILIZATION
-- =========================
CREATE TABLE gold.fact_vessel_utilization (
    utilization_sk          BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    vessel_sk               INT NOT NULL,
    month_sk                INT NOT NULL,
    
    -- Time Period
    utilization_year        INT NOT NULL,
    utilization_month       INT NOT NULL,
    utilization_quarter     INT NOT NULL,
    
    -- Utilization Metrics
    total_operational_hours DECIMAL(10,2),
    sailing_hours           DECIMAL(10,2),
    anchored_hours          DECIMAL(10,2),
    moored_hours            DECIMAL(10,2),
    maintenance_hours       DECIMAL(10,2),
    idle_hours              DECIMAL(10,2),
    
    -- Utilization Percentages
    sailing_percentage      DECIMAL(5,2),
    anchored_percentage     DECIMAL(5,2),
    moored_percentage       DECIMAL(5,2),
    maintenance_percentage  DECIMAL(5,2),
    idle_percentage         DECIMAL(5,2),
    overall_utilization     DECIMAL(5,2),
    
    -- Performance Metrics
    total_distance_covered  DECIMAL(12,2),
    avg_daily_distance      DECIMAL(8,2),
    total_fuel_consumed     DECIMAL(12,2),
    avg_daily_fuel          DECIMAL(8,2),
    
    -- Routes and Ports
    unique_routes_served    INT,
    unique_ports_visited    INT,
    total_voyages           INT,
    completed_voyages       INT,
    cancelled_voyages       INT,
    
    -- Efficiency Scores
    fuel_efficiency_score   DECIMAL(5,2),       -- 0-100
    time_efficiency_score   DECIMAL(5,2),       -- 0-100
    route_efficiency_score  DECIMAL(5,2),       -- 0-100
    overall_efficiency      DECIMAL(5,2),       -- 0-100
    
    -- Maintenance Impact
    scheduled_maintenance   INT,
    unscheduled_maintenance INT,
    maintenance_cost_impact DECIMAL(12,2),
    availability_percentage DECIMAL(5,2),
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_vessel_utilization UNIQUE (vessel_sk, utilization_year, utilization_month)
);
GO

-- =========================
-- FACT SENSOR HEALTH
-- =========================
CREATE TABLE gold.fact_sensor_health (
    sensor_health_sk        BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    vessel_sk               INT NOT NULL,
    sensor_sk               INT NOT NULL,
    month_sk                INT NOT NULL,
    
    -- Time Period
    health_year             INT NOT NULL,
    health_month            INT NOT NULL,
    health_quarter          INT NOT NULL,
    
    -- Health Metrics
    total_readings          INT,
    successful_readings     INT,
    failed_readings         INT,
    accuracy_percentage     DECIMAL(5,2),
    uptime_percentage       DECIMAL(5,2),
    
    -- Maintenance Metrics
    calibration_events      INT,
    maintenance_events      INT,
    replacement_events      INT,
    days_since_calibration  INT,
    days_until_next_service INT,
    
    -- Performance Indicators
    data_quality_score      DECIMAL(5,2),       -- 0-100
    reliability_score       DECIMAL(5,2),       -- 0-100
    performance_trend       NVARCHAR(20),       -- Improving, Stable, Declining
    
    -- Alert Metrics
    warning_alerts          INT,
    critical_alerts         INT,
    maintenance_alerts      INT,
    calibration_alerts      INT,
    
    -- Cost Impact
    maintenance_cost_usd    DECIMAL(10,2),
    downtime_cost_usd       DECIMAL(10,2),
    replacement_cost_usd    DECIMAL(10,2),
    total_cost_impact       DECIMAL(12,2),
    
    -- Operational Impact
    data_gaps_hours         DECIMAL(8,2),
    operational_impact      NVARCHAR(50),       -- None, Low, Medium, High, Critical
    backup_sensor_usage     DECIMAL(5,2),       -- Percentage of time
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_sensor_health UNIQUE (vessel_sk, sensor_sk, health_year, health_month)
);
GO

-- =========================
-- FACT NAVIGATION EFFICIENCY
-- =========================
CREATE TABLE gold.fact_navigation_efficiency (
    nav_efficiency_sk       BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    vessel_sk               INT NOT NULL,
    route_sk                INT NOT NULL,
    navigation_sk           INT NOT NULL,
    month_sk                INT NOT NULL,
    
    -- Time Period
    efficiency_year         INT NOT NULL,
    efficiency_month        INT NOT NULL,
    efficiency_quarter      INT NOT NULL,
    
    -- Navigation Metrics
    total_voyage_time       DECIMAL(10,2),      -- Hours
    actual_distance         DECIMAL(10,2),      -- Miles
    optimal_distance        DECIMAL(10,2),      -- Miles
    distance_efficiency     DECIMAL(5,2),       -- Percentage
    
    -- Speed Analysis
    avg_speed               DECIMAL(8,2),
    optimal_speed           DECIMAL(8,2),
    speed_variance          DECIMAL(8,4),
    speed_efficiency        DECIMAL(5,2),
    
    -- Course Efficiency
    course_deviations       INT,
    avg_course_deviation    DECIMAL(6,2),       -- Degrees
    navigation_accuracy     DECIMAL(5,2),       -- Percentage
    
    -- Maneuver Analysis
    total_maneuvers         INT,
    sharp_turns             INT,
    unnecessary_turns       INT,
    maneuver_efficiency     DECIMAL(5,2),
    
    -- Fuel Impact
    extra_fuel_due_to_nav   DECIMAL(10,2),      -- Liters
    navigation_fuel_cost    DECIMAL(10,2),      -- USD
    fuel_waste_percentage   DECIMAL(5,2),
    
    -- Time Efficiency
    planned_arrival_time    DATETIME2,
    actual_arrival_time     DATETIME2,
    time_deviation_hours    DECIMAL(8,2),
    schedule_efficiency     DECIMAL(5,2),
    
    -- Weather Response
    weather_route_changes   INT,
    storm_avoidance_miles   DECIMAL(8,2),
    weather_delay_hours     DECIMAL(6,2),
    weather_response_score  DECIMAL(5,2),
    
    -- Overall Efficiency
    navigation_score        DECIMAL(5,2),       -- 0-100 composite score
    efficiency_grade        NVARCHAR(2),        -- A+, A, B+, B, C+, C, D, F
    improvement_potential   DECIMAL(5,2),       -- Percentage
    
    -- Audit Fields
    created_date            DATETIME2 NOT NULL DEFAULT GETDATE(),
    last_updated            DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT UQ_nav_efficiency UNIQUE (vessel_sk, route_sk, efficiency_year, efficiency_month)
);
GO

-- =========================
-- CREATE INDEXES FOR PERFORMANCE
-- =========================

-- Ship Performance Daily Indexes
CREATE NONCLUSTERED INDEX IX_ship_performance_vessel_date ON gold.fact_ship_performance_daily (vessel_sk, fact_date);
CREATE NONCLUSTERED INDEX IX_ship_performance_date ON gold.fact_ship_performance_daily (fact_date);
CREATE NONCLUSTERED INDEX IX_ship_performance_route ON gold.fact_ship_performance_daily (route_sk);
GO

-- Route Analytics Indexes
CREATE NONCLUSTERED INDEX IX_route_analytics_route_month ON gold.fact_route_analytics (route_sk, analysis_year, analysis_month);
CREATE NONCLUSTERED INDEX IX_route_analytics_period ON gold.fact_route_analytics (analysis_year, analysis_quarter);
GO

-- Fuel Consumption Indexes
CREATE NONCLUSTERED INDEX IX_fuel_summary_vessel_period ON gold.fact_fuel_consumption_summary (vessel_sk, summary_year, summary_month);
CREATE NONCLUSTERED INDEX IX_fuel_summary_route ON gold.fact_fuel_consumption_summary (route_sk);
GO

-- Weather Impact Indexes
CREATE NONCLUSTERED INDEX IX_weather_impact_route_period ON gold.fact_weather_impact (route_sk, impact_year, impact_month);
CREATE NONCLUSTERED INDEX IX_weather_impact_weather ON gold.fact_weather_impact (weather_sk);
GO

-- Port Traffic Indexes
CREATE NONCLUSTERED INDEX IX_port_traffic_port_date ON gold.fact_port_traffic (port_name, traffic_date);
CREATE NONCLUSTERED INDEX IX_port_traffic_date ON gold.fact_port_traffic (traffic_date);
GO

-- Vessel Utilization Indexes
CREATE NONCLUSTERED INDEX IX_vessel_utilization_vessel_period ON gold.fact_vessel_utilization (vessel_sk, utilization_year, utilization_month);
CREATE NONCLUSTERED INDEX IX_vessel_utilization_efficiency ON gold.fact_vessel_utilization (overall_efficiency);
GO

-- Sensor Health Indexes
CREATE NONCLUSTERED INDEX IX_sensor_health_vessel_sensor ON gold.fact_sensor_health (vessel_sk, sensor_sk);
CREATE NONCLUSTERED INDEX IX_sensor_health_period ON gold.fact_sensor_health (health_year, health_month);
GO

-- Navigation Efficiency Indexes
CREATE NONCLUSTERED INDEX IX_nav_efficiency_vessel_route ON gold.fact_navigation_efficiency (vessel_sk, route_sk);
CREATE NONCLUSTERED INDEX IX_nav_efficiency_period ON gold.fact_navigation_efficiency (efficiency_year, efficiency_month);
CREATE NONCLUSTERED INDEX IX_nav_efficiency_score ON gold.fact_navigation_efficiency (navigation_score);
GO

PRINT 'Gold layer fact tables created successfully!';
PRINT 'Tables created:';
PRINT '- fact_ship_performance_daily';
PRINT '- fact_route_analytics';
PRINT '- fact_fuel_consumption_summary';
PRINT '- fact_weather_impact';
PRINT '- fact_port_traffic';
PRINT '- fact_vessel_utilization';
PRINT '- fact_sensor_health';
PRINT '- fact_navigation_efficiency';
GO
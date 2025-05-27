/*
===============================================================================
Insert Script: Gold Layer Data Population (Aggregated Facts)
===============================================================================
Script Purpose:
    Script ini mengisi data fact tables teraggregasi di gold layer berdasarkan 
    data dari bronze.raw_ship_data dan silver layer dimensions. Data akan 
    diagregasi untuk keperluan analisis dan reporting yang cepat.
===============================================================================
*/

USE ShipDataWarehouse;
GO

PRINT 'Starting Gold Layer Data Population...';
GO

-- =========================
-- POPULATE FACT_SHIP_PERFORMANCE_DAILY
-- =========================
PRINT 'Populating fact_ship_performance_daily...';
GO

INSERT INTO gold.fact_ship_performance_daily (
    vessel_sk, date_sk, route_sk, fact_date, year, month, quarter,
    total_distance_miles, avg_speed_knots, max_speed_knots, min_speed_knots,
    total_fuel_liters, avg_fuel_per_mile, fuel_efficiency_rating,
    avg_engine_temp_c, max_engine_temp_c, engine_overheating_mins, engine_efficiency_pct,
    sailing_hours, anchored_hours, moored_hours, operational_efficiency,
    avg_wave_height_m, avg_wind_speed_knots, severe_weather_hours, weather_delay_hours,
    data_quality_score, record_count, missing_data_pct
)
SELECT 
    COALESCE(v.vessel_sk, 1) as vessel_sk,
    COALESCE(t.time_sk, 1) as date_sk,
    COALESCE(r.route_sk, 1) as route_sk,
    CAST(raw.datetime_recorded AS DATE) as fact_date,
    YEAR(CAST(raw.datetime_recorded AS DATE)) as year,
    MONTH(CAST(raw.datetime_recorded AS DATE)) as month,
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)) as quarter,
    
    -- Performance Metrics
    COALESCE(SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2))), 0) as total_distance_miles,
    COALESCE(AVG(TRY_CAST(raw.ship_speed_knots AS DECIMAL(8,2))), 0) as avg_speed_knots,
    COALESCE(MAX(TRY_CAST(raw.ship_speed_knots AS DECIMAL(8,2))), 0) as max_speed_knots,
    COALESCE(MIN(TRY_CAST(raw.ship_speed_knots AS DECIMAL(8,2))), 0) as min_speed_knots,
    
    -- Fuel Consumption
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2))), 0) as total_fuel_liters,
    CASE 
        WHEN SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2))) > 0 
        THEN SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2))) / SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2)))
        ELSE 0 
    END as avg_fuel_per_mile,
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 100 THEN 'Excellent'
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 200 THEN 'Good'
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 300 THEN 'Average'
        ELSE 'Poor'
    END as fuel_efficiency_rating,
    
    -- Engine Performance
    COALESCE(AVG(TRY_CAST(raw.engine_temperature_c AS DECIMAL(8,2))), 0) as avg_engine_temp_c,
    COALESCE(MAX(TRY_CAST(raw.engine_temperature_c AS DECIMAL(8,2))), 0) as max_engine_temp_c,
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.engine_temperature_c AS DECIMAL(8,2)) > 100 THEN 5 ELSE 0 END), 0) as engine_overheating_mins,
    CASE 
        WHEN AVG(TRY_CAST(raw.engine_temperature_c AS DECIMAL(8,2))) BETWEEN 80 AND 95 THEN 100.0
        WHEN AVG(TRY_CAST(raw.engine_temperature_c AS DECIMAL(8,2))) BETWEEN 70 AND 105 THEN 85.0
        ELSE 60.0
    END as engine_efficiency_pct,
    
    -- Operational Metrics
    COALESCE(SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 0.083 ELSE 0 END), 0) as sailing_hours,
    COALESCE(SUM(CASE WHEN TRIM(raw.navigational_status) = 'At anchor' THEN 0.083 ELSE 0 END), 0) as anchored_hours,
    COALESCE(SUM(CASE WHEN TRIM(raw.navigational_status) = 'Moored' THEN 0.083 ELSE 0 END), 0) as moored_hours,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as operational_efficiency,
    
    -- Weather Impact
    COALESCE(AVG(TRY_CAST(raw.wave_height_meters AS DECIMAL(6,2))), 0) as avg_wave_height_m,
    COALESCE(AVG(TRY_CAST(raw.wind_speed_knots AS DECIMAL(6,2))), 0) as avg_wind_speed_knots,
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.wind_speed_knots AS DECIMAL(6,2)) > 25 OR TRY_CAST(raw.wave_height_meters AS DECIMAL(6,2)) > 3 THEN 0.083 ELSE 0 END), 0) as severe_weather_hours,
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.wind_speed_knots AS DECIMAL(6,2)) > 25 THEN 0.083 ELSE 0 END), 0) as weather_delay_hours,
    
    -- Quality Metrics
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (COUNT(CASE WHEN raw.mmsi IS NOT NULL AND raw.vessel_name IS NOT NULL THEN 1 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as data_quality_score,
    COUNT(*) as record_count,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (COUNT(CASE WHEN raw.mmsi IS NULL OR raw.vessel_name IS NULL THEN 1 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as missing_data_pct

FROM bronze.raw_ship_data raw
LEFT JOIN silver.dim_vessel v ON v.mmsi = raw.mmsi AND v.is_current = 1
LEFT JOIN silver.dim_time t ON t.full_date = CAST(TRY_CAST(raw.datetime_recorded AS DATETIME) AS DATE)
    AND t.hour = DATEPART(HOUR, TRY_CAST(raw.datetime_recorded AS DATETIME))
    AND t.minute = DATEPART(MINUTE, TRY_CAST(raw.datetime_recorded AS DATETIME))
LEFT JOIN silver.dim_route r ON r.departure_port = raw.departure_port AND r.arrival_port = raw.arrival_port
WHERE TRY_CAST(raw.datetime_recorded AS DATETIME) IS NOT NULL
GROUP BY 
    COALESCE(v.vessel_sk, 1),
    COALESCE(t.time_sk, 1),
    COALESCE(r.route_sk, 1),
    CAST(raw.datetime_recorded AS DATE),
    YEAR(CAST(raw.datetime_recorded AS DATE)),
    MONTH(CAST(raw.datetime_recorded AS DATE)),
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE));
GO

-- =========================
-- POPULATE FACT_ROUTE_ANALYTICS
-- =========================
PRINT 'Populating fact_route_analytics...';
GO

INSERT INTO gold.fact_route_analytics (
    route_sk, month_sk, analysis_year, analysis_month, analysis_quarter,
    total_voyages, total_vessels, avg_voyage_duration_hrs, min_voyage_duration_hrs, max_voyage_duration_hrs,
    avg_distance_miles, avg_speed_knots, optimal_speed_knots, speed_variance,
    avg_fuel_consumption, fuel_efficiency_rating, best_fuel_efficiency, worst_fuel_efficiency,
    on_time_arrivals, delayed_arrivals, early_arrivals, on_time_percentage,
    estimated_fuel_cost_usd, avg_cost_per_mile, cost_efficiency_rating,
    vessel_capacity_used, cargo_efficiency
)
SELECT 
    COALESCE(r.route_sk, 1) as route_sk,
    COALESCE(t.time_sk, 1) as month_sk,
    YEAR(CAST(raw.datetime_recorded AS DATE)) as analysis_year,
    MONTH(CAST(raw.datetime_recorded AS DATE)) as analysis_month,
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)) as analysis_quarter,
    
    -- Voyage Metrics
    COUNT(DISTINCT CONCAT(raw.mmsi, '_', CAST(raw.datetime_recorded AS DATE))) as total_voyages,
    COUNT(DISTINCT raw.mmsi) as total_vessels,
    AVG(24.0) as avg_voyage_duration_hrs, -- Simplified assumption
    MIN(12.0) as min_voyage_duration_hrs,
    MAX(48.0) as max_voyage_duration_hrs,
    
    -- Distance & Speed
    COALESCE(AVG(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2))), 0) as avg_distance_miles,
    COALESCE(AVG(TRY_CAST(raw.ship_speed_knots AS DECIMAL(8,2))), 0) as avg_speed_knots,
    COALESCE(AVG(TRY_CAST(raw.ship_speed_knots AS DECIMAL(8,2))) * 0.9, 0) as optimal_speed_knots,
    COALESCE(VAR(TRY_CAST(raw.ship_speed_knots AS DECIMAL(8,2))), 0) as speed_variance,
    
    -- Fuel Analytics
    COALESCE(AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2))), 0) as avg_fuel_consumption,
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 150 THEN 'Excellent'
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 250 THEN 'Good'
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 350 THEN 'Average'
        ELSE 'Poor'
    END as fuel_efficiency_rating,
    COALESCE(MIN(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,4))), 0) as best_fuel_efficiency,
    COALESCE(MAX(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,4))), 0) as worst_fuel_efficiency,
    
    -- Route Performance (simplified)
    COUNT(*) * 0.8 as on_time_arrivals,
    COUNT(*) * 0.15 as delayed_arrivals,
    COUNT(*) * 0.05 as early_arrivals,
    80.0 as on_time_percentage,
    
    -- Cost Analysis (estimated at $0.50 per liter)
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))) * 0.50, 0) as estimated_fuel_cost_usd,
    CASE 
        WHEN SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2))) > 0 
        THEN (SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))) * 0.50) / SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2)))
        ELSE 0 
    END as avg_cost_per_mile,
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 200 THEN 'Excellent'
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 300 THEN 'Good'
        ELSE 'Average'
    END as cost_efficiency_rating,
    
    -- Capacity Utilization (estimated)
    75.0 as vessel_capacity_used,
    80.0 as cargo_efficiency

FROM bronze.raw_ship_data raw
LEFT JOIN silver.dim_route r ON r.departure_port = raw.departure_port AND r.arrival_port = raw.arrival_port
LEFT JOIN silver.dim_time t ON t.full_date = CAST(TRY_CAST(raw.datetime_recorded AS DATETIME) AS DATE)
WHERE TRY_CAST(raw.datetime_recorded AS DATETIME) IS NOT NULL
    AND raw.departure_port IS NOT NULL 
    AND raw.arrival_port IS NOT NULL
GROUP BY 
    COALESCE(r.route_sk, 1),
    COALESCE(t.time_sk, 1),
    YEAR(CAST(raw.datetime_recorded AS DATE)),
    MONTH(CAST(raw.datetime_recorded AS DATE)),
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE));
GO

-- =========================
-- POPULATE FACT_FUEL_CONSUMPTION_SUMMARY
-- =========================
PRINT 'Populating fact_fuel_consumption_summary...';
GO

INSERT INTO gold.fact_fuel_consumption_summary (
    vessel_sk, route_sk, weather_sk, month_sk, summary_year, summary_month, summary_quarter,
    total_fuel_liters, avg_hourly_consumption, peak_consumption_hour, min_consumption_hour,
    fuel_per_mile, fuel_per_hour, efficiency_vs_baseline, efficiency_trend,
    fuel_in_calm_weather, fuel_in_rough_weather, weather_impact_pct,
    fuel_at_low_speed, fuel_at_medium_speed, fuel_at_high_speed, optimal_speed_range,
    estimated_fuel_cost_usd, cost_per_mile, monthly_budget_variance,
    estimated_co2_tons, emission_efficiency, green_score
)
SELECT 
    COALESCE(v.vessel_sk, 1) as vessel_sk,
    COALESCE(r.route_sk, 1) as route_sk,
    COALESCE(w.weather_sk, 1) as weather_sk,
    COALESCE(t.time_sk, 1) as month_sk,
    YEAR(CAST(raw.datetime_recorded AS DATE)) as summary_year,
    MONTH(CAST(raw.datetime_recorded AS DATE)) as summary_month,
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)) as summary_quarter,
    
    -- Fuel Consumption Metrics
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))), 0) as total_fuel_liters,
    COALESCE(AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))), 0) as avg_hourly_consumption,
    COALESCE(MAX(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))), 0) as peak_consumption_hour,
    COALESCE(MIN(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))), 0) as min_consumption_hour,
    
    -- Efficiency Metrics
    CASE 
        WHEN SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2))) > 0 
        THEN SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))) / SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2)))
        ELSE 0 
    END as fuel_per_mile,
    COALESCE(AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))), 0) as fuel_per_hour,
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))) > 0 
        THEN ((200.0 - AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2)))) / 200.0) * 100
        ELSE 0 
    END as efficiency_vs_baseline,
    'Stable' as efficiency_trend,
    
    -- Weather Impact
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.wind_speed_knots AS DECIMAL(6,2)) < 15 THEN TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)) ELSE 0 END), 0) as fuel_in_calm_weather,
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.wind_speed_knots AS DECIMAL(6,2)) >= 15 THEN TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)) ELSE 0 END), 0) as fuel_in_rough_weather,
    CASE 
        WHEN SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2))) > 0 
        THEN (SUM(CASE WHEN TRY_CAST(raw.wind_speed_knots AS DECIMAL(6,2)) >= 15 THEN TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)) ELSE 0 END) * 100.0) / SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)))
        ELSE 0 
    END as weather_impact_pct,
    
    -- Speed Impact
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.ship_speed_knots AS DECIMAL(6,2)) < 10 THEN TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)) ELSE 0 END), 0) as fuel_at_low_speed,
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.ship_speed_knots AS DECIMAL(6,2)) BETWEEN 10 AND 15 THEN TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)) ELSE 0 END), 0) as fuel_at_medium_speed,
    COALESCE(SUM(CASE WHEN TRY_CAST(raw.ship_speed_knots AS DECIMAL(6,2)) > 15 THEN TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2)) ELSE 0 END), 0) as fuel_at_high_speed,
    '10-15 knots' as optimal_speed_range,
    
    -- Cost Analysis
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))) * 0.50, 0) as estimated_fuel_cost_usd,
    CASE 
        WHEN SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2))) > 0 
        THEN (SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))) * 0.50) / SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(10,2)))
        ELSE 0 
    END as cost_per_mile,
    0.0 as monthly_budget_variance, -- Simplified
    
    -- Environmental Impact (estimated 2.6 kg CO2 per liter diesel)
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2))) * 2.6 / 1000, 0) as estimated_co2_tons,
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))) < 150 THEN 90.0
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))) < 250 THEN 70.0
        ELSE 50.0
    END as emission_efficiency,
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))) < 150 THEN 85.0
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(10,2))) < 250 THEN 65.0
        ELSE 45.0
    END as green_score

FROM bronze.raw_ship_data raw
LEFT JOIN silver.dim_vessel v ON v.mmsi = raw.mmsi AND v.is_current = 1
LEFT JOIN silver.dim_route r ON r.departure_port = raw.departure_port AND r.arrival_port = raw.arrival_port
LEFT JOIN silver.dim_weather w ON w.weather_id = '1' -- Simplified lookup
LEFT JOIN silver.dim_time t ON t.full_date = CAST(TRY_CAST(raw.datetime_recorded AS DATETIME) AS DATE)
WHERE TRY_CAST(raw.datetime_recorded AS DATETIME) IS NOT NULL
    AND TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(15,2)) IS NOT NULL
GROUP BY 
    COALESCE(v.vessel_sk, 1),
    COALESCE(r.route_sk, 1),
    COALESCE(w.weather_sk, 1),
    COALESCE(t.time_sk, 1),
    YEAR(CAST(raw.datetime_recorded AS DATE)),
    MONTH(CAST(raw.datetime_recorded AS DATE)),
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE));
GO

-- =========================
-- POPULATE FACT_PORT_TRAFFIC
-- =========================
PRINT 'Populating fact_port_traffic...';
GO

-- Port Departures
INSERT INTO gold.fact_port_traffic (
    port_name, port_country, port_type, traffic_date, year, month, quarter, day_of_week,
    vessel_departures, total_vessel_movements, unique_vessels,
    cargo_vessels, fishing_vessels, passenger_vessels, military_vessels, other_vessels,
    total_gross_tonnage, avg_vessel_size, max_vessel_size, capacity_utilization,
    avg_port_time_hours, min_port_time_hours, max_port_time_hours, peak_traffic_hour,
    on_schedule_arrivals, delayed_arrivals, early_arrivals, port_efficiency_score
)
SELECT 
    raw.departure_port as port_name,
    CASE 
        WHEN raw.departure_port IN ('Surabaya', 'Jakarta', 'Makassar', 'Medan', 'Belawan', 'Tanjung Priok') THEN 'Indonesia'
        WHEN raw.departure_port LIKE '%Klang%' THEN 'Malaysia'
        WHEN raw.departure_port LIKE '%Singapore%' THEN 'Singapore'
        ELSE 'Unknown'
    END as port_country,
    'Departure' as port_type,
    CAST(raw.datetime_recorded AS DATE) as traffic_date,
    YEAR(CAST(raw.datetime_recorded AS DATE)) as year,
    MONTH(CAST(raw.datetime_recorded AS DATE)) as month,
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)) as quarter,
    DATEPART(WEEKDAY, CAST(raw.datetime_recorded AS DATE)) as day_of_week,
    
    -- Traffic Metrics
    COUNT(*) as vessel_departures,
    COUNT(*) as total_vessel_movements,
    COUNT(DISTINCT raw.mmsi) as unique_vessels,
    
    -- Vessel Types
    SUM(CASE WHEN raw.vessel_type = 'Cargo' THEN 1 ELSE 0 END) as cargo_vessels,
    SUM(CASE WHEN raw.vessel_type = 'Fishing' THEN 1 ELSE 0 END) as fishing_vessels,
    SUM(CASE WHEN raw.vessel_type LIKE '%passenger%' THEN 1 ELSE 0 END) as passenger_vessels,
    SUM(CASE WHEN raw.vessel_type = 'Military' THEN 1 ELSE 0 END) as military_vessels,
    SUM(CASE WHEN raw.vessel_type NOT IN ('Cargo', 'Fishing', 'Military') AND raw.vessel_type NOT LIKE '%passenger%' THEN 1 ELSE 0 END) as other_vessels,
    
    -- Capacity Metrics
    COALESCE(SUM(TRY_CAST(raw.gross_tonnage AS BIGINT)), 0) as total_gross_tonnage,
    COALESCE(AVG(TRY_CAST(raw.gross_tonnage AS DECIMAL(10,2))), 0) as avg_vessel_size,
    COALESCE(MAX(TRY_CAST(raw.gross_tonnage AS INT)), 0) as max_vessel_size,
    75.0 as capacity_utilization, -- Estimated
    
    -- Timing Analysis
    6.0 as avg_port_time_hours, -- Estimated
    2.0 as min_port_time_hours,
    12.0 as max_port_time_hours,
    DATEPART(HOUR, TRY_CAST(raw.datetime_recorded AS DATETIME)) as peak_traffic_hour,
    
    -- Efficiency Metrics
    COUNT(*) * 0.85 as on_schedule_arrivals,
    COUNT(*) * 0.10 as delayed_arrivals,
    COUNT(*) * 0.05 as early_arrivals,
    85.0 as port_efficiency_score

FROM bronze.raw_ship_data raw
WHERE TRY_CAST(raw.datetime_recorded AS DATETIME) IS NOT NULL
    AND raw.departure_port IS NOT NULL
    AND TRIM(raw.departure_port) <> ''
GROUP BY 
    raw.departure_port,
    CAST(raw.datetime_recorded AS DATE),
    YEAR(CAST(raw.datetime_recorded AS DATE)),
    MONTH(CAST(raw.datetime_recorded AS DATE)),
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)),
    DATEPART(WEEKDAY, CAST(raw.datetime_recorded AS DATE)),
    DATEPART(HOUR, TRY_CAST(raw.datetime_recorded AS DATETIME));

-- Port Arrivals
INSERT INTO gold.fact_port_traffic (
    port_name, port_country, port_type, traffic_date, year, month, quarter, day_of_week,
    vessel_arrivals, total_vessel_movements, unique_vessels,
    cargo_vessels, fishing_vessels, passenger_vessels, military_vessels, other_vessels,
    total_gross_tonnage, avg_vessel_size, max_vessel_size, capacity_utilization,
    avg_port_time_hours, min_port_time_hours, max_port_time_hours, peak_traffic_hour,
    on_schedule_arrivals, delayed_arrivals, early_arrivals, port_efficiency_score
)
SELECT 
    raw.arrival_port as port_name,
    CASE 
        WHEN raw.arrival_port IN ('Surabaya', 'Jakarta', 'Makassar', 'Medan', 'Belawan', 'Tanjung Priok') THEN 'Indonesia'
        WHEN raw.arrival_port LIKE '%Klang%' THEN 'Malaysia'
        WHEN raw.arrival_port LIKE '%Singapore%' THEN 'Singapore'
        ELSE 'Unknown'
    END as port_country,
    'Arrival' as port_type,
    CAST(raw.datetime_recorded AS DATE) as traffic_date,
    YEAR(CAST(raw.datetime_recorded AS DATE)) as year,
    MONTH(CAST(raw.datetime_recorded AS DATE)) as month,
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)) as quarter,
    DATEPART(WEEKDAY, CAST(raw.datetime_recorded AS DATE)) as day_of_week,
    
    -- Traffic Metrics
    COUNT(*) as vessel_arrivals,
    COUNT(*) as total_vessel_movements,
    COUNT(DISTINCT raw.mmsi) as unique_vessels,
    
    -- Vessel Types
    SUM(CASE WHEN raw.vessel_type = 'Cargo' THEN 1 ELSE 0 END) as cargo_vessels,
    SUM(CASE WHEN raw.vessel_type = 'Fishing' THEN 1 ELSE 0 END) as fishing_vessels,
    SUM(CASE WHEN raw.vessel_type LIKE '%passenger%' THEN 1 ELSE 0 END) as passenger_vessels,
    SUM(CASE WHEN raw.vessel_type = 'Military' THEN 1 ELSE 0 END) as military_vessels,
    SUM(CASE WHEN raw.vessel_type NOT IN ('Cargo', 'Fishing', 'Military') AND raw.vessel_type NOT LIKE '%passenger%' THEN 1 ELSE 0 END) as other_vessels,
    
    -- Capacity Metrics
    COALESCE(SUM(TRY_CAST(raw.gross_tonnage AS BIGINT)), 0) as total_gross_tonnage,
    COALESCE(AVG(TRY_CAST(raw.gross_tonnage AS DECIMAL(10,2))), 0) as avg_vessel_size,
    COALESCE(MAX(TRY_CAST(raw.gross_tonnage AS INT)), 0) as max_vessel_size,
    80.0 as capacity_utilization, -- Estimated
    
    -- Timing Analysis
    8.0 as avg_port_time_hours, -- Estimated
    3.0 as min_port_time_hours,
    15.0 as max_port_time_hours,
    DATEPART(HOUR, TRY_CAST(raw.datetime_recorded AS DATETIME)) as peak_traffic_hour,
    
    -- Efficiency Metrics
    COUNT(*) * 0.82 as on_schedule_arrivals,
    COUNT(*) * 0.13 as delayed_arrivals,
    COUNT(*) * 0.05 as early_arrivals,
    82.0 as port_efficiency_score

FROM bronze.raw_ship_data raw
WHERE TRY_CAST(raw.datetime_recorded AS DATETIME) IS NOT NULL
    AND raw.arrival_port IS NOT NULL
    AND TRIM(raw.arrival_port) <> ''
GROUP BY 
    raw.arrival_port,
    CAST(raw.datetime_recorded AS DATE),
    YEAR(CAST(raw.datetime_recorded AS DATE)),
    MONTH(CAST(raw.datetime_recorded AS DATE)),
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)),
    DATEPART(WEEKDAY, CAST(raw.datetime_recorded AS DATE)),
    DATEPART(HOUR, TRY_CAST(raw.datetime_recorded AS DATETIME));
GO

-- =========================
-- POPULATE FACT_VESSEL_UTILIZATION
-- =========================
PRINT 'Populating fact_vessel_utilization...';
GO

INSERT INTO gold.fact_vessel_utilization (
    vessel_sk, month_sk, utilization_year, utilization_month, utilization_quarter,
    total_operational_hours, sailing_hours, anchored_hours, moored_hours, maintenance_hours, idle_hours,
    sailing_percentage, anchored_percentage, moored_percentage, maintenance_percentage, idle_percentage, overall_utilization,
    total_distance_covered, avg_daily_distance, total_fuel_consumed, avg_daily_fuel,
    unique_routes_served, unique_ports_visited, total_voyages, completed_voyages, cancelled_voyages,
    fuel_efficiency_score, time_efficiency_score, route_efficiency_score, overall_efficiency,
    scheduled_maintenance, unscheduled_maintenance, maintenance_cost_impact, availability_percentage
)
SELECT 
    COALESCE(v.vessel_sk, 1) as vessel_sk,
    COALESCE(t.time_sk, 1) as month_sk,
    YEAR(CAST(raw.datetime_recorded AS DATE)) as utilization_year,
    MONTH(CAST(raw.datetime_recorded AS DATE)) as utilization_month,
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE)) as utilization_quarter,
    
    -- Utilization Metrics (assuming 5-minute intervals)
    COUNT(*) * 0.083 as total_operational_hours,
    SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 0.083 ELSE 0 END) as sailing_hours,
    SUM(CASE WHEN TRIM(raw.navigational_status) = 'At anchor' THEN 0.083 ELSE 0 END) as anchored_hours,
    SUM(CASE WHEN TRIM(raw.navigational_status) = 'Moored' THEN 0.083 ELSE 0 END) as moored_hours,
    SUM(CASE WHEN TRIM(raw.sensor_status) LIKE '%Maintenance%' THEN 0.083 ELSE 0 END) as maintenance_hours,
    SUM(CASE WHEN TRY_CAST(raw.ship_speed_knots AS DECIMAL(6,2)) = 0 AND TRIM(raw.navigational_status) = 'Unknown value' THEN 0.083 ELSE 0 END) as idle_hours,
    
    -- Utilization Percentages
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as sailing_percentage,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.navigational_status) = 'At anchor' THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as anchored_percentage,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.navigational_status) = 'Moored' THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as moored_percentage,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.sensor_status) LIKE '%Maintenance%' THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as maintenance_percentage,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRY_CAST(raw.ship_speed_knots AS DECIMAL(6,2)) = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as idle_percentage,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') AND TRY_CAST(raw.ship_speed_knots AS DECIMAL(6,2)) > 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 0 
    END as overall_utilization,
    
    -- Performance Metrics
    COALESCE(SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(12,2))), 0) as total_distance_covered,
    COALESCE(SUM(TRY_CAST(raw.travel_distance_miles AS DECIMAL(12,2))) / DATEDIFF(DAY, MIN(CAST(raw.datetime_recorded AS DATE)), MAX(CAST(raw.datetime_recorded AS DATE)) + 1), 0) as avg_daily_distance,
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2))), 0) as total_fuel_consumed,
    COALESCE(SUM(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(12,2))) / DATEDIFF(DAY, MIN(CAST(raw.datetime_recorded AS DATE)), MAX(CAST(raw.datetime_recorded AS DATE)) + 1), 0) as avg_daily_fuel,
    
    -- Routes and Ports
    COUNT(DISTINCT CONCAT(raw.departure_port, '-', raw.arrival_port)) as unique_routes_served,
    COUNT(DISTINCT raw.departure_port) + COUNT(DISTINCT raw.arrival_port) as unique_ports_visited,
    COUNT(DISTINCT CAST(raw.datetime_recorded AS DATE)) as total_voyages,
    COUNT(DISTINCT CAST(raw.datetime_recorded AS DATE)) as completed_voyages, -- Simplified
    0 as cancelled_voyages,
    
    -- Efficiency Scores
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 200 THEN 90.0
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 300 THEN 70.0
        ELSE 50.0
    END as fuel_efficiency_score,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            CASE 
                WHEN (SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 1 ELSE 0 END) * 100.0) / COUNT(*) > 80 THEN 90.0
                WHEN (SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 1 ELSE 0 END) * 100.0) / COUNT(*) > 60 THEN 70.0
                ELSE 50.0
            END
        ELSE 0 
    END as time_efficiency_score,
    75.0 as route_efficiency_score, -- Estimated
    CASE 
        WHEN AVG(TRY_CAST(raw.fuel_consumption_liters AS DECIMAL(8,2))) < 200 AND COUNT(*) > 0 THEN 
            ((90.0 + (CASE WHEN (SUM(CASE WHEN TRIM(raw.navigational_status) NOT IN ('At anchor', 'Moored') THEN 1 ELSE 0 END) * 100.0) / COUNT(*) > 80 THEN 90.0 ELSE 70.0 END) + 75.0) / 3)
        ELSE 65.0
    END as overall_efficiency,
    
    -- Maintenance Impact
    1 as scheduled_maintenance, -- Estimated
    SUM(CASE WHEN TRIM(raw.sensor_status) = 'Maintenance Required' THEN 1 ELSE 0 END) as unscheduled_maintenance,
    SUM(CASE WHEN TRIM(raw.sensor_status) LIKE '%Maintenance%' THEN 500.0 ELSE 0 END) as maintenance_cost_impact,
    CASE 
        WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN TRIM(raw.sensor_status) NOT LIKE '%Maintenance%' THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
        ELSE 100.0 
    END as availability_percentage

FROM bronze.raw_ship_data raw
LEFT JOIN silver.dim_vessel v ON v.mmsi = raw.mmsi AND v.is_current = 1
LEFT JOIN silver.dim_time t ON t.full_date = CAST(TRY_CAST(raw.datetime_recorded AS DATETIME) AS DATE)
WHERE TRY_CAST(raw.datetime_recorded AS DATETIME) IS NOT NULL
    AND raw.mmsi IS NOT NULL
GROUP BY 
    COALESCE(v.vessel_sk, 1),
    COALESCE(t.time_sk, 1),
    YEAR(CAST(raw.datetime_recorded AS DATE)),
    MONTH(CAST(raw.datetime_recorded AS DATE)),
    DATEPART(QUARTER, CAST(raw.datetime_recorded AS DATE));
GO

-- =========================
-- DATA SUMMARY
-- =========================
PRINT 'Gold layer data population completed!';
PRINT '========================================';
GO

SELECT 'fact_ship_performance_daily' as table_name, COUNT(*) as record_count FROM gold.fact_ship_performance_daily
UNION ALL
SELECT 'fact_route_analytics', COUNT(*) FROM gold.fact_route_analytics
UNION ALL
SELECT 'fact_fuel_consumption_summary', COUNT(*) FROM gold.fact_fuel_consumption_summary
UNION ALL
SELECT 'fact_port_traffic', COUNT(*) FROM gold.fact_port_traffic
UNION ALL
SELECT 'fact_vessel_utilization', COUNT(*) FROM gold.fact_vessel_utilization
ORDER BY table_name;
GO

PRINT 'Gold Layer ETL Process Completed Successfully!';
PRINT 'Ready for Analytics and Dashboard Creation!';
GO
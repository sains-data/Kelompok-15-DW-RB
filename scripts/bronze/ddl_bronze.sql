USE ShipDataWarehouse;
GO

-- Drop tabel yang ada
IF OBJECT_ID('bronze.raw_ship_data', 'U') IS NOT NULL
    DROP TABLE bronze.raw_ship_data;
GO

-- Buat tabel dengan urutan kolom yang sesuai CSV
CREATE TABLE bronze.raw_ship_data (
    datetime_recorded       NVARCHAR(50),        -- Kolom pertama di CSV
    id                      NVARCHAR(50),        -- Kolom kedua di CSV
    mmsi                    NVARCHAR(100),
    navigational_status     NVARCHAR(100),       -- NavigationalStatus
    sog                     NVARCHAR(50),        -- SpeedOverGround_SOG
    cog                     NVARCHAR(50),        -- CourseOverGround_COG
    heading                 NVARCHAR(50),
    vessel_type             NVARCHAR(100),       -- VesselType
    width                   NVARCHAR(50),
    length                  NVARCHAR(50),
    draught                 NVARCHAR(50),
    rotation                NVARCHAR(50),        -- Rotation
    speed                   NVARCHAR(50),        -- Speed
    rudder                  NVARCHAR(50),        -- Rudder
    wind_direction          NVARCHAR(50),        -- WindDirection
    wind_speed_knots        NVARCHAR(50),        -- WindSpeed_Knots
    class_value             NVARCHAR(50),        -- Class
    vessel_sk               NVARCHAR(50),        -- Vessel_SK
    time_sk                 NVARCHAR(50),        -- Time_SK
    route_sk                NVARCHAR(50),        -- Route_SK
    weather_sk              NVARCHAR(50),        -- Weather_SK
    position_sk             NVARCHAR(50),        -- Position_SK
    sensor_sk               NVARCHAR(50),        -- Sensor_SK
    navigation_sk           NVARCHAR(50),        -- Navigation_SK
    fuel_consumption_liters NVARCHAR(50),        -- FuelConsumption_Liters
    ship_speed_knots        NVARCHAR(50),        -- ShipSpeed_Knots
    engine_temperature_c    NVARCHAR(50),        -- EngineTemperature_C
    travel_distance_miles   NVARCHAR(50),        -- TravelDistance_Miles
    latitude                NVARCHAR(50),        -- Latitude
    longitude               NVARCHAR(50),        -- Longitude
    wave_height_meters      NVARCHAR(50),        -- WaveHeight_Meters
    air_temperature_c       NVARCHAR(50),        -- AirTemperature_C
    departure_port          NVARCHAR(200),       -- DeparturePort
    arrival_port            NVARCHAR(200),       -- ArrivalPort
    route_name              NVARCHAR(200),       -- RouteName
    vessel_name             NVARCHAR(200),       -- VesselName
    imo_number              NVARCHAR(100),       -- IMONumber
    flag                    NVARCHAR(100),       -- Flag
    year_built              NVARCHAR(50),        -- YearBuilt
    gross_tonnage           NVARCHAR(50),        -- GrossTonnage
    deadweight              NVARCHAR(50),        -- Deadweight
    sensor_type             NVARCHAR(200),       -- SensorType
    sensor_status           NVARCHAR(100),       -- Status
    last_calibration_date   NVARCHAR(50)         -- LastCalibrationDate
);
GO

PRINT 'Table bronze.raw_ship_data created with correct column order!';
GO
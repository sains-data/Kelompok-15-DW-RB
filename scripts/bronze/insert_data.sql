/*
===============================================================================
Insert Script: Bronze Layer Data Loading
===============================================================================
Script Purpose:
    Script ini memuat data dari file CSV ke dalam tabel 'bronze.raw_ship_data'
    untuk proyek "Perancangan-Arsitektur-Data-Warehouse-untuk-Optimalisasi-
    Kinerja-Kapal-Berbasis-Big-Data".
===============================================================================
*/

USE ShipDataWarehouse;
GO

-- Pastikan menggunakan database yang benar
PRINT 'Loading data from CSV file into bronze.raw_ship_data table...';
GO

-- BULK INSERT untuk memuat data dari file CSV
BULK INSERT bronze.raw_ship_data
FROM 'd:\Kuliah\Semester 6\Pergudangan Data\Tugas Besar\Perancangan-Arsitektur-Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-Berbasis-Big-Data\dataset\data_kapal.csv'
WITH (
    FIELDTERMINATOR = ',',      -- Delimiter koma
    ROWTERMINATOR = '\n',       -- Baris baru
    FIRSTROW = 2,               -- Skip header row
    CODEPAGE = 'ACP',           -- Encoding
    TABLOCK,                    -- Table lock untuk performance
    KEEPNULLS                   -- Keep NULL values
);
GO

-- Verifikasi data yang berhasil dimuat
SELECT COUNT(*) AS TotalRecords 
FROM bronze.raw_ship_data;
GO

-- Tampilkan sample data pertama
SELECT TOP 5 
    id,
    datetime_recorded,
    mmsi,
    vessel_name,
    vessel_type,
    departure_port,
    arrival_port,
    sog,
    cog
FROM bronze.raw_ship_data
ORDER BY id;
GO

PRINT 'Data loading completed successfully!';
GO

-- Create indexes for better query performance
CREATE NONCLUSTERED INDEX IX_raw_ship_data_datetime 
ON bronze.raw_ship_data (datetime_recorded);
GO

CREATE NONCLUSTERED INDEX IX_raw_ship_data_mmsi 
ON bronze.raw_ship_data (mmsi);
GO

CREATE NONCLUSTERED INDEX IX_raw_ship_data_vessel_name 
ON bronze.raw_ship_data (vessel_name);
GO

PRINT 'Indexes created successfully!';
GO
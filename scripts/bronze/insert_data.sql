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

PRINT 'Loading data from CSV file into bronze.raw_ship_data table...';
GO

-- BULK INSERT dengan konfigurasi sederhana
BULK INSERT bronze.raw_ship_data
FROM 'd:\Kuliah\Semester 6\Pergudangan Data\Tugas Besar\Perancangan-Arsitektur-Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-Berbasis-Big-Data\dataset\data_kapal.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = 'ACP',           -- Coba dengan ANSI encoding dulu
    TABLOCK,
    KEEPNULLS,
    MAXERRORS = 0               -- Stop di error pertama untuk debugging
);
GO

-- Verifikasi data yang berhasil dimuat
SELECT COUNT(*) AS TotalRecords 
FROM bronze.raw_ship_data;
GO

-- Tampilkan sample data pertama
SELECT TOP 5 
    datetime_recorded,
    id,
    mmsi,
    vessel_name,
    vessel_type,
    departure_port,
    arrival_port
FROM bronze.raw_ship_data
ORDER BY CAST(id AS INT);
GO

PRINT 'Data loading completed successfully!';
GO
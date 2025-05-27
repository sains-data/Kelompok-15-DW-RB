/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
     Script ini membuat database baru bernama 'ShipDataWarehouse' setelah memeriksa apakah sudah ada. 
    Jika database sudah ada, maka akan dihapus dan dibuat ulang. Selain itu, script ini menyiapkan tiga skema 
    dalam database: 'bronze', 'silver', dan 'gold' untuk proyek "Perancangan-Arsitektur-
    Data-Warehouse-untuk-Optimalisasi-Kinerja-Kapal-Berbasis-Big-Data".
    
Warning:
    Menjalankan script ini akan menghapus seluruh database 'ShipDataWarehouse' jika sudah ada. 
    Semua data dalam database akan dihapus secara permanen. Lanjutkan dengan hati-hati 
    dan pastikan Anda memiliki backup yang tepat sebelum menjalankan script ini.
*/

USE master;
GO

-- Hapus dan buat ulang database 'ShipDataWarehouse'
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ShipDataWarehouse')
BEGIN
    ALTER DATABASE ShipDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ShipDataWarehouse;
END;
GO

-- Buat database 'ShipDataWarehouse'
CREATE DATABASE ShipDataWarehouse;
GO

USE ShipDataWarehouse;
GO

-- Buat Skema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

PRINT 'Database ShipDataWarehouse dan skema (bronze, silver, gold) berhasil dibuat!';
GO
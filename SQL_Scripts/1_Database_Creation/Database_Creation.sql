/*
====================================================================================================================================
Create a DataWarehouse database and schemas:
====================================================================================================================================
Script Purpose:
	This script creates a new database named "DataWarehouse" after checking if it already exists
	If the database exists, it is droped and recreated. Additionally the script sets up three schemas
	Within the database: ("bronze" - "silver" - "gold").

Warning:
	Running this script will drop the entire "DataWarehouse" database if it exists.
*/

USE MASTER;
GO

-- Drop the "DataWarehouse" database if exists:
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

-- Recreate Database DataWarehouse:
CREATE DATABASE DataWarehouse;

-- Create DataWarehouse Database Schemas:
Use DataWarehouse;
GO
-- Create bronze schema:
CREATE SCHEMA bronze;
GO
-- Create silver schema:
CREATE SCHEMA silver;
GO
-- Create gold schema:
CREATE SCHEMA gold;
GO

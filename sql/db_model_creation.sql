-- NYC Motor Vehicle Collisions - Data Warehouse Model (T-SQL)
-- Target: MS SQL Server / Visual Studio

 -- OPTIONAL: UNCOMMENT THE LINES BELOW TO RESET THE TABLES
-- (Must be in this specific order because of Foreign Key constraints)
DROP TABLE IF EXISTS [Vehicle_Factor];
DROP TABLE IF EXISTS [Person];
DROP TABLE IF EXISTS [Vehicle];
DROP TABLE IF EXISTS [Crash];
DROP TABLE IF EXISTS [Location];
DROP TABLE IF EXISTS [Precinct];
DROP TABLE IF EXISTS [Weather_Observation];
DROP TABLE IF EXISTS [Contributing_Factor];
DROP TABLE IF EXISTS [Person_Type];
DROP TABLE IF EXISTS [Vehicle_Type];
DROP TABLE IF EXISTS [Borough];


-- 1. Independent Lookup Tables
CREATE TABLE [Borough] (
  [borough_id] INT PRIMARY KEY IDENTITY(1, 1),
  [borough_name] NVARCHAR(100) NOT NULL,
);

CREATE TABLE [Vehicle_Type] (
  [vehicle_type_id] INT PRIMARY KEY IDENTITY(1, 1),
  [type_code] NVARCHAR(50) NOT NULL UNIQUE,
  [type_description] NVARCHAR(255) NOT NULL,
  [type_category] NVARCHAR(100) NOT NULL
);

CREATE TABLE [Person_Type] (
  [person_type_id] INT PRIMARY KEY IDENTITY(1, 1),
  [type_code] NVARCHAR(50) NOT NULL UNIQUE,
  [type_description] NVARCHAR(255) NOT NULL
);

CREATE TABLE [Contributing_Factor] (
  [factor_id] INT PRIMARY KEY IDENTITY(1, 1),
  [factor_code] NVARCHAR(100) NOT NULL UNIQUE,
  [factor_description] NVARCHAR(255) NOT NULL,
  [factor_category] NVARCHAR(100) NOT NULL
);

CREATE TABLE [Weather_Observation] (
  [observation_id] INT PRIMARY KEY IDENTITY(1, 1),
  [borough_id] INT NOT NULL,
  [observation_date] DATE NOT NULL,
  [observation_hour] INT NOT NULL,
  [hourly_precip] DECIMAL(5, 2),
  [hourly_visibility] DECIMAL(7, 2),
  [hourly_wind_speed] DECIMAL(5, 2),
  [hourly_temperature] DECIMAL(5, 2),
  [snow_depth] DECIMAL(5, 2),
  CONSTRAINT [FK_Weather_Borough] FOREIGN KEY ([borough_id]) REFERENCES [Borough] ([borough_id])
);

-- 2. Geography Hierarchy
CREATE TABLE [Precinct] (
  [precinct_id] INT PRIMARY KEY IDENTITY(1, 1),
  [borough_id] INT NOT NULL,
  [precinct_number] NVARCHAR(20) NOT NULL,
  [precinct_name] NVARCHAR(100),
  CONSTRAINT [FK_Precinct_Borough] FOREIGN KEY ([borough_id]) REFERENCES [Borough] ([borough_id])
);

CREATE TABLE [Location] (
  [location_id] INT PRIMARY KEY IDENTITY(1, 1),
  [borough_id] INT NOT NULL,
  [precinct_id] INT,
  [street_name] NVARCHAR(255),
  [zip_code] NVARCHAR(20),
  [latitude] DECIMAL(10, 8),
  [longitude] DECIMAL(11, 8),
  CONSTRAINT [FK_Location_Borough] FOREIGN KEY ([borough_id]) REFERENCES [Borough] ([borough_id]),
  CONSTRAINT [FK_Location_Precinct] FOREIGN KEY ([precinct_id]) REFERENCES [Precinct] ([precinct_id])
);

-- 3. Central Fact Table
CREATE TABLE [Crash] (
  [collision_id] INT PRIMARY KEY IDENTITY(1, 1),
  [crash_date] DATE NOT NULL,
  [crash_time] TIME NOT NULL,
  [location_id] INT,
  [number_of_persons_injured] INT DEFAULT 0,
  [number_of_persons_killed] INT DEFAULT 0,
  CONSTRAINT [FK_Crash_Location] FOREIGN KEY ([location_id]) REFERENCES [Location] ([location_id])
);

-- 4. Vehicle & Person Entities
CREATE TABLE [Vehicle] (
  [vehicle_id] INT PRIMARY KEY IDENTITY(1, 1),
  [collision_id] INT NOT NULL,
  [vehicle_type_id] INT NOT NULL,
  [state_registration] NVARCHAR(50),
  [vehicle_year] INT,
  CONSTRAINT [FK_Vehicle_Crash] FOREIGN KEY ([collision_id]) REFERENCES [Crash] ([collision_id]),
  CONSTRAINT [FK_Vehicle_Type] FOREIGN KEY ([vehicle_type_id]) REFERENCES [Vehicle_Type] ([vehicle_type_id])
);

CREATE TABLE [Person] (
  [person_id] INT PRIMARY KEY IDENTITY(1, 1),
  [collision_id] INT NOT NULL,
  [vehicle_id] INT,
  [person_type_id] INT NOT NULL,
  [injury_type] NVARCHAR(100),
  [age] INT,
  [sex] NCHAR(1),
  CONSTRAINT [FK_Person_Crash] FOREIGN KEY ([collision_id]) REFERENCES [Crash] ([collision_id]),
  CONSTRAINT [FK_Person_Vehicle] FOREIGN KEY ([vehicle_id]) REFERENCES [Vehicle] ([vehicle_id]),
  CONSTRAINT [FK_Person_Type] FOREIGN KEY ([person_type_id]) REFERENCES [Person_Type] ([person_type_id])
);

-- 5. Many-to-Many Linking Table
CREATE TABLE [Vehicle_Factor] (
  [vehicle_factor_id] INT PRIMARY KEY IDENTITY(1, 1),
  [vehicle_id] INT NOT NULL,
  [factor_id] INT NOT NULL,
  CONSTRAINT [FK_VehicleFactor_Vehicle] FOREIGN KEY ([vehicle_id]) REFERENCES [Vehicle] ([vehicle_id]),
  CONSTRAINT [FK_VehicleFactor_Factor] FOREIGN KEY ([factor_id]) REFERENCES [Contributing_Factor] ([factor_id])
);
GO
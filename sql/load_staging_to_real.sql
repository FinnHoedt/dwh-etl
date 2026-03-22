-- Load script: staging tables (st_*) -> real DW tables
-- Target: SQL Server (T-SQL)
--
-- Assumptions:
-- 1) You imported output/st_*.csv into staging tables named:
--    st_borough, st_precinct, st_location, st_crash, st_vehicle_type,
--    st_vehicle, st_person_type, st_person, st_contributing_factor,
--    st_vehicle_factor, st_weather_observation
-- 2) Target tables already exist (Borough, Precinct, Location, Crash, Vehicle, Person, ...)
-- 3) IDs from staging should be preserved to keep foreign keys aligned.
--
-- If your staging objects are in a schema (for example [st].[borough]),
-- replace source table names accordingly.
--
-- Important:
-- Some CSV importers create staging columns as TEXT/NTEXT.
-- SQL Server cannot convert TEXT/NTEXT directly with TRY_CONVERT to DATE/TIME/INT/DECIMAL.
-- Therefore this script first CONVERTs those values to VARCHAR/NVARCHAR.

SET XACT_ABORT ON;
BEGIN TRANSACTION;

-- Optional: full reload (uncomment to clear destination first, child -> parent)
-- DELETE FROM [Vehicle_Factor];
-- DELETE FROM [Person];
-- DELETE FROM [Vehicle];
-- DELETE FROM [Crash];
-- DELETE FROM [Location];
-- DELETE FROM [Precinct];
-- DELETE FROM [Weather_Observation];
-- DELETE FROM [Contributing_Factor];
-- DELETE FROM [Person_Type];
-- DELETE FROM [Vehicle_Type];
-- DELETE FROM [Borough];

/* 1) Lookup / parent tables */

SET IDENTITY_INSERT [Borough] ON;
INSERT INTO [Borough] ([borough_id], [borough_name])
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])),
    CONVERT(NVARCHAR(100), s.[borough_name])
FROM [st_borough] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Borough] t
      WHERE t.[borough_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id]))
  );
SET IDENTITY_INSERT [Borough] OFF;

SET IDENTITY_INSERT [Vehicle_Type] ON;
INSERT INTO [Vehicle_Type] ([vehicle_type_id], [type_code], [type_description], [type_category])
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_type_id])),
    CONVERT(NVARCHAR(50), s.[type_code]),
    CONVERT(NVARCHAR(255), s.[type_description]),
    CONVERT(NVARCHAR(100), s.[type_category])
FROM [st_vehicle_type] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_type_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Vehicle_Type] t
      WHERE t.[vehicle_type_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_type_id]))
  );
SET IDENTITY_INSERT [Vehicle_Type] OFF;

SET IDENTITY_INSERT [Person_Type] ON;
INSERT INTO [Person_Type] ([person_type_id], [type_code], [type_description])
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_type_id])),
    CONVERT(NVARCHAR(50), s.[type_code]),
    CONVERT(NVARCHAR(255), s.[type_description])
FROM [st_person_type] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_type_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Person_Type] t
      WHERE t.[person_type_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_type_id]))
  );
SET IDENTITY_INSERT [Person_Type] OFF;

SET IDENTITY_INSERT [Contributing_Factor] ON;
INSERT INTO [Contributing_Factor] ([factor_id], [factor_code], [factor_description], [factor_category])
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[factor_id])),
    CONVERT(NVARCHAR(100), s.[factor_code]),
    CONVERT(NVARCHAR(255), s.[factor_description]),
    CONVERT(NVARCHAR(100), s.[factor_category])
FROM [st_contributing_factor] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[factor_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Contributing_Factor] t
      WHERE t.[factor_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[factor_id]))
  );
SET IDENTITY_INSERT [Contributing_Factor] OFF;

/* 2) Geography + weather */

SET IDENTITY_INSERT [Precinct] ON;
INSERT INTO [Precinct] ([precinct_id], [borough_id], [precinct_number], [precinct_name])
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[precinct_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])),
    CONVERT(NVARCHAR(20), s.[precinct_number]),
    CONVERT(NVARCHAR(100), s.[precinct_name])
FROM [st_precinct] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[precinct_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Precinct] t
      WHERE t.[precinct_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[precinct_id]))
  );
SET IDENTITY_INSERT [Precinct] OFF;

SET IDENTITY_INSERT [Weather_Observation] ON;
INSERT INTO [Weather_Observation] (
    [observation_id],
    [borough_id],
    [observation_date],
    [observation_hour],
    [hourly_precip],
    [hourly_visibility],
    [hourly_wind_speed],
    [hourly_temperature],
    [snow_depth]
)
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[observation_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])),
    TRY_CONVERT(DATE, CONVERT(VARCHAR(100), s.[observation_date])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[observation_hour])),
    TRY_CONVERT(DECIMAL(5, 2), CONVERT(VARCHAR(100), s.[hourly_precip])),
    TRY_CONVERT(DECIMAL(7, 2), CONVERT(VARCHAR(100), s.[hourly_visibility])),
    TRY_CONVERT(DECIMAL(5, 2), CONVERT(VARCHAR(100), s.[hourly_wind_speed])),
    TRY_CONVERT(DECIMAL(5, 2), CONVERT(VARCHAR(100), s.[hourly_temperature])),
    TRY_CONVERT(DECIMAL(5, 2), CONVERT(VARCHAR(100), s.[snow_depth]))
FROM [st_weather_observation] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[observation_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])) IS NOT NULL
  AND TRY_CONVERT(DATE, CONVERT(VARCHAR(100), s.[observation_date])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[observation_hour])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Weather_Observation] t
      WHERE t.[observation_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[observation_id]))
  );
SET IDENTITY_INSERT [Weather_Observation] OFF;

SET IDENTITY_INSERT [Location] ON;
INSERT INTO [Location] (
    [location_id],
    [borough_id],
    [precinct_id],
    [street_name],
    [zip_code],
    [latitude],
    [longitude]
)
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[location_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[precinct_id])),
    CONVERT(NVARCHAR(255), s.[street_name]),
    CONVERT(NVARCHAR(20), s.[zip_code]),
    TRY_CONVERT(DECIMAL(10, 8), CONVERT(VARCHAR(100), s.[latitude])),
    TRY_CONVERT(DECIMAL(11, 8), CONVERT(VARCHAR(100), s.[longitude]))
FROM [st_location] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[location_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[borough_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Location] t
      WHERE t.[location_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[location_id]))
  );
SET IDENTITY_INSERT [Location] OFF;

/* 3) Central fact and dependents */

SET IDENTITY_INSERT [Crash] ON;
INSERT INTO [Crash] (
    [collision_id],
    [crash_date],
    [crash_time],
    [location_id],
    [number_of_persons_injured],
    [number_of_persons_killed]
)
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id])),
    TRY_CONVERT(DATE, CONVERT(VARCHAR(100), s.[crash_date])),
    TRY_CONVERT(TIME, CONVERT(VARCHAR(100), s.[crash_time])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[location_id])),
    COALESCE(TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[number_of_persons_injured])), 0),
    COALESCE(TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[number_of_persons_killed])), 0)
FROM [st_crash] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id])) IS NOT NULL
  AND TRY_CONVERT(DATE, CONVERT(VARCHAR(100), s.[crash_date])) IS NOT NULL
  AND TRY_CONVERT(TIME, CONVERT(VARCHAR(100), s.[crash_time])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Crash] t
      WHERE t.[collision_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id]))
  );
SET IDENTITY_INSERT [Crash] OFF;

SET IDENTITY_INSERT [Vehicle] ON;
INSERT INTO [Vehicle] (
    [vehicle_id],
    [collision_id],
    [vehicle_type_id],
    [state_registration],
    [vehicle_year]
)
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_type_id])),
    CONVERT(NVARCHAR(50), s.[state_registration]),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_year]))
FROM [st_vehicle] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_type_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Vehicle] t
      WHERE t.[vehicle_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_id]))
  );
SET IDENTITY_INSERT [Vehicle] OFF;

SET IDENTITY_INSERT [Person] ON;
INSERT INTO [Person] (
    [person_id],
    [collision_id],
    [vehicle_id],
    [person_type_id],
    [injury_type],
    [age],
    [sex]
)
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_type_id])),
    CONVERT(NVARCHAR(100), s.[injury_type]),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[age])),
    CASE
        WHEN s.[sex] IS NULL THEN NULL
        ELSE LEFT(CONVERT(NVARCHAR(10), s.[sex]), 1)
    END
FROM [st_person] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[collision_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_type_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Person] t
      WHERE t.[person_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[person_id]))
  );
SET IDENTITY_INSERT [Person] OFF;

SET IDENTITY_INSERT [Vehicle_Factor] ON;
INSERT INTO [Vehicle_Factor] ([vehicle_factor_id], [vehicle_id], [factor_id])
SELECT
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_factor_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_id])),
    TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[factor_id]))
FROM [st_vehicle_factor] s
WHERE TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_factor_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_id])) IS NOT NULL
  AND TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[factor_id])) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [Vehicle_Factor] t
      WHERE t.[vehicle_factor_id] = TRY_CONVERT(INT, CONVERT(VARCHAR(100), s.[vehicle_factor_id]))
  );
SET IDENTITY_INSERT [Vehicle_Factor] OFF;

COMMIT TRANSACTION;
GO

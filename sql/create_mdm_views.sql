-- Star schema views for cube build
-- Source (BDB/base tables): Borough, Precinct, Location, Weather_Observation,
-- Crash, Vehicle, Vehicle_Type, Person, Person_Type, Contributing_Factor, Vehicle_Factor
-- Target schema for views: mdm
-- SQL Server (T-SQL)

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mdm')
BEGIN
    EXEC('CREATE SCHEMA mdm');
END;
GO

/* =========================================================
   DIM_TIME
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Dim_Time]
AS
WITH base AS (
    SELECT DISTINCT
        c.[crash_date] AS [datum],
        c.[crash_time] AS [uhrzeit]
    FROM [Crash] c
    WHERE c.[crash_date] IS NOT NULL
      AND c.[crash_time] IS NOT NULL
),
enriched AS (
    SELECT
        CAST(CONVERT(VARCHAR(8), b.[datum], 112) + REPLACE(LEFT(CONVERT(VARCHAR(8), b.[uhrzeit], 108), 5), ':', '') AS INT) AS [time_key],
        b.[datum],
        b.[uhrzeit],
        DATEPART(HOUR, b.[uhrzeit]) AS [stunde],
        YEAR(b.[datum]) AS [jahr],
        DATEPART(QUARTER, b.[datum]) AS [quartal],
        MONTH(b.[datum]) AS [monat],
        DATENAME(MONTH, b.[datum]) AS [monat_name],
        DATEPART(ISO_WEEK, b.[datum]) AS [kalenderwoche],
        DAY(b.[datum]) AS [tag_im_monat],
        DATEPART(DAYOFYEAR, b.[datum]) AS [tag_im_jahr],
        ((DATEDIFF(DAY, '19000101', b.[datum]) % 7) + 1) AS [wochentag]
    FROM base b
)
SELECT
    e.[time_key],
    e.[datum],
    e.[uhrzeit],
    e.[stunde],
    e.[jahr],
    e.[quartal],
    e.[monat],
    e.[monat_name],
    e.[kalenderwoche],
    e.[tag_im_monat],
    e.[tag_im_jahr],
    e.[wochentag],
    CASE e.[wochentag]
        WHEN 1 THEN N'Montag'
        WHEN 2 THEN N'Dienstag'
        WHEN 3 THEN N'Mittwoch'
        WHEN 4 THEN N'Donnerstag'
        WHEN 5 THEN N'Freitag'
        WHEN 6 THEN N'Samstag'
        ELSE N'Sonntag'
    END AS [wochentag_name],
    CAST(CASE WHEN e.[wochentag] IN (6, 7) THEN 1 ELSE 0 END AS BIT) AS [ist_wochenende],
    CASE
        WHEN e.[stunde] BETWEEN 5 AND 10 THEN N'Morgen'
        WHEN e.[stunde] BETWEEN 11 AND 15 THEN N'Mittag'
        WHEN e.[stunde] BETWEEN 16 AND 21 THEN N'Abend'
        ELSE N'Nacht'
    END AS [tageszeit_kategorie],
    CAST(CASE WHEN e.[stunde] BETWEEN 7 AND 9 OR e.[stunde] BETWEEN 16 AND 18 THEN 1 ELSE 0 END AS BIT) AS [ist_rushhour]
FROM enriched e;
GO

/* =========================================================
   DIM_LOCATION
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Dim_Location]
AS
SELECT
    l.[location_id] AS [location_key],
    CAST(b.[borough_name] AS NVARCHAR(100)) AS [borough_code],
    CAST(ISNULL(b.[borough_name], N'Unbekannt') AS NVARCHAR(100)) AS [borough_name],
    CAST(p.[precinct_number] AS NVARCHAR(20)) AS [precinct_number],
    CAST(p.[precinct_name] AS NVARCHAR(100)) AS [precinct_name],
    CAST(l.[street_name] AS NVARCHAR(255)) AS [strasse],
    CAST(l.[zip_code] AS NVARCHAR(20)) AS [zip_code],
    CAST(l.[latitude] AS DECIMAL(10, 8)) AS [latitude],
    CAST(l.[longitude] AS DECIMAL(11, 8)) AS [longitude],
    CAST(0 AS BIT) AS [ist_kuestennah]
FROM [Location] l
LEFT JOIN [Borough] b
    ON b.[borough_id] = l.[borough_id]
LEFT JOIN [Precinct] p
    ON p.[precinct_id] = l.[precinct_id];
GO

/* =========================================================
   DIM_WEATHER
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Dim_Weather]
AS
SELECT
    w.[observation_id] AS [weather_key],
    CAST(w.[hourly_precip] AS DECIMAL(5, 2)) AS [niederschlag_mm],
    CAST(
        CASE
            WHEN w.[hourly_visibility] IS NULL THEN NULL
            ELSE w.[hourly_visibility] / 1609.344
        END AS DECIMAL(5, 2)
    ) AS [sichtweite_meilen],
    CAST(
        CASE
            WHEN w.[hourly_wind_speed] IS NULL THEN NULL
            ELSE w.[hourly_wind_speed] * 0.621371
        END AS DECIMAL(5, 2)
    ) AS [windgeschwindigkeit_mph],
    CAST(w.[hourly_temperature] AS DECIMAL(5, 2)) AS [temperatur_f],
    CAST(
        CASE
            WHEN w.[snow_depth] IS NULL THEN NULL
            ELSE w.[snow_depth] * 100.0
        END AS DECIMAL(5, 2)
    ) AS [schneemenge_cm],
    CASE
        WHEN ISNULL(w.[hourly_precip], 0) = 0 THEN N'Kein'
        WHEN w.[hourly_precip] < 2 THEN N'Leicht'
        WHEN w.[hourly_precip] < 7 THEN N'Mäßig'
        WHEN w.[hourly_precip] < 20 THEN N'Stark'
        ELSE N'Extrem'
    END AS [niederschlag_kategorie],
    CASE
        WHEN ISNULL(w.[hourly_precip], 0) = 0 THEN N'Keiner'
        WHEN ISNULL(w.[snow_depth], 0) > 0 THEN N'Schnee'
        ELSE N'Regen'
    END AS [niederschlag_typ],
    CASE
        WHEN w.[hourly_visibility] IS NULL THEN N'Gut (>5mi)'
        WHEN (w.[hourly_visibility] / 1609.344) < 1 THEN N'Sehr schlecht (<1mi)'
        WHEN (w.[hourly_visibility] / 1609.344) < 3 THEN N'Eingeschränkt (1-3mi)'
        WHEN (w.[hourly_visibility] / 1609.344) < 5 THEN N'Mäßig (3-5mi)'
        ELSE N'Gut (>5mi)'
    END AS [sichtweite_kategorie],
    CASE
        WHEN w.[hourly_visibility] IS NULL THEN N'Niedrig'
        WHEN (w.[hourly_visibility] / 1609.344) < 1 THEN N'Hoch'
        WHEN (w.[hourly_visibility] / 1609.344) < 3 THEN N'Mittel'
        ELSE N'Niedrig'
    END AS [sichtweite_risiko_klasse],
    CASE
        WHEN w.[hourly_wind_speed] IS NULL THEN N'Windstill (0-10)'
        WHEN (w.[hourly_wind_speed] * 0.621371) <= 10 THEN N'Windstill (0-10)'
        WHEN (w.[hourly_wind_speed] * 0.621371) <= 20 THEN N'Mäßig (11-20)'
        ELSE N'Sturm (>20 mph)'
    END AS [wind_kategorie],
    CAST(CASE WHEN (ISNULL(w.[hourly_wind_speed], 0) * 0.621371) > 20 THEN 1 ELSE 0 END AS BIT) AS [ist_sturmwarnung],
    CASE
        WHEN (ISNULL(w.[hourly_wind_speed], 0) * 0.621371) > 20 THEN N'Sturm'
        WHEN ISNULL(w.[snow_depth], 0) > 0 THEN N'Schnee'
        WHEN ISNULL(w.[hourly_precip], 0) > 0 THEN N'Regen'
        WHEN (ISNULL(w.[hourly_visibility], 999999) / 1609.344) < 1 THEN N'Nebel'
        ELSE N'Klar'
    END AS [wetterlage]
FROM [Weather_Observation] w;
GO

/* =========================================================
   DIM_VEHICLE
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Dim_Vehicle]
AS
SELECT
    v.[vehicle_id] AS [vehicle_key],
    CAST(v.[vehicle_id] AS NVARCHAR(50)) AS [vehicle_id],
    CAST(vt.[type_code] AS NVARCHAR(50)) AS [type_code],
    CAST(vt.[type_description] AS NVARCHAR(255)) AS [type_beschreibung],
    CASE
        WHEN vt.[type_code] IN ('COMMERCIAL_TRUCK', 'BOX_TRUCK', 'BUS', 'TRAILER', 'CONSTRUCTION_EQUIPMENT', 'VAN_MINIVAN') THEN N'LKW/Bus'
        WHEN vt.[type_code] IN ('MOTORCYCLE_MOPED', 'BICYCLE_MICROMOBILITY') THEN N'Zweirad'
        WHEN vt.[type_code] IN ('PASSENGER_CAR', 'SUV', 'PICKUP_TRUCK', 'TAXI_FOR_HIRE') THEN N'PKW'
        ELSE N'Sonstige'
    END AS [fahrzeug_kategorie],
    CASE
        WHEN vt.[type_code] IN ('BUS', 'COMMERCIAL_TRUCK', 'BOX_TRUCK', 'TRAILER', 'CONSTRUCTION_EQUIPMENT') THEN N'Groß'
        WHEN vt.[type_code] IN ('VAN_MINIVAN', 'SUV', 'PICKUP_TRUCK', 'TAXI_FOR_HIRE') THEN N'Mittel'
        ELSE N'Klein'
    END AS [fahrzeug_klasse],
    CAST(NULL AS NVARCHAR(100)) AS [vehicle_make],
    CAST(NULL AS NVARCHAR(100)) AS [vehicle_model],
    v.[vehicle_year],
    CAST(v.[state_registration] AS NVARCHAR(50)) AS [state_registration],
    CASE UPPER(LTRIM(RTRIM(ISNULL(v.[state_registration], ''))))
        WHEN 'NY' THEN N'New York'
        WHEN 'NJ' THEN N'New Jersey'
        WHEN 'CT' THEN N'Connecticut'
        WHEN '' THEN NULL
        ELSE N'Other'
    END AS [state_name],
    CAST(CASE WHEN UPPER(LTRIM(RTRIM(ISNULL(v.[state_registration], '')))) = 'NY' THEN 1 ELSE 0 END AS BIT) AS [ist_ny_zulassung],
    CAST(
        CASE
            WHEN NULLIF(LTRIM(RTRIM(ISNULL(v.[state_registration], ''))), '') IS NULL THEN 0
            WHEN UPPER(LTRIM(RTRIM(v.[state_registration]))) <> 'NY' THEN 1
            ELSE 0
        END AS BIT
    ) AS [ist_tourist],
    CASE
        WHEN UPPER(LTRIM(RTRIM(ISNULL(v.[state_registration], '')))) = 'NY' THEN N'NY'
        WHEN UPPER(LTRIM(RTRIM(ISNULL(v.[state_registration], '')))) IN ('NJ', 'CT') THEN N'Tri-State'
        WHEN NULLIF(LTRIM(RTRIM(ISNULL(v.[state_registration], ''))), '') IS NULL THEN N'Unbekannt'
        WHEN LEN(UPPER(LTRIM(RTRIM(v.[state_registration])))) = 2 THEN N'USA-Andere'
        ELSE N'International'
    END AS [region_kategorie],
    CAST(
        CASE
            WHEN vt.[type_code] IN ('COMMERCIAL_TRUCK', 'BOX_TRUCK', 'BUS', 'TRAILER', 'CONSTRUCTION_EQUIPMENT', 'VAN_MINIVAN') THEN 1
            ELSE 0
        END AS BIT
    ) AS [ist_grossfahrzeug],
    CAST(
        CASE
            WHEN vt.[type_code] IN ('COMMERCIAL_TRUCK', 'BOX_TRUCK', 'BUS', 'TRAILER', 'CONSTRUCTION_EQUIPMENT', 'VAN_MINIVAN') THEN 1
            ELSE 0
        END AS BIT
    ) AS [ist_windanfaellig]
FROM [Vehicle] v
LEFT JOIN [Vehicle_Type] vt
    ON vt.[vehicle_type_id] = v.[vehicle_type_id];
GO

/* =========================================================
   DIM_CONTRIBUTING_FACTOR
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Dim_Contributing_Factor]
AS
SELECT
    cf.[factor_id] AS [factor_key],
    CAST(cf.[factor_code] AS NVARCHAR(100)) AS [factor_code],
    CAST(cf.[factor_description] AS NVARCHAR(255)) AS [factor_beschreibung],
    CASE UPPER(ISNULL(cf.[factor_category], ''))
        WHEN 'DRIVER ERROR' THEN N'Fahrerfehler'
        WHEN 'VEHICLE DEFECT' THEN N'Fahrzeugmängel'
        WHEN 'ENVIRONMENTAL' THEN N'Umwelt'
        WHEN 'IMPAIRMENT' THEN N'Fahrerfehler'
        WHEN 'PEDESTRIAN/CYCLIST' THEN N'Sonstige'
        ELSE N'Sonstige'
    END AS [faktor_kategorie],
    CAST(cf.[factor_description] AS NVARCHAR(255)) AS [faktor_detail],
    CAST(
        CASE
            WHEN UPPER(ISNULL(cf.[factor_code], '')) LIKE '%FOLLOWING TOO CLOSELY%' THEN 1
            ELSE 0
        END AS BIT
    ) AS [ist_auffahrunfall_faktor],
    CAST(
        CASE
            WHEN UPPER(ISNULL(cf.[factor_code], '')) LIKE '%INATTENTION%' THEN 1
            ELSE 0
        END AS BIT
    ) AS [ist_unachtsamkeit]
FROM [Contributing_Factor] cf;
GO

/* =========================================================
   DIM_PERSON
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Dim_Person]
AS
SELECT
    p.[person_id] AS [person_key],
    CAST(p.[person_id] AS NVARCHAR(50)) AS [person_id],
    CAST(ISNULL(pt.[type_code], N'UNKNOWN') AS NVARCHAR(50)) AS [type_code],
    CAST(ISNULL(pt.[type_description], N'Unknown') AS NVARCHAR(255)) AS [type_beschreibung],
    CASE
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%PEDESTRIAN%' THEN N'Fußgänger'
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%BICYCL%' OR UPPER(ISNULL(pt.[type_code], '')) LIKE '%CYCLIST%' THEN N'Radfahrer'
        ELSE N'Fahrzeuginsasse'
    END AS [beteiligungsart],
    CASE
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%DRIVER%' THEN N'Fahrer'
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%PASSENGER%' THEN N'Beifahrer'
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%PEDESTRIAN%' THEN N'Fußgänger'
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%BICYCL%' OR UPPER(ISNULL(pt.[type_code], '')) LIKE '%CYCLIST%' THEN N'Radfahrer'
        ELSE N'Unbekannt'
    END AS [verkehrsteilnehmer_typ],
    p.[age] AS [alter],
    CASE
        WHEN p.[age] IS NULL THEN N'Unbekannt'
        WHEN p.[age] < 14 THEN N'Kind'
        WHEN p.[age] < 18 THEN N'Jugendlich'
        WHEN p.[age] < 65 THEN N'Erwachsen'
        ELSE N'Senior'
    END AS [alter_kategorie],
    CASE UPPER(ISNULL(p.[sex], ''))
        WHEN 'M' THEN N'M'
        WHEN 'F' THEN N'F'
        ELSE N'Unbekannt'
    END AS [geschlecht],
    CAST(p.[injury_type] AS NVARCHAR(100)) AS [injury_type],
    CASE
        WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%PEDESTRIAN%'
          OR UPPER(ISNULL(pt.[type_code], '')) LIKE '%BICYCL%'
          OR UPPER(ISNULL(pt.[type_code], '')) LIKE '%CYCLIST%' THEN N'Ungeschützt'
        ELSE N'Geschützt'
    END AS [verletzlichkeit]
FROM [Person] p
LEFT JOIN [Person_Type] pt
    ON pt.[person_type_id] = p.[person_type_id];
GO

/* =========================================================
   FACT_CRASH
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Fact_Crash]
AS
WITH vehicle_agg AS (
    SELECT
        v.[collision_id],
        COUNT(*) AS [anzahl_fahrzeuge],
        SUM(CASE WHEN vt.[type_code] IN ('COMMERCIAL_TRUCK', 'BOX_TRUCK', 'BUS', 'TRAILER', 'CONSTRUCTION_EQUIPMENT', 'VAN_MINIVAN') THEN 1 ELSE 0 END) AS [anzahl_grossfahrzeuge],
        SUM(CASE WHEN UPPER(LTRIM(RTRIM(ISNULL(v.[state_registration], '')))) = 'NY' THEN 1 ELSE 0 END) AS [anzahl_ny_fahrzeuge],
        SUM(
            CASE
                WHEN NULLIF(LTRIM(RTRIM(ISNULL(v.[state_registration], ''))), '') IS NOT NULL
                 AND UPPER(LTRIM(RTRIM(v.[state_registration]))) <> 'NY'
                THEN 1 ELSE 0
            END
        ) AS [anzahl_auswaertige_fahrzeuge]
    FROM [Vehicle] v
    LEFT JOIN [Vehicle_Type] vt
        ON vt.[vehicle_type_id] = v.[vehicle_type_id]
    GROUP BY v.[collision_id]
),
person_agg AS (
    SELECT
        p.[collision_id],
        COUNT(*) AS [anzahl_personen],
        SUM(CASE WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%PEDESTRIAN%' THEN 1 ELSE 0 END) AS [anzahl_fussgaenger],
        SUM(CASE WHEN UPPER(ISNULL(pt.[type_code], '')) LIKE '%BICYCL%' OR UPPER(ISNULL(pt.[type_code], '')) LIKE '%CYCLIST%' THEN 1 ELSE 0 END) AS [anzahl_radfahrer],
        SUM(CASE WHEN UPPER(ISNULL(pt.[type_code], '')) NOT LIKE '%PEDESTRIAN%' AND UPPER(ISNULL(pt.[type_code], '')) NOT LIKE '%BICYCL%' AND UPPER(ISNULL(pt.[type_code], '')) NOT LIKE '%CYCLIST%' THEN 1 ELSE 0 END) AS [anzahl_fahrzeuginsassen]
    FROM [Person] p
    LEFT JOIN [Person_Type] pt
        ON pt.[person_type_id] = p.[person_type_id]
    GROUP BY p.[collision_id]
),
factor_flag AS (
    SELECT
        v.[collision_id],
        MAX(
            CASE
                WHEN UPPER(ISNULL(cf.[factor_code], '')) LIKE '%FOLLOWING TOO CLOSELY%'
                  OR UPPER(ISNULL(cf.[factor_code], '')) LIKE '%INATTENTION%'
                THEN 1 ELSE 0
            END
        ) AS [ist_auffahrunfall]
    FROM [Vehicle] v
    LEFT JOIN [Vehicle_Factor] vf
        ON vf.[vehicle_id] = v.[vehicle_id]
    LEFT JOIN [Contributing_Factor] cf
        ON cf.[factor_id] = vf.[factor_id]
    GROUP BY v.[collision_id]
)
SELECT
    c.[collision_id] AS [crash_key],
    CAST(CONVERT(VARCHAR(8), c.[crash_date], 112) + REPLACE(LEFT(CONVERT(VARCHAR(8), c.[crash_time], 108), 5), ':', '') AS INT) AS [time_key],
    c.[location_id] AS [location_key],
    wmap.[observation_id] AS [weather_key],
    c.[collision_id],
    1 AS [anzahl_unfaelle],
    ISNULL(c.[number_of_persons_injured], 0) AS [personen_verletzt],
    ISNULL(c.[number_of_persons_killed], 0) AS [personen_getoetet],
    ISNULL(c.[number_of_persons_injured], 0) + ISNULL(c.[number_of_persons_killed], 0) AS [personenschaden_gesamt],
    CAST(ISNULL(ff.[ist_auffahrunfall], 0) AS BIT) AS [ist_auffahrunfall],
    CAST(CASE WHEN ISNULL(va.[anzahl_grossfahrzeuge], 0) > 0 THEN 1 ELSE 0 END AS BIT) AS [ist_grossfahrzeug_beteiligt],
    ISNULL(va.[anzahl_fahrzeuge], 0) AS [anzahl_fahrzeuge],
    ISNULL(va.[anzahl_grossfahrzeuge], 0) AS [anzahl_grossfahrzeuge],
    ISNULL(va.[anzahl_ny_fahrzeuge], 0) AS [anzahl_ny_fahrzeuge],
    ISNULL(va.[anzahl_auswaertige_fahrzeuge], 0) AS [anzahl_auswaertige_fahrzeuge],
    CAST(CASE WHEN ISNULL(va.[anzahl_auswaertige_fahrzeuge], 0) > 0 THEN 1 ELSE 0 END AS BIT) AS [ist_tourist_beteiligt],
    ISNULL(pa.[anzahl_personen], 0) AS [anzahl_personen],
    ISNULL(pa.[anzahl_fussgaenger], 0) AS [anzahl_fussgaenger],
    ISNULL(pa.[anzahl_radfahrer], 0) AS [anzahl_radfahrer],
    ISNULL(pa.[anzahl_fahrzeuginsassen], 0) AS [anzahl_fahrzeuginsassen]
FROM [Crash] c
LEFT JOIN [Location] l
    ON l.[location_id] = c.[location_id]
OUTER APPLY (
    SELECT TOP 1 w.[observation_id]
    FROM [Weather_Observation] w
    WHERE w.[borough_id] = l.[borough_id]
      AND w.[observation_date] = c.[crash_date]
      AND w.[observation_hour] = DATEPART(HOUR, c.[crash_time])
    ORDER BY w.[observation_id]
) wmap
LEFT JOIN vehicle_agg va
    ON va.[collision_id] = c.[collision_id]
LEFT JOIN person_agg pa
    ON pa.[collision_id] = c.[collision_id]
LEFT JOIN factor_flag ff
    ON ff.[collision_id] = c.[collision_id];
GO

/* =========================================================
   BRIDGE_CRASH_VEHICLE
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Bridge_Crash_Vehicle]
AS
WITH vbase AS (
    SELECT
        v.[collision_id] AS [crash_key],
        v.[vehicle_id] AS [vehicle_key],
        ROW_NUMBER() OVER (PARTITION BY v.[collision_id] ORDER BY v.[vehicle_id]) AS [vehicle_sequence]
    FROM [Vehicle] v
),
insassen AS (
    SELECT
        p.[collision_id] AS [crash_key],
        p.[vehicle_id] AS [vehicle_key],
        COUNT(*) AS [anzahl_insassen]
    FROM [Person] p
    WHERE p.[vehicle_id] IS NOT NULL
    GROUP BY p.[collision_id], p.[vehicle_id]
),
joined AS (
    SELECT
        vb.[crash_key],
        vb.[vehicle_key],
        vf.[factor_id] AS [factor_key],
        vb.[vehicle_sequence],
        ISNULL(i.[anzahl_insassen], 0) AS [anzahl_insassen]
    FROM vbase vb
    LEFT JOIN [Vehicle_Factor] vf
        ON vf.[vehicle_id] = vb.[vehicle_key]
    LEFT JOIN insassen i
        ON i.[crash_key] = vb.[crash_key]
       AND i.[vehicle_key] = vb.[vehicle_key]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY j.[crash_key], j.[vehicle_key], j.[factor_key]) AS [bridge_key],
    j.[crash_key],
    j.[vehicle_key],
    j.[factor_key],
    j.[vehicle_sequence],
    j.[anzahl_insassen]
FROM joined j;
GO

/* =========================================================
   BRIDGE_CRASH_PERSON
   ========================================================= */
CREATE OR ALTER VIEW [mdm].[Bridge_Crash_Person]
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.[collision_id], p.[person_id]) AS [bridge_key],
    p.[collision_id] AS [crash_key],
    p.[person_id] AS [person_key],
    p.[vehicle_id] AS [vehicle_key],
    CAST(NULL AS NVARCHAR(100)) AS [position_in_vehicle],
    CAST(NULL AS NVARCHAR(100)) AS [safety_equipment],
    CAST(
        CASE
            WHEN UPPER(ISNULL(p.[injury_type], '')) LIKE '%INJUR%'
              OR UPPER(ISNULL(p.[injury_type], '')) LIKE '%KILL%'
            THEN 1 ELSE 0
        END AS BIT
    ) AS [wurde_verletzt],
    CAST(CASE WHEN UPPER(ISNULL(p.[injury_type], '')) LIKE '%KILL%' THEN 1 ELSE 0 END AS BIT) AS [wurde_getoetet]
FROM [Person] p;
GO


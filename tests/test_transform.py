import json

import geopandas as gpd
import pandas as pd
import pytest


def test_build_borough_returns_correct_columns():
    from transform import build_borough
    crashes = pd.DataFrame([{"collision_id": "1", "borough": "MANHATTAN"}])
    result = build_borough(crashes)
    assert list(result.columns) == ["borough_id", "borough_name", "borough_code"]


def test_build_borough_deduplicates():
    from transform import build_borough
    crashes = pd.DataFrame([
        {"collision_id": "1", "borough": "MANHATTAN"},
        {"collision_id": "2", "borough": "MANHATTAN"},
        {"collision_id": "3", "borough": "BROOKLYN"},
    ])
    result = build_borough(crashes)
    assert len(result) == 5


def test_build_borough_excludes_null_and_empty():
    from transform import build_borough
    crashes = pd.DataFrame([
        {"collision_id": "1", "borough": "MANHATTAN"},
        {"collision_id": "2", "borough": None},
        {"collision_id": "3", "borough": ""},
    ])
    result = build_borough(crashes)
    assert len(result) == 5
    assert "MANHATTAN" in result["borough_name"].values


def test_build_borough_normalizes_case_and_whitespace():
    from transform import build_borough
    crashes = pd.DataFrame([
        {"collision_id": "1", "borough": " queens "},
        {"collision_id": "2", "borough": "QUEENS"},
    ])
    result = build_borough(crashes)
    assert len(result[result["borough_name"] == "QUEENS"]) == 1


def test_build_borough_empty_crashes():
    from transform import build_borough
    result = build_borough(pd.DataFrame())
    assert len(result) == 5
    assert list(result.columns) == ["borough_id", "borough_name", "borough_code"]


def test_build_borough_missing_column():
    from transform import build_borough
    crashes = pd.DataFrame([{"collision_id": "1"}])
    result = build_borough(crashes)
    assert len(result) == 5


def test_build_location_uses_surrogate_location_id():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{
        "collision_id": "42", "borough": "MANHATTAN",
        "on_street_name": "BROADWAY", "zip_code": "10001",
        "latitude": "40.7128", "longitude": "-74.0060",
    }])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert result.iloc[0]["location_id"] == 1


def test_build_location_maps_borough_id():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{"collision_id": "1", "borough": "BROOKLYN"}])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    expected_id = boroughs.loc[boroughs["borough_name"] == "BROOKLYN", "borough_id"].iloc[0]
    assert result.iloc[0]["borough_id"] == expected_id


def test_build_location_falls_back_to_cross_street():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{
        "collision_id": "1", "borough": "MANHATTAN",
        "on_street_name": None, "cross_street_name": "5 AVENUE",
    }])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert result.iloc[0]["street_name"] == "5 AVENUE"


def test_build_location_coerces_lat_lon():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{
        "collision_id": "1", "borough": "MANHATTAN",
        "latitude": "40.7128", "longitude": "bad_value",
    }])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert abs(result.iloc[0]["latitude"] - 40.7128) < 0.0001
    assert pd.isna(result.iloc[0]["longitude"])


def test_build_location_empty_crashes():
    from transform import build_location
    result = build_location(pd.DataFrame(), pd.DataFrame())
    assert result.empty
    assert "location_id" in result.columns


def test_build_crash_location_id_is_surrogate_key():
    from transform import build_crash
    crashes = pd.DataFrame([{"collision_id": "99", "crash_date": "2024-01-01", "crash_time": "08:00"}])
    result = build_crash(crashes)
    assert result.iloc[0]["location_id"] == 1
    assert result.iloc[0]["collision_id"] == "99"


def test_build_crash_coerces_injury_counts():
    from transform import build_crash
    crashes = pd.DataFrame([{
        "collision_id": "1",
        "number_of_persons_injured": "3",
        "number_of_persons_killed": "bad",
    }])
    result = build_crash(crashes)
    assert result.iloc[0]["number_of_persons_injured"] == 3
    assert pd.isna(result.iloc[0]["number_of_persons_killed"])


def test_build_crash_crash_date_is_date_only():
    from transform import build_crash
    crashes = pd.DataFrame([{
        "collision_id": "1",
        "crash_date": "2024-01-01T13:45:00",
    }])
    result = build_crash(crashes)
    assert str(result.iloc[0]["crash_date"]) == "2024-01-01"


def test_build_crash_handles_missing_columns():
    from transform import build_crash
    crashes = pd.DataFrame([{"collision_id": "1"}])
    result = build_crash(crashes)
    assert "crash_date" in result.columns
    assert pd.isna(result.iloc[0]["crash_date"])


def test_build_crash_empty():
    from transform import build_crash
    result = build_crash(pd.DataFrame())
    assert result.empty
    assert "collision_id" in result.columns


def test_build_vehicle_type_deduplicates():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([
        {"unique_id": "1", "vehicle_type": "Sedan"},
        {"unique_id": "2", "vehicle_type": "Sedan"},
        {"unique_id": "3", "vehicle_type": "Bus"},
    ])
    result = build_vehicle_type(vehicles)
    assert len(result) == 3
    assert "UNKNOWN" in result["type_code"].values


def test_build_vehicle_type_normalizes_case_variants():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([
        {"unique_id": "1", "vehicle_type": "AMBULANCE"},
        {"unique_id": "2", "vehicle_type": "ambulance"},
        {"unique_id": "3", "vehicle_type": " Ambulance "},
    ])
    result = build_vehicle_type(vehicles)
    assert len(result) == 2
    assert "AMBULANCE" in result["type_code"].values
    assert "UNKNOWN" in result["type_code"].values


def test_build_vehicle_type_maps_known_category():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([{"unique_id": "1", "vehicle_type": "Sedan"}])
    result = build_vehicle_type(vehicles)
    assert result.iloc[0]["type_category"] == "Passenger Vehicle"


def test_build_vehicle_type_unknown_maps_to_unknown():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([{"unique_id": "1", "vehicle_type": "Hovercraft"}])
    result = build_vehicle_type(vehicles)
    assert result.iloc[0]["type_category"] == "Unknown"


def test_build_vehicle_type_excludes_null_and_empty():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([
        {"unique_id": "1", "vehicle_type": "Sedan"},
        {"unique_id": "2", "vehicle_type": None},
        {"unique_id": "3", "vehicle_type": ""},
    ])
    result = build_vehicle_type(vehicles)
    assert len(result) == 2
    assert set(result["type_code"]) == {"SEDAN", "UNKNOWN"}


def test_build_vehicle_type_aliases_pickup_variants_to_single_code():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([
        {"unique_id": "1", "vehicle_type": "PICK-UP TRUCK"},
        {"unique_id": "2", "vehicle_type": "PKUP"},
        {"unique_id": "3", "vehicle_type": "PK"},
    ])
    result = build_vehicle_type(vehicles)
    assert "PICK-UP TRUCK" in result["type_code"].values
    assert len(result[result["type_code"] == "PICK-UP TRUCK"]) == 1


def test_build_vehicle_type_empty():
    from transform import build_vehicle_type
    result = build_vehicle_type(pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["vehicle_type_id", "type_code", "type_description", "type_category"]


def test_build_vehicle_uses_unique_id():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type": "Sedan", "vehicle_year": "2018",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    assert result.iloc[0]["vehicle_id"] == "V1"


def test_build_vehicle_maps_type_id():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type": "Sedan", "vehicle_year": "2018",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    sedan_id = vt.loc[vt["type_code"] == "SEDAN", "vehicle_type_id"].iloc[0]
    assert result.iloc[0]["vehicle_type_id"] == sedan_id


def test_build_vehicle_coerces_year():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type": "Sedan", "vehicle_year": "bad",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    assert pd.isna(result.iloc[0]["vehicle_year"])


def test_build_vehicle_missing_type_maps_to_unknown_id():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type": None, "vehicle_year": "2018",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    unknown_id = vt.loc[vt["type_code"] == "UNKNOWN", "vehicle_type_id"].iloc[0]
    assert result.iloc[0]["vehicle_type_id"] == unknown_id


def test_build_vehicle_empty():
    from transform import build_vehicle
    result = build_vehicle(pd.DataFrame(), pd.DataFrame())
    assert result.empty
    assert "vehicle_id" in result.columns


def test_build_person_type_deduplicates():
    from transform import build_person_type
    persons = pd.DataFrame([
        {"unique_id": "P1", "person_type": "Occupant"},
        {"unique_id": "P2", "person_type": "Occupant"},
        {"unique_id": "P3", "person_type": "Pedestrian"},
    ])
    result = build_person_type(persons)
    assert len(result) == 2


def test_build_person_type_normalizes_case_variants():
    from transform import build_person_type
    persons = pd.DataFrame([
        {"unique_id": "P1", "person_type": "occupant"},
        {"unique_id": "P2", "person_type": " Occupant "},
    ])
    result = build_person_type(persons)
    assert len(result) == 1


def test_build_person_type_empty():
    from transform import build_person_type
    result = build_person_type(pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["person_type_id", "type_code", "type_description"]


def test_build_person_uses_unique_id():
    from transform import build_person, build_person_type
    persons = pd.DataFrame([{
        "unique_id": "P1", "collision_id": "1", "vehicle_id": "V1",
        "person_type": "Occupant", "person_injury": "Injured",
        "person_age": "34", "person_sex": "M",
    }])
    pt = build_person_type(persons)
    result = build_person(persons, pt)
    assert result.iloc[0]["person_id"] == "P1"


def test_build_person_maps_type_id():
    from transform import build_person, build_person_type
    persons = pd.DataFrame([{
        "unique_id": "P1", "collision_id": "1", "vehicle_id": "V1",
        "person_type": "Pedestrian", "person_injury": "None",
        "person_age": "25", "person_sex": "F",
    }])
    pt = build_person_type(persons)
    result = build_person(persons, pt)
    assert result.iloc[0]["person_type_id"] == pt.iloc[0]["person_type_id"]


def test_build_person_vehicle_id_nullable():
    from transform import build_person, build_person_type
    persons = pd.DataFrame([{
        "unique_id": "P1", "collision_id": "1", "vehicle_id": "",
        "person_type": "Pedestrian", "person_injury": "None",
        "person_age": "30", "person_sex": "M",
    }])
    pt = build_person_type(persons)
    result = build_person(persons, pt)
    assert pd.isna(result.iloc[0]["vehicle_id"])


def test_build_person_coerces_age():
    from transform import build_person, build_person_type
    persons = pd.DataFrame([{
        "unique_id": "P1", "collision_id": "1", "vehicle_id": "V1",
        "person_type": "Occupant", "person_injury": "Injured",
        "person_age": "not_a_number", "person_sex": "M",
    }])
    pt = build_person_type(persons)
    result = build_person(persons, pt)
    assert pd.isna(result.iloc[0]["age"])


def test_build_person_empty():
    from transform import build_person
    result = build_person(pd.DataFrame(), pd.DataFrame())
    assert result.empty
    assert "person_id" in result.columns


def test_build_contributing_factor_deduplicates_across_columns():
    from transform import build_contributing_factor
    vehicles = pd.DataFrame([
        {"unique_id": "V1", "contributing_factor_1": "Unsafe Speed", "contributing_factor_2": "Unsafe Speed"},
        {"unique_id": "V2", "contributing_factor_1": "Glare", "contributing_factor_2": None},
    ])
    result = build_contributing_factor(vehicles)
    assert len(result) == 2


def test_build_contributing_factor_normalizes_case_variants():
    from transform import build_contributing_factor
    vehicles = pd.DataFrame([
        {"unique_id": "V1", "contributing_factor_1": "unsafe speed", "contributing_factor_2": "Unsafe Speed"},
    ])
    result = build_contributing_factor(vehicles)
    assert len(result) == 1
    assert result.iloc[0]["factor_category"] == "Driver Error"


def test_build_contributing_factor_excludes_unspecified():
    from transform import build_contributing_factor
    vehicles = pd.DataFrame([{
        "unique_id": "V1",
        "contributing_factor_1": "Unspecified",
        "contributing_factor_2": "Unsafe Speed",
    }])
    result = build_contributing_factor(vehicles)
    assert len(result) == 1
    assert result.iloc[0]["factor_code"] == "Unsafe Speed"


def test_build_contributing_factor_maps_category():
    from transform import build_contributing_factor
    vehicles = pd.DataFrame([{
        "unique_id": "V1",
        "contributing_factor_1": "Glare",
        "contributing_factor_2": None,
    }])
    result = build_contributing_factor(vehicles)
    assert result.iloc[0]["factor_category"] == "Environmental"


def test_build_contributing_factor_unknown_maps_to_unknown():
    from transform import build_contributing_factor
    vehicles = pd.DataFrame([{
        "unique_id": "V1",
        "contributing_factor_1": "Something Weird",
        "contributing_factor_2": None,
    }])
    result = build_contributing_factor(vehicles)
    assert result.iloc[0]["factor_category"] == "Unknown"


def test_build_contributing_factor_empty():
    from transform import build_contributing_factor
    result = build_contributing_factor(pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["factor_id", "factor_code", "factor_description", "factor_category"]


def test_build_vehicle_factor_creates_junction_rows():
    from transform import build_contributing_factor, build_vehicle_factor
    vehicles = pd.DataFrame([{
        "unique_id": "V1",
        "contributing_factor_1": "Unsafe Speed",
        "contributing_factor_2": "Glare",
    }])
    factors = build_contributing_factor(vehicles)
    result = build_vehicle_factor(vehicles, factors)
    assert len(result) == 2
    assert set(result.columns) == {"vehicle_factor_id", "vehicle_id", "factor_id"}


def test_build_vehicle_factor_deduplicates_same_factor_twice():
    from transform import build_contributing_factor, build_vehicle_factor
    vehicles = pd.DataFrame([{
        "unique_id": "V1",
        "contributing_factor_1": "Unsafe Speed",
        "contributing_factor_2": "Unsafe Speed",
    }])
    factors = build_contributing_factor(vehicles)
    result = build_vehicle_factor(vehicles, factors)
    assert len(result) == 1


def test_build_vehicle_factor_skips_unresolved_factors():
    from transform import build_vehicle_factor
    vehicles = pd.DataFrame([{
        "unique_id": "V1",
        "contributing_factor_1": "Unknown Factor XYZ",
        "contributing_factor_2": None,
    }])
    factors_df = pd.DataFrame(columns=["factor_id", "factor_code", "factor_description", "factor_category"])
    result = build_vehicle_factor(vehicles, factors_df)
    assert result.empty


def test_build_vehicle_factor_empty_vehicles():
    from transform import build_vehicle_factor
    result = build_vehicle_factor(pd.DataFrame(), pd.DataFrame())
    assert result.empty
    assert "vehicle_factor_id" in result.columns


def test_build_precinct_returns_correct_columns():
    from transform import build_precinct, build_borough
    crashes = pd.DataFrame([{"collision_id": "1", "borough": "MANHATTAN"}])
    boroughs = build_borough(crashes)
    precincts = pd.DataFrame([{"precinct": 1}])
    result = build_precinct(precincts, boroughs)
    assert list(result.columns) == ["precinct_id", "borough_id", "precinct_number", "precinct_name"]


def test_build_precinct_maps_borough_id():
    from transform import build_precinct, build_borough
    crashes = pd.DataFrame([{"collision_id": "1", "borough": "MANHATTAN"}])
    boroughs = build_borough(crashes)
    precincts = pd.DataFrame([{"precinct": 1}])
    result = build_precinct(precincts, boroughs)
    assert result.iloc[0]["borough_id"] == boroughs.iloc[0]["borough_id"]


def test_build_precinct_116_maps_to_queens_borough_id():
    from transform import build_precinct, build_borough

    crashes = pd.DataFrame([{"collision_id": "1", "borough": "QUEENS"}])
    boroughs = build_borough(crashes)
    queens_borough_id = boroughs.loc[
        boroughs["borough_name"] == "QUEENS", "borough_id"
    ].iloc[0]

    precincts = pd.DataFrame([{"precinct": 116}])
    result = build_precinct(precincts, boroughs)

    assert result.iloc[0]["borough_id"] == queens_borough_id


def test_build_precinct_precinct_name_is_null():
    from transform import build_precinct
    precincts = pd.DataFrame([{"precinct": 1}])
    result = build_precinct(precincts, pd.DataFrame())
    assert pd.isna(result.iloc[0]["precinct_name"])


def test_build_precinct_empty():
    from transform import build_precinct
    result = build_precinct(pd.DataFrame(), pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["precinct_id", "borough_id", "precinct_number", "precinct_name"]


def test_parse_precincts_gdf_returns_geodataframe():
    from transform import parse_precincts_gdf
    precincts = pd.DataFrame([{
        "precinct": 1,
        "the_geom": json.dumps({
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]
        })
    }])
    result = parse_precincts_gdf(precincts)
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 1
    assert "geometry" in result.columns


def test_parse_precincts_gdf_empty():
    from transform import parse_precincts_gdf
    result = parse_precincts_gdf(pd.DataFrame())
    assert isinstance(result, gpd.GeoDataFrame)
    assert result.empty


def test_build_location_includes_precinct_id_column():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{"collision_id": "1", "borough": "MANHATTAN"}])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert "precinct_id" in result.columns


def test_build_location_no_precincts_gives_null_precinct_id():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{
        "collision_id": "1", "borough": "MANHATTAN",
        "latitude": "40.7", "longitude": "-74.0",
    }])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert pd.isna(result.iloc[0]["precinct_id"])


def test_build_location_spatial_join_assigns_precinct_id():
    from transform import build_borough, build_location, parse_precincts_gdf, build_precinct
    crashes = pd.DataFrame([{
        "collision_id": "1", "borough": "MANHATTAN",
        "latitude": "0.5", "longitude": "0.5",
    }])
    boroughs = build_borough(crashes)
    precincts_raw = pd.DataFrame([{
        "precinct": 1,
        "the_geom": json.dumps({
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]
        }),
    }])
    precincts_gdf = parse_precincts_gdf(precincts_raw)
    precinct_df = build_precinct(precincts_raw, boroughs)
    result = build_location(crashes, boroughs, precincts_gdf, precinct_df)
    assert result.iloc[0]["precinct_id"] == precinct_df.iloc[0]["precinct_id"]


def test_build_location_null_latlon_gets_null_precinct_id():
    from transform import build_borough, build_location, parse_precincts_gdf, build_precinct
    crashes = pd.DataFrame([{
        "collision_id": "1", "borough": "MANHATTAN",
        "latitude": None, "longitude": None,
    }])
    boroughs = build_borough(crashes)
    precincts_raw = pd.DataFrame([{
        "precinct": 1,
        "the_geom": json.dumps({
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]
        }),
    }])
    precincts_gdf = parse_precincts_gdf(precincts_raw)
    precinct_df = build_precinct(precincts_raw, boroughs)
    result = build_location(crashes, boroughs, precincts_gdf, precinct_df)
    assert pd.isna(result.iloc[0]["precinct_id"])


def test_build_borough_always_includes_all_five_nyc_boroughs():
    from transform import build_borough
    result = build_borough(pd.DataFrame())
    assert set(result["borough_name"]) == {"MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"}
    assert len(result) == 5


def test_filter_locatable_crashes_infers_borough_from_latlon():
    from transform import filter_locatable_crashes, parse_precincts_gdf
    crashes = pd.DataFrame([{
        "collision_id": "1",
        "borough": None,
        "latitude": "0.5",
        "longitude": "0.5",
    }])
    precincts_raw = pd.DataFrame([{
        "precinct": 1,
        "the_geom": json.dumps({
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]
        }),
    }])
    precincts_gdf = parse_precincts_gdf(precincts_raw)
    result = filter_locatable_crashes(crashes, precincts_gdf)
    assert len(result) == 1
    assert result.iloc[0]["borough"] == "MANHATTAN"


def test_filter_locatable_crashes_drops_null_borough_null_latlon():
    from transform import filter_locatable_crashes
    crashes = pd.DataFrame([{
        "collision_id": "1",
        "borough": None,
        "latitude": None,
        "longitude": None,
    }])
    result = filter_locatable_crashes(crashes, None)
    assert result.empty


def test_filter_locatable_crashes_keeps_known_borough_no_latlon():
    from transform import filter_locatable_crashes
    crashes = pd.DataFrame([{
        "collision_id": "1",
        "borough": "BROOKLYN",
        "latitude": None,
        "longitude": None,
    }])
    result = filter_locatable_crashes(crashes, None)
    assert len(result) == 1
    assert result.iloc[0]["borough"] == "BROOKLYN"


def test_filter_locatable_crashes_empty():
    from transform import filter_locatable_crashes
    result = filter_locatable_crashes(pd.DataFrame(), None)
    assert result.empty

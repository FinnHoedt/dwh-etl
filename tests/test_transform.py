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
    assert len(result) == 2


def test_build_borough_excludes_null_and_empty():
    from transform import build_borough
    crashes = pd.DataFrame([
        {"collision_id": "1", "borough": "MANHATTAN"},
        {"collision_id": "2", "borough": None},
        {"collision_id": "3", "borough": ""},
    ])
    result = build_borough(crashes)
    assert len(result) == 1
    assert result.iloc[0]["borough_name"] == "MANHATTAN"


def test_build_borough_empty_crashes():
    from transform import build_borough
    result = build_borough(pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["borough_id", "borough_name", "borough_code"]


def test_build_borough_missing_column():
    from transform import build_borough
    crashes = pd.DataFrame([{"collision_id": "1"}])
    result = build_borough(crashes)
    assert result.empty


def test_build_location_uses_collision_id_as_location_id():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{
        "collision_id": "42", "borough": "MANHATTAN",
        "on_street_name": "BROADWAY", "zip_code": "10001",
        "latitude": "40.7128", "longitude": "-74.0060",
    }])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert result.iloc[0]["location_id"] == "42"


def test_build_location_maps_borough_id():
    from transform import build_borough, build_location
    crashes = pd.DataFrame([{"collision_id": "1", "borough": "BROOKLYN"}])
    boroughs = build_borough(crashes)
    result = build_location(crashes, boroughs)
    assert result.iloc[0]["borough_id"] == boroughs.iloc[0]["borough_id"]


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


def test_build_crash_location_id_equals_collision_id():
    from transform import build_crash
    crashes = pd.DataFrame([{"collision_id": "99", "crash_date": "2024-01-01", "crash_time": "08:00"}])
    result = build_crash(crashes)
    assert result.iloc[0]["location_id"] == "99"
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
        {"unique_id": "1", "vehicle_type_code": "Sedan"},
        {"unique_id": "2", "vehicle_type_code": "Sedan"},
        {"unique_id": "3", "vehicle_type_code": "Bus"},
    ])
    result = build_vehicle_type(vehicles)
    assert len(result) == 2


def test_build_vehicle_type_maps_known_category():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([{"unique_id": "1", "vehicle_type_code": "Sedan"}])
    result = build_vehicle_type(vehicles)
    assert result.iloc[0]["type_category"] == "Passenger Vehicle"


def test_build_vehicle_type_unknown_maps_to_unknown():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([{"unique_id": "1", "vehicle_type_code": "Hovercraft"}])
    result = build_vehicle_type(vehicles)
    assert result.iloc[0]["type_category"] == "Unknown"


def test_build_vehicle_type_excludes_null_and_empty():
    from transform import build_vehicle_type
    vehicles = pd.DataFrame([
        {"unique_id": "1", "vehicle_type_code": "Sedan"},
        {"unique_id": "2", "vehicle_type_code": None},
        {"unique_id": "3", "vehicle_type_code": ""},
    ])
    result = build_vehicle_type(vehicles)
    assert len(result) == 1


def test_build_vehicle_type_empty():
    from transform import build_vehicle_type
    result = build_vehicle_type(pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["vehicle_type_id", "type_code", "type_description", "type_category"]


def test_build_vehicle_uses_unique_id():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type_code": "Sedan", "vehicle_year": "2018",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    assert result.iloc[0]["vehicle_id"] == "V1"


def test_build_vehicle_maps_type_id():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type_code": "Sedan", "vehicle_year": "2018",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    assert result.iloc[0]["vehicle_type_id"] == vt.iloc[0]["vehicle_type_id"]


def test_build_vehicle_coerces_year():
    from transform import build_vehicle, build_vehicle_type
    vehicles = pd.DataFrame([{
        "unique_id": "V1", "collision_id": "1",
        "vehicle_type_code": "Sedan", "vehicle_year": "bad",
        "state_registration": "NY",
    }])
    vt = build_vehicle_type(vehicles)
    result = build_vehicle(vehicles, vt)
    assert pd.isna(result.iloc[0]["vehicle_year"])


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

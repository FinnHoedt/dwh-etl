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

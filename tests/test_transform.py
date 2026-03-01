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

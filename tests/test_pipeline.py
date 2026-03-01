import pytest
import yaml


def test_load_config_returns_dict(tmp_path):
    config = {
        "socrata": {"domain": "example.com", "datasets": {}, "limit": 10},
        "output": {"directory": "out", "filename": "result", "formats": ["csv"]},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(config))

    from main import load_config
    result = load_config(str(cfg_file))

    assert result["socrata"]["domain"] == "example.com"
    assert result["socrata"]["limit"] == 10
    assert result["output"]["filename"] == "result"


def test_load_config_missing_file():
    from main import load_config
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent.yaml")


def test_build_id_filter_formats_correctly():
    from main import build_id_filter
    result = build_id_filter(["123", "456", "789"])
    assert result == "collision_id in('123', '456', '789')"


def test_build_id_filter_single_id():
    from main import build_id_filter
    result = build_id_filter(["42"])
    assert result == "collision_id in('42')"


def test_build_id_filter_empty_list():
    from main import build_id_filter
    result = build_id_filter([])
    assert result == "collision_id in()"


import pandas as pd
from unittest.mock import MagicMock


def test_fetch_dataset_returns_dataframe():
    from main import fetch_dataset
    client = MagicMock()
    client.get.return_value = [{"collision_id": "1", "crash_date": "2024-01-01"}]

    result = fetch_dataset(client, "dataset-id", limit=1)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert "collision_id" in result.columns


def test_fetch_dataset_empty_response_returns_empty_df():
    from main import fetch_dataset
    client = MagicMock()
    client.get.return_value = []

    result = fetch_dataset(client, "dataset-id")

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_fetch_dataset_api_error_returns_empty_df():
    from main import fetch_dataset
    client = MagicMock()
    client.get.side_effect = Exception("API failure")

    result = fetch_dataset(client, "dataset-id")

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_merge_datasets_joins_on_collision_id():
    from main import merge_datasets
    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    vehicles = pd.DataFrame([{"collision_id": "1", "vehicle_type": "Sedan"}])
    persons = pd.DataFrame([{"collision_id": "1", "person_type": "Driver"}])

    result = merge_datasets(crashes, vehicles, persons)

    assert len(result) == 1
    assert "crash_date" in result.columns
    assert "vehicle_type" in result.columns
    assert "person_type" in result.columns


def test_merge_datasets_empty_vehicles_keeps_crash_rows():
    from main import merge_datasets
    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    persons = pd.DataFrame([{"collision_id": "1", "person_type": "Driver"}])

    result = merge_datasets(crashes, pd.DataFrame(), persons)

    assert len(result) == 1
    assert "crash_date" in result.columns


def test_merge_datasets_empty_persons_keeps_crash_rows():
    from main import merge_datasets
    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    vehicles = pd.DataFrame([{"collision_id": "1", "vehicle_type": "Sedan"}])

    result = merge_datasets(crashes, vehicles, pd.DataFrame())

    assert len(result) == 1
    assert "vehicle_type" in result.columns


def test_merge_datasets_multiple_vehicles_expands_rows():
    from main import merge_datasets
    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    vehicles = pd.DataFrame([
        {"collision_id": "1", "vehicle_type": "Sedan"},
        {"collision_id": "1", "vehicle_type": "SUV"},
    ])
    persons = pd.DataFrame()

    result = merge_datasets(crashes, vehicles, persons)

    assert len(result) == 2


def test_merge_datasets_casts_collision_id_to_str():
    from main import merge_datasets
    crashes = pd.DataFrame([{"collision_id": 1, "crash_date": "2024-01-01"}])
    vehicles = pd.DataFrame([{"collision_id": "1", "vehicle_type": "Sedan"}])
    persons = pd.DataFrame()

    result = merge_datasets(crashes, vehicles, persons)

    assert len(result) == 1

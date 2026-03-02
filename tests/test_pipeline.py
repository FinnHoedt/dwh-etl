import pytest
import yaml
import pandas as pd
from unittest.mock import MagicMock


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


def test_read_output_entity_loads_csv(tmp_path):
    from main import _read_output_entity

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "crash.csv").write_text("collision_id,crash_date\n1,2024-01-01\n")
    cfg = {"output": {"directory": str(out_dir), "formats": ["csv", "parquet"]}}

    result = _read_output_entity(cfg, "crash")

    assert len(result) == 1
    assert result.iloc[0]["collision_id"] == 1


def test_run_weather_only_writes_weather_output(monkeypatch):
    from main import run_weather_only

    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    boroughs = pd.DataFrame([{"borough_id": 1, "borough_name": "MANHATTAN"}])
    weather_df = pd.DataFrame([{
        "observation_id": 1,
        "borough_id": 1,
        "observation_date": "2024-01-01",
        "observation_hour": 0,
        "hourly_precip": 0.1,
        "hourly_visibility": 10000.0,
        "hourly_wind_speed": 3.0,
        "hourly_temperature": 10.0,
        "snow_depth": 0.0,
    }])

    def fake_read(cfg, entity_name):
        if entity_name == "crash":
            return crashes
        if entity_name == "borough":
            return boroughs
        return pd.DataFrame()

    write_calls = []

    monkeypatch.setattr("main._read_output_entity", fake_read)
    monkeypatch.setattr("main.build_weather_observation", lambda c, b, cfg: weather_df)
    monkeypatch.setattr(
        "main.write_output",
        lambda df, cfg, filename=None: write_calls.append((filename, len(df))),
    )

    run_weather_only({"output": {"directory": "output", "formats": ["csv"]}})

    assert write_calls == [("weather_observation", 1)]


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


def test_build_crashes_date_where_with_both_bounds():
    from main import build_crashes_date_where
    cfg = {
        "socrata": {
            "crashes_date_filter": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            }
        }
    }
    result = build_crashes_date_where(cfg)
    assert result == "crash_date >= '2024-01-01T00:00:00' AND crash_date <= '2024-01-31T23:59:59'"


def test_build_crashes_date_where_with_no_bounds():
    from main import build_crashes_date_where
    cfg = {"socrata": {"crashes_date_filter": {"start_date": None, "end_date": None}}}
    result = build_crashes_date_where(cfg)
    assert result is None


def test_chunk_ids_splits_into_expected_batches():
    from main import chunk_ids
    result = chunk_ids(["1", "2", "3", "4", "5"], 2)
    assert result == [["1", "2"], ["3", "4"], ["5"]]


def test_chunk_ids_invalid_batch_size_raises():
    from main import chunk_ids
    with pytest.raises(ValueError):
        chunk_ids(["1", "2"], 0)


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


def test_fetch_crashes_paginated_collects_all_pages():
    from main import fetch_crashes_paginated
    client = MagicMock()
    client.get.side_effect = [
        [{"collision_id": "1"}, {"collision_id": "2"}],
        [{"collision_id": "3"}, {"collision_id": "4"}],
        [{"collision_id": "5"}],
    ]

    result = fetch_crashes_paginated(client, "dataset-id", page_limit=2)

    assert len(result) == 5
    assert client.get.call_count == 3
    assert client.get.call_args_list[0].kwargs["offset"] == 0
    assert client.get.call_args_list[1].kwargs["offset"] == 2
    assert client.get.call_args_list[2].kwargs["offset"] == 4


def test_fetch_crashes_paginated_respects_max_pages():
    from main import fetch_crashes_paginated
    client = MagicMock()
    client.get.side_effect = [
        [{"collision_id": "1"}, {"collision_id": "2"}],
        [{"collision_id": "3"}, {"collision_id": "4"}],
    ]

    result = fetch_crashes_paginated(client, "dataset-id", page_limit=2, max_pages=1)

    assert len(result) == 2
    assert client.get.call_count == 1


def test_fetch_related_in_batches_combines_all_chunks():
    from main import fetch_related_in_batches
    client = MagicMock()
    client.get.side_effect = [
        [{"collision_id": "1", "unique_id": "A"}],
        [{"collision_id": "3", "unique_id": "B"}],
    ]

    result = fetch_related_in_batches(
        client,
        "dataset-id",
        ["1", "2", "3", "4"],
        batch_size=2,
        limit=100,
    )

    assert len(result) == 2
    assert set(result["unique_id"]) == {"A", "B"}
    assert client.get.call_count == 2


def test_fetch_related_in_batches_empty_ids_returns_empty_df():
    from main import fetch_related_in_batches
    client = MagicMock()
    result = fetch_related_in_batches(
        client,
        "dataset-id",
        [],
        batch_size=2,
        limit=100,
    )
    assert result.empty
    assert client.get.call_count == 0


def test_weather_date_range_returns_min_max_dates():
    from main import _weather_date_range

    crashes = pd.DataFrame([
        {"crash_date": "2024-01-03"},
        {"crash_date": "2024-01-01"},
        {"crash_date": "2024-01-05"},
    ])

    assert _weather_date_range(crashes) == ("2024-01-01", "2024-01-05")


def test_build_weather_observation_builds_rows_for_each_borough():
    from main import build_weather_observation

    class FakeVariable:
        def __init__(self, values):
            self.values = values

        def ValuesAsNumpy(self):
            return self.values

    class FakeHourly:
        def __init__(self):
            self.variables = [
                FakeVariable([0.1, 0.2]),
                FakeVariable([10000.0, 9500.0]),
                FakeVariable([3.3, 3.8]),
                FakeVariable([10.1, 9.8]),
                FakeVariable([0.0, 0.0]),
            ]

        def Time(self):
            return 1704067200  # 2024-01-01T00:00:00Z

        def TimeEnd(self):
            return 1704074400  # 2024-01-01T02:00:00Z

        def Interval(self):
            return 3600

        def Variables(self, i):
            return self.variables[i]

    class FakeResponse:
        def Hourly(self):
            return FakeHourly()

    client = MagicMock()
    client.weather_api.return_value = [FakeResponse()]

    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    boroughs = pd.DataFrame([
        {"borough_id": 1, "borough_name": "MANHATTAN"},
        {"borough_id": 2, "borough_name": "BRONX"},
    ])
    cfg = {
        "open_meteo": {
            "archive_url": "https://archive-api.open-meteo.com/v1/archive",
            "timezone": "UTC",
            "visibility_source": "none",
            "borough_coordinates": {
                "MANHATTAN": {"latitude": 40.7, "longitude": -74.0},
                "BRONX": {"latitude": 40.8, "longitude": -73.9},
            },
        }
    }

    result = build_weather_observation(crashes, boroughs, cfg, open_meteo_client=client)

    assert list(result.columns) == [
        "observation_id",
        "borough_id",
        "observation_date",
        "observation_hour",
        "hourly_precip",
        "hourly_visibility",
        "hourly_wind_speed",
        "hourly_temperature",
        "snow_depth",
    ]
    assert len(result) == 4
    assert result["observation_id"].tolist() == [1, 2, 3, 4]
    assert set(result["borough_id"]) == {1, 2}
    assert client.weather_api.call_count == 2


def test_build_weather_observation_missing_coordinates_skips_borough():
    from main import build_weather_observation

    class FakeVariable:
        def __init__(self, values):
            self.values = values

        def ValuesAsNumpy(self):
            return self.values

    class FakeHourly:
        def Time(self):
            return 1704067200

        def TimeEnd(self):
            return 1704070800

        def Interval(self):
            return 3600

        def Variables(self, i):
            return [
                FakeVariable([0.1]),
                FakeVariable([10000.0]),
                FakeVariable([3.3]),
                FakeVariable([10.1]),
                FakeVariable([0.0]),
            ][i]

    class FakeResponse:
        def Hourly(self):
            return FakeHourly()

    client = MagicMock()
    client.weather_api.return_value = [FakeResponse()]

    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    boroughs = pd.DataFrame([{"borough_id": 1, "borough_name": "MANHATTAN"}])
    cfg = {"open_meteo": {"timezone": "UTC", "visibility_source": "none", "borough_coordinates": {}}}

    result = build_weather_observation(crashes, boroughs, cfg, open_meteo_client=client)

    assert result.empty
    assert client.weather_api.call_count == 0


def test_build_weather_observation_fills_visibility_from_fallback():
    from main import build_weather_observation

    class FakeVariable:
        def __init__(self, values):
            self.values = values

        def ValuesAsNumpy(self):
            return self.values

    class FakeHourly:
        def __init__(self):
            self.variables = [
                FakeVariable([0.1]),
                FakeVariable([float("nan")]),
                FakeVariable([3.3]),
                FakeVariable([10.1]),
                FakeVariable([0.0]),
            ]

        def Time(self):
            return 1704067200

        def TimeEnd(self):
            return 1704070800

        def Interval(self):
            return 3600

        def Variables(self, i):
            return self.variables[i]

    class FakeResponse:
        def Hourly(self):
            return FakeHourly()

    client = MagicMock()
    client.weather_api.return_value = [FakeResponse()]

    crashes = pd.DataFrame([{"collision_id": "1", "crash_date": "2024-01-01"}])
    boroughs = pd.DataFrame([{"borough_id": 1, "borough_name": "MANHATTAN"}])
    cfg = {
        "open_meteo": {
            "archive_url": "https://archive-api.open-meteo.com/v1/archive",
            "timezone": "UTC",
            "visibility_source": "meteostat",
            "borough_coordinates": {
                "MANHATTAN": {"latitude": 40.7, "longitude": -74.0},
            },
        }
    }

    def fake_visibility_fetcher(latitude, longitude, start_date, end_date, timezone):
        return pd.DataFrame([{
            "observation_date": "2024-01-01",
            "observation_hour": 0,
            "hourly_visibility": 9000.0,
        }])

    result = build_weather_observation(
        crashes,
        boroughs,
        cfg,
        open_meteo_client=client,
        visibility_fetcher=fake_visibility_fetcher,
    )

    assert len(result) == 1
    assert result.iloc[0]["hourly_visibility"] == 9000.0


def test_fill_visibility_from_iem_reuses_station_fetches():
    from main import fill_visibility_from_iem

    weather = pd.DataFrame([
        {"borough_id": 1, "observation_date": "2024-01-01", "observation_hour": 0, "hourly_visibility": float("nan")},
        {"borough_id": 2, "observation_date": "2024-01-01", "observation_hour": 0, "hourly_visibility": float("nan")},
    ])
    boroughs = pd.DataFrame([
        {"borough_id": 1, "borough_name": "MANHATTAN"},
        {"borough_id": 2, "borough_name": "BRONX"},
    ])
    cfg = {
        "open_meteo": {
            "timezone": "UTC",
            "visibility_iem_stations": {
                "MANHATTAN": "KLGA",
                "BRONX": "KLGA",
            },
        }
    }

    calls = []

    def fake_iem_fetcher(station, start_date, end_date, timezone):
        calls.append(station)
        return pd.DataFrame([{
            "observation_date": "2024-01-01",
            "observation_hour": 0,
            "hourly_visibility": 8000.0,
        }])

    result = fill_visibility_from_iem(
        weather,
        boroughs,
        cfg,
        start_date="2024-01-01",
        end_date="2024-01-01",
        iem_fetcher=fake_iem_fetcher,
    )

    assert calls == ["KLGA"]
    assert result["hourly_visibility"].tolist() == [8000.0, 8000.0]



def test_write_output_creates_csv(tmp_path):
    from main import write_output
    df = pd.DataFrame([{"col": "val"}])
    cfg = {"output": {"directory": str(tmp_path), "filename": "result", "formats": ["csv"]}}

    write_output(df, cfg)

    assert (tmp_path / "result.csv").exists()


def test_write_output_applies_csv_prefix(tmp_path):
    from main import write_output
    df = pd.DataFrame([{"col": "val"}])
    cfg = {
        "output": {
            "directory": str(tmp_path),
            "filename": "result",
            "formats": ["csv"],
            "csv_prefix": "st_",
        }
    }

    write_output(df, cfg)

    assert (tmp_path / "st_result.csv").exists()
    assert not (tmp_path / "result.csv").exists()


def test_write_output_creates_parquet(tmp_path):
    from main import write_output
    df = pd.DataFrame([{"col": "val"}])
    cfg = {"output": {"directory": str(tmp_path), "filename": "result", "formats": ["parquet"]}}

    write_output(df, cfg)

    assert (tmp_path / "result.parquet").exists()


def test_write_output_creates_both(tmp_path):
    from main import write_output
    df = pd.DataFrame([{"col": "val"}])
    cfg = {"output": {"directory": str(tmp_path), "filename": "result", "formats": ["csv", "parquet"]}}

    write_output(df, cfg)

    assert (tmp_path / "result.csv").exists()
    assert (tmp_path / "result.parquet").exists()


def test_write_output_creates_directory(tmp_path):
    from main import write_output
    df = pd.DataFrame([{"col": "val"}])
    nested = tmp_path / "a" / "b"
    cfg = {"output": {"directory": str(nested), "filename": "result", "formats": ["csv"]}}

    write_output(df, cfg)

    assert (nested / "result.csv").exists()


def test_write_output_empty_dataframe_still_writes(tmp_path):
    from main import write_output
    df = pd.DataFrame()
    cfg = {"output": {"directory": str(tmp_path), "filename": "result", "formats": ["csv"]}}

    write_output(df, cfg)

    assert (tmp_path / "result.csv").exists()

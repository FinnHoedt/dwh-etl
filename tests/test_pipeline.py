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

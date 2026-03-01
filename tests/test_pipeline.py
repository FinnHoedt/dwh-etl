import pytest
import pandas as pd
import yaml
from pathlib import Path


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

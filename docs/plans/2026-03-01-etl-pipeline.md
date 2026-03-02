# ETL Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rewrite `main.py` as a clean, typed, testable ETL pipeline that fetches NYC crash data, joins it with persons and vehicles, and writes output to CSV and Parquet.

**Architecture:** Functions-only design (`load_config`, `build_client`, `fetch_dataset`, `build_id_filter`, `merge_datasets`, `write_output`, `main`). All parameters live in `config.yaml`. Credentials in `.env`. Tests mock the Socrata client.

**Tech Stack:** Python 3.13, pandas, sodapy, pyyaml, pyarrow, python-dotenv, pytest

---

### Task 1: Add dependencies, create config.yaml and tests directory

**Files:**
- Modify: `pyproject.toml`
- Create: `config.yaml`
- Create: `tests/__init__.py`
- Create: `tests/test_pipeline.py` (empty stub)
- Modify: `.gitignore` (add `output/`)

**Step 1: Update pyproject.toml**

Replace the `dependencies` block:

```toml
[project]
name = "etl"
version = "0.1.0"
description = "NYC crash data ETL pipeline"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "python-dotenv>=1.0.0",
    "pandas>=3.0.1",
    "sodapy>=2.2.0",
    "pyyaml>=6.0",
    "pyarrow>=15.0",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
]
```

**Step 2: Create config.yaml**

```yaml
socrata:
  domain: data.cityofnewyork.us
  datasets:
    crashes: h9gi-nx95
    persons: f55k-p6yu
    vehicles: bm4k-52h4
  limit: 100

output:
  directory: output
  filename: crashes_merged
  formats:
    - csv
    - parquet
```

**Step 3: Create tests/__init__.py**

Empty file.

**Step 4: Create tests/test_pipeline.py stub**

```python
# tests/test_pipeline.py
```

**Step 5: Add output/ to .gitignore**

Create `.gitignore` if it doesn't exist, add:

```
output/
.env
__pycache__/
.pytest_cache/
```

**Step 6: Sync dependencies**

```bash
uv sync --dev
```

Expected: dependencies installed without errors.

**Step 7: Commit**

```bash
git add pyproject.toml uv.lock config.yaml tests/__init__.py tests/test_pipeline.py .gitignore
git commit -m "chore: add deps, config.yaml, test scaffold"
```

---

### Task 2: Implement `load_config`

**Files:**
- Modify: `main.py` (replace all contents)
- Modify: `tests/test_pipeline.py`

**Step 1: Write the failing test**

In `tests/test_pipeline.py`:

```python
import pytest
import pandas as pd
import tempfile
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
```

**Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_pipeline.py::test_load_config_returns_dict -v
```

Expected: FAIL with `ImportError` or `ModuleNotFoundError`.

**Step 3: Write main.py with load_config**

Replace `main.py` entirely:

```python
import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from sodapy import Socrata

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)
```

**Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pipeline.py -v
```

Expected: 2 PASSED.

**Step 5: Commit**

```bash
git add main.py tests/test_pipeline.py
git commit -m "feat: add load_config with tests"
```

---

### Task 3: Implement `build_id_filter`

**Files:**
- Modify: `main.py`
- Modify: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

Append to `tests/test_pipeline.py`:

```python
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
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline.py -k "build_id_filter" -v
```

Expected: FAIL with `ImportError`.

**Step 3: Add build_id_filter to main.py**

```python
def build_id_filter(ids: list[str]) -> str:
    quoted = ", ".join(f"'{i}'" for i in ids)
    return f"collision_id in({quoted})"
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline.py -k "build_id_filter" -v
```

Expected: 3 PASSED.

**Step 5: Commit**

```bash
git add main.py tests/test_pipeline.py
git commit -m "feat: add build_id_filter with tests"
```

---

### Task 4: Implement `fetch_dataset`

**Files:**
- Modify: `main.py`
- Modify: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

Append to `tests/test_pipeline.py`:

```python
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
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline.py -k "fetch_dataset" -v
```

Expected: FAIL with `ImportError`.

**Step 3: Add fetch_dataset to main.py**

```python
def fetch_dataset(client: Socrata, dataset_id: str, **kwargs) -> pd.DataFrame:
    try:
        records = client.get(dataset_id, **kwargs)
    except Exception:
        logger.error("Failed to fetch dataset %s", dataset_id, exc_info=True)
        return pd.DataFrame()

    if not records:
        logger.warning("Dataset %s returned no records", dataset_id)
        return pd.DataFrame()

    return pd.DataFrame.from_records(records)
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline.py -k "fetch_dataset" -v
```

Expected: 3 PASSED.

**Step 5: Commit**

```bash
git add main.py tests/test_pipeline.py
git commit -m "feat: add fetch_dataset with empty/error handling"
```

---

### Task 5: Implement `merge_datasets`

**Files:**
- Modify: `main.py`
- Modify: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

Append to `tests/test_pipeline.py`:

```python
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
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline.py -k "merge_datasets" -v
```

Expected: FAIL with `ImportError`.

**Step 3: Add merge_datasets to main.py**

```python
def merge_datasets(
    crashes: pd.DataFrame,
    vehicles: pd.DataFrame,
    persons: pd.DataFrame,
) -> pd.DataFrame:
    for df in [crashes, vehicles, persons]:
        if not df.empty and "collision_id" in df.columns:
            df["collision_id"] = df["collision_id"].astype(str)

    merged = pd.merge(
        crashes,
        vehicles if not vehicles.empty else pd.DataFrame(columns=["collision_id"]),
        on="collision_id",
        how="left",
        suffixes=("_crash", "_veh"),
    )
    return pd.merge(
        merged,
        persons if not persons.empty else pd.DataFrame(columns=["collision_id"]),
        on="collision_id",
        how="left",
        suffixes=("_crash", "_pers"),
    )
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline.py -k "merge_datasets" -v
```

Expected: 5 PASSED.

**Step 5: Commit**

```bash
git add main.py tests/test_pipeline.py
git commit -m "feat: add merge_datasets with empty DataFrame handling"
```

---

### Task 6: Implement `write_output`

**Files:**
- Modify: `main.py`
- Modify: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

Append to `tests/test_pipeline.py`:

```python
def test_write_output_creates_csv(tmp_path):
    from main import write_output
    df = pd.DataFrame([{"col": "val"}])
    cfg = {"output": {"directory": str(tmp_path), "filename": "result", "formats": ["csv"]}}

    write_output(df, cfg)

    assert (tmp_path / "result.csv").exists()


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
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline.py -k "write_output" -v
```

Expected: FAIL with `ImportError`.

**Step 3: Add write_output to main.py**

```python
def write_output(df: pd.DataFrame, cfg: dict) -> None:
    out_cfg = cfg["output"]
    directory = Path(out_cfg["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    filename = out_cfg["filename"]

    for fmt in out_cfg.get("formats", []):
        path = directory / f"{filename}.{fmt}"
        if fmt == "csv":
            df.to_csv(path, index=False)
        elif fmt == "parquet":
            df.to_parquet(path, index=False)
        logger.info("Written %s: %s", fmt.upper(), path)
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline.py -k "write_output" -v
```

Expected: 5 PASSED.

**Step 5: Commit**

```bash
git add main.py tests/test_pipeline.py
git commit -m "feat: add write_output with CSV and Parquet support"
```

---

### Task 7: Implement `build_client` and `main`, rewrite entry point

**Files:**
- Modify: `main.py`

**Step 1: Add build_client and main to main.py**

Append to `main.py`:

```python
def build_client(cfg: dict) -> Socrata:
    load_dotenv()
    return Socrata(
        cfg["socrata"]["domain"],
        os.getenv("APP_TOKEN"),
        username=os.getenv("CLIENT_ID"),
        password=os.getenv("CLIENT_SECRET"),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = load_config()
    client = build_client(cfg)

    datasets = cfg["socrata"]["datasets"]
    limit = cfg["socrata"]["limit"]

    crashes = fetch_dataset(client, datasets["crashes"], limit=limit)
    if crashes.empty:
        logger.warning("No crashes fetched — exiting.")
        return

    if "collision_id" not in crashes.columns:
        logger.error("Crashes dataset is missing 'collision_id' column — exiting.")
        return

    id_filter = build_id_filter(crashes["collision_id"].tolist())

    vehicles = fetch_dataset(client, datasets["vehicles"], where=id_filter)
    persons = fetch_dataset(client, datasets["persons"], where=id_filter)

    if vehicles.empty:
        logger.warning("No vehicles found for fetched crashes.")
    if persons.empty:
        logger.warning("No persons found for fetched crashes.")

    final = merge_datasets(crashes, vehicles, persons)

    logger.info("Crashes:     %d", len(crashes))
    logger.info("Vehicles:    %d", len(vehicles))
    logger.info("Persons:     %d", len(persons))
    logger.info("Merged rows: %d", len(final))

    write_output(final, cfg)


if __name__ == "__main__":
    main()
```

**Step 2: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: All tests PASSED.

**Step 3: Smoke test (requires valid .env)**

```bash
uv run python main.py
```

Expected: INFO log lines, `output/crashes_merged.csv` and `output/crashes_merged.parquet` created.

**Step 4: Commit**

```bash
git add main.py
git commit -m "feat: add build_client and main, complete pipeline"
```

---

### Task 8: Final verification

**Step 1: Run full test suite one last time**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: All tests PASSED, no warnings about missing imports.

**Step 2: Verify output files exist after a real run**

```bash
uv run python main.py && ls -lh output/
```

Expected: `crashes_merged.csv` and `crashes_merged.parquet` listed with non-zero sizes.

**Step 3: Final commit**

```bash
git add .
git commit -m "chore: final verification pass"
```

# Precinct Spatial Join Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Produce `precinct.csv` (78 rows from NYC police precinct polygons) and add `precinct_id` to `location.csv` via a point-in-polygon spatial join on crash lat/lon.

**Architecture:** Fetch precinct polygon boundaries from Socrata (`y76i-bdw7`), parse `the_geom` into a geopandas GeoDataFrame, use `gpd.sjoin` to assign each crash location to a precinct, build the Precinct dimension table with borough assignment via a hardcoded lookup dict (NYC precinct numbers → borough names). `build_location` gains two optional params (`precincts_gdf`, `precinct_df`) and remains backwards compatible.

**Tech Stack:** Python 3.13, pandas, geopandas, shapely, sodapy, pytest

---

### Task 1: Add geopandas dependency and precincts dataset to config

**Files:**
- Modify: `pyproject.toml`
- Modify: `config.yaml`

**Step 1: Add geopandas to pyproject.toml dependencies**

In the `dependencies` list, add:
```toml
"geopandas>=1.0",
```

**Step 2: Add precincts dataset to config.yaml**

In `config.yaml` under `socrata.datasets`, add:
```yaml
    precincts: y76i-bdw7
```

So the datasets section looks like:
```yaml
  datasets:
    crashes: h9gi-nx95
    persons: f55k-p6yu
    vehicles: bm4k-52h4
    precincts: y76i-bdw7
```

**Step 3: Sync dependencies**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv sync --dev
```

Expected: geopandas and its deps (shapely, pyproj, fiona, etc.) installed. No errors.

**Step 4: Verify import works**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run python -c "import geopandas; print(geopandas.__version__)"
```

Expected: version string printed, no error.

**Step 5: Run existing tests to confirm no regressions**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 52 PASSED.

**Step 6: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add pyproject.toml uv.lock config.yaml
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "chore: add geopandas dep and precincts dataset to config

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Add `PRECINCT_BOROUGHS`, `build_precinct`, and `parse_precincts_gdf` to transform.py

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Read the current end of tests/test_transform.py**

**Step 2: Append these tests to tests/test_transform.py**

```python


import json
import geopandas as gpd


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
```

**Step 3: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "precinct or parse_precincts" -v
```

Expected: FAIL with ImportError.

**Step 4: Read transform.py, then append this code**

```python


import geopandas as gpd
from shapely.geometry import shape


PRECINCT_BOROUGHS: dict[int, str] = {
    # Manhattan
    1: "MANHATTAN", 5: "MANHATTAN", 6: "MANHATTAN", 7: "MANHATTAN",
    9: "MANHATTAN", 10: "MANHATTAN", 13: "MANHATTAN", 14: "MANHATTAN",
    17: "MANHATTAN", 18: "MANHATTAN", 19: "MANHATTAN", 20: "MANHATTAN",
    22: "MANHATTAN", 23: "MANHATTAN", 24: "MANHATTAN", 25: "MANHATTAN",
    26: "MANHATTAN", 28: "MANHATTAN", 30: "MANHATTAN", 32: "MANHATTAN",
    33: "MANHATTAN", 34: "MANHATTAN",
    # Bronx
    40: "BRONX", 41: "BRONX", 42: "BRONX", 43: "BRONX", 44: "BRONX",
    45: "BRONX", 46: "BRONX", 47: "BRONX", 48: "BRONX", 49: "BRONX",
    50: "BRONX", 52: "BRONX",
    # Brooklyn
    60: "BROOKLYN", 61: "BROOKLYN", 62: "BROOKLYN", 63: "BROOKLYN",
    66: "BROOKLYN", 67: "BROOKLYN", 68: "BROOKLYN", 69: "BROOKLYN",
    70: "BROOKLYN", 71: "BROOKLYN", 72: "BROOKLYN", 73: "BROOKLYN",
    75: "BROOKLYN", 76: "BROOKLYN", 77: "BROOKLYN", 78: "BROOKLYN",
    79: "BROOKLYN", 81: "BROOKLYN", 83: "BROOKLYN", 84: "BROOKLYN",
    88: "BROOKLYN", 90: "BROOKLYN", 94: "BROOKLYN",
    # Queens
    100: "QUEENS", 101: "QUEENS", 102: "QUEENS", 103: "QUEENS",
    104: "QUEENS", 105: "QUEENS", 106: "QUEENS", 107: "QUEENS",
    108: "QUEENS", 109: "QUEENS", 110: "QUEENS", 111: "QUEENS",
    112: "QUEENS", 113: "QUEENS", 114: "QUEENS", 115: "QUEENS",
    # Staten Island
    120: "STATEN ISLAND", 121: "STATEN ISLAND", 122: "STATEN ISLAND",
    123: "STATEN ISLAND",
}


def parse_precincts_gdf(precincts: pd.DataFrame) -> gpd.GeoDataFrame:
    if precincts.empty or "the_geom" not in precincts.columns:
        return gpd.GeoDataFrame(columns=["precinct", "geometry"])

    def _parse(geom) -> object:
        if isinstance(geom, str):
            return shape(json.loads(geom))
        if isinstance(geom, dict):
            return shape(geom)
        return None

    return gpd.GeoDataFrame(
        {"precinct": pd.to_numeric(precincts["precinct"], errors="coerce")},
        geometry=precincts["the_geom"].apply(_parse),
        crs="EPSG:4326",
    )


def build_precinct(precincts: pd.DataFrame, boroughs: pd.DataFrame) -> pd.DataFrame:
    cols = ["precinct_id", "borough_id", "precinct_number", "precinct_name"]
    if precincts.empty or "precinct" not in precincts.columns:
        return pd.DataFrame(columns=cols)

    numbers = pd.to_numeric(precincts["precinct"], errors="coerce").dropna().astype(int)
    if len(numbers) == 0:
        return pd.DataFrame(columns=cols)

    borough_lookup = (
        {} if boroughs.empty
        else dict(zip(boroughs["borough_name"], boroughs["borough_id"]))
    )

    return pd.DataFrame({
        "precinct_id": range(1, len(numbers) + 1),
        "borough_id": [borough_lookup.get(PRECINCT_BOROUGHS.get(n)) for n in numbers],
        "precinct_number": numbers.astype(str).values,
        "precinct_name": [None] * len(numbers),
    })
```

**Step 5: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 58 PASSED (52 existing + 6 new).

**Step 6: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add PRECINCT_BOROUGHS, parse_precincts_gdf, build_precinct

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Update `build_location` to include `precinct_id` via spatial join

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Context:** `build_location` currently returns 6 columns. It needs a new `precinct_id` column (3rd position, after `borough_id`). Two new optional params: `precincts_gdf` and `precinct_df`. When not supplied, `precinct_id` is NULL (backwards compatible — existing tests still pass). A private `_assign_precinct_id` helper keeps `build_location` readable.

**Step 1: Append these tests to tests/test_transform.py**

```python


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
```

**Step 2: Run new tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "precinct_id" -v
```

Expected: `test_build_location_includes_precinct_id_column` FAIL (column missing), others FAIL (ImportError or missing column).

**Step 3: Read transform.py — find the current `build_location` function**

**Step 4: Replace `build_location` with the updated version**

Find the current `build_location` function and replace it entirely with:

```python
def _assign_precinct_id(
    crashes: pd.DataFrame,
    lat: pd.Series,
    lon: pd.Series,
    precincts_gdf: gpd.GeoDataFrame | None,
    precinct_df: pd.DataFrame | None,
) -> pd.Series:
    empty = pd.Series(index=crashes.index, dtype=object)
    if (
        precincts_gdf is None or precincts_gdf.empty
        or precinct_df is None or precinct_df.empty
    ):
        return empty

    valid_mask = lat.notna() & lon.notna()
    if not valid_mask.any():
        return empty

    points = gpd.GeoDataFrame(
        index=crashes.index[valid_mask],
        geometry=gpd.points_from_xy(lon[valid_mask], lat[valid_mask]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(points, precincts_gdf[["precinct", "geometry"]], how="left", predicate="within")
    joined = joined[~joined.index.duplicated(keep="first")]

    num_to_id = dict(zip(
        pd.to_numeric(precinct_df["precinct_number"], errors="coerce"),
        precinct_df["precinct_id"],
    ))
    return joined["precinct"].map(num_to_id).reindex(crashes.index)


def build_location(
    crashes: pd.DataFrame,
    boroughs: pd.DataFrame,
    precincts_gdf: gpd.GeoDataFrame | None = None,
    precinct_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    cols = ["location_id", "borough_id", "precinct_id", "street_name", "zip_code", "latitude", "longitude"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    borough_lookup = (
        {} if boroughs.empty
        else dict(zip(boroughs["borough_name"], boroughs["borough_id"]))
    )
    lat = pd.to_numeric(_col(crashes, "latitude"), errors="coerce")
    lon = pd.to_numeric(_col(crashes, "longitude"), errors="coerce")

    return pd.DataFrame({
        "location_id": crashes["collision_id"],
        "borough_id": _col(crashes, "borough").map(borough_lookup),
        "precinct_id": _assign_precinct_id(crashes, lat, lon, precincts_gdf, precinct_df),
        "street_name": _col(crashes, "on_street_name").fillna(_col(crashes, "cross_street_name")),
        "zip_code": _col(crashes, "zip_code"),
        "latitude": lat,
        "longitude": lon,
    })
```

**Step 5: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 62 PASSED (52 original + 6 precinct/parse + 4 new location tests). Zero failures — existing location tests still pass because `precinct_id` is just an added column.

**Step 6: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add precinct_id to build_location via spatial join

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 4: Update `main.py` to fetch precincts and wire everything

**Files:**
- Modify: `main.py`

**Step 1: Read main.py fully**

**Step 2: Update the import from transform** — add `build_precinct` and `parse_precincts_gdf`:

```python
from transform import (
    build_borough,
    build_contributing_factor,
    build_crash,
    build_location,
    build_person,
    build_person_type,
    build_precinct,
    build_vehicle,
    build_vehicle_factor,
    build_vehicle_type,
    parse_precincts_gdf,
)
```

**Step 3: In `main()`, after fetching vehicles and persons, add precinct fetch**

Find this block:
```python
    vehicles = fetch_dataset(client, datasets["vehicles"], where=id_filter)
    persons = fetch_dataset(client, datasets["persons"], where=id_filter)
```

Add after it:
```python
    precincts_raw = fetch_dataset(client, datasets["precincts"])
    if precincts_raw.empty:
        logger.warning("No precincts fetched — precinct_id will be NULL in locations.")
    precincts_gdf = parse_precincts_gdf(precincts_raw)
```

Note: precincts are fetched without `where=id_filter` — we want all 78, not filtered by crash IDs.

**Step 4: Update the transform section in `main()`**

Find:
```python
    boroughs = build_borough(crashes)
    locations = build_location(crashes, boroughs)
```

Replace the `build_precinct` and `build_location` calls with:
```python
    boroughs = build_borough(crashes)
    precinct_df = build_precinct(precincts_raw, boroughs)
    locations = build_location(crashes, boroughs, precincts_gdf, precinct_df)
```

**Step 5: Add `precinct` to the entities dict**

Find the `entities` dict and add `"precinct": precinct_df` after `"borough"` and before `"location"`:

```python
    entities: dict[str, pd.DataFrame] = {
        "borough": boroughs,
        "precinct": precinct_df,
        "location": locations,
        "crash": crash_df,
        "vehicle_type": vehicle_types,
        "vehicle": vehicle_df,
        "person_type": person_types,
        "person": person_df,
        "contributing_factor": factors,
        "vehicle_factor": vehicle_factors,
    }
```

**Step 6: Run the full test suite**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 62 PASSED.

**Step 7: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add main.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: fetch precincts and wire spatial join in main

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 5: Final verification

**Step 1: Run full test suite**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 62 PASSED, 0 failures.

**Step 2: Check git log**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl log --oneline -8
```

Expected: clean linear history with the 4 new commits at top.

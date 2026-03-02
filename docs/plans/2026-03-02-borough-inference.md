# Borough Inference Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Infer missing borough from lat/lon via precinct spatial join and drop crashes with neither borough nor lat/lon from all output files.

**Architecture:** A new `filter_locatable_crashes` function enriches crashes by filling null borough values using the precinct spatial join (lat/lon → precinct polygon → `PRECINCT_BOROUGHS` → borough name), then drops rows that remain without a borough. Called in `main.py` before all transforms so the cascade is automatic — all 8 builders receive the pre-filtered DataFrame. `build_borough` is updated to always seed all 5 NYC boroughs so inferred borough names always have a matching `borough_id`.

**Tech Stack:** Python 3.13, pandas, geopandas, shapely, pytest

---

### Task 1: Seed `build_borough` with all 5 NYC boroughs

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Add a new test for the seeding behaviour**

In `tests/test_transform.py`, append at the end:

```python


def test_build_borough_always_includes_all_five_nyc_boroughs():
    from transform import build_borough
    result = build_borough(pd.DataFrame())
    assert set(result["borough_name"]) == {"MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"}
    assert len(result) == 5
```

**Step 2: Run the new test to confirm it fails**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py::test_build_borough_always_includes_all_five_nyc_boroughs -v
```

Expected: FAIL — empty DataFrame returned, assertion on length fails.

**Step 3: Read transform.py lines 1–30 to see the current `build_borough`**

**Step 4: Replace `build_borough` in transform.py**

Find the block:
```python
def build_borough(crashes: pd.DataFrame) -> pd.DataFrame:
    cols = ["borough_id", "borough_name", "borough_code"]
    if crashes.empty or "borough" not in crashes.columns:
        return pd.DataFrame(columns=cols)

    names = (
        crashes["borough"]
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .unique()
    )
    if len(names) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "borough_id": range(1, len(names) + 1),
        "borough_name": names,
        "borough_code": names,
    })
```

Replace it with:

```python
NYC_BOROUGHS: list[str] = ["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"]


def build_borough(crashes: pd.DataFrame) -> pd.DataFrame:
    crash_names: list[str] = []
    if not crashes.empty and "borough" in crashes.columns:
        crash_names = (
            crashes["borough"]
            .dropna()
            .pipe(lambda s: s[s.str.strip() != ""])
            .unique()
            .tolist()
        )

    names = list(dict.fromkeys(NYC_BOROUGHS + crash_names))

    return pd.DataFrame({
        "borough_id": range(1, len(names) + 1),
        "borough_name": names,
        "borough_code": names,
    })
```

Note: `dict.fromkeys` deduplicates while preserving insertion order — NYC_BOROUGHS entries always come first.

**Step 5: Update two existing tests that now have wrong expectations**

Find and replace `test_build_borough_empty_crashes`:

Old:
```python
def test_build_borough_empty_crashes():
    from transform import build_borough
    result = build_borough(pd.DataFrame())
    assert result.empty
    assert list(result.columns) == ["borough_id", "borough_name", "borough_code"]
```

New:
```python
def test_build_borough_empty_crashes():
    from transform import build_borough
    result = build_borough(pd.DataFrame())
    assert len(result) == 5
    assert list(result.columns) == ["borough_id", "borough_name", "borough_code"]
```

Find and replace `test_build_borough_missing_column`:

Old:
```python
def test_build_borough_missing_column():
    from transform import build_borough
    result = build_borough(pd.DataFrame([{"collision_id": "1"}]))
    assert result.empty
```

New:
```python
def test_build_borough_missing_column():
    from transform import build_borough
    result = build_borough(pd.DataFrame([{"collision_id": "1"}]))
    assert len(result) == 5
```

**Step 6: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 63 PASSED (62 existing + 1 new). The two updated tests now pass with the new behaviour.

**Step 7: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "$(cat <<'EOF'
feat: seed build_borough with all 5 NYC boroughs

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add `filter_locatable_crashes` to transform.py

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append 4 new tests to `tests/test_transform.py`**

```python


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
```

Note: precinct 1 maps to `"MANHATTAN"` via `PRECINCT_BOROUGHS` — that is the expected inferred borough.

**Step 2: Run new tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "filter_locatable" -v
```

Expected: FAIL with ImportError — `filter_locatable_crashes` does not exist yet.

**Step 3: Append `filter_locatable_crashes` to the end of transform.py**

Read the current end of `transform.py` first to confirm the last line, then append:

```python


def filter_locatable_crashes(
    crashes: pd.DataFrame,
    precincts_gdf: gpd.GeoDataFrame | None = None,
) -> pd.DataFrame:
    if crashes.empty:
        return crashes

    borough_col = _col(crashes, "borough")
    borough_null = borough_col.isna() | borough_col.str.strip().eq("")
    lat = pd.to_numeric(_col(crashes, "latitude"), errors="coerce")
    lon = pd.to_numeric(_col(crashes, "longitude"), errors="coerce")
    has_latlon = lat.notna() & lon.notna()

    infer_mask = borough_null & has_latlon
    if infer_mask.any() and precincts_gdf is not None and not precincts_gdf.empty:
        clean_gdf = precincts_gdf[precincts_gdf.geometry.notna()][["precinct", "geometry"]]
        if not clean_gdf.empty:
            points = gpd.GeoDataFrame(
                index=crashes.index[infer_mask],
                geometry=gpd.points_from_xy(lon[infer_mask], lat[infer_mask]),
                crs="EPSG:4326",
            )
            joined = gpd.sjoin(points, clean_gdf, how="left", predicate="intersects")
            joined = joined[~joined.index.duplicated(keep="first")]
            inferred = joined["precinct"].apply(
                lambda n: PRECINCT_BOROUGHS.get(int(n)) if pd.notna(n) else None
            )
            crashes = crashes.copy()
            crashes.loc[inferred.index, "borough"] = inferred.values

    borough_col = _col(crashes, "borough")
    locatable = borough_col.notna() & (borough_col.str.strip() != "")
    return crashes[locatable].reset_index(drop=True)
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 67 PASSED (63 + 4 new). If any fail, read the error and fix before continuing.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "$(cat <<'EOF'
feat: add filter_locatable_crashes to infer and filter by borough

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Wire `filter_locatable_crashes` in main.py

**Files:**
- Modify: `main.py`

**Step 1: Read main.py fully**

**Step 2: Add `filter_locatable_crashes` to the import block**

Find:
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

Replace with:
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
    filter_locatable_crashes,
    parse_precincts_gdf,
)
```

**Step 3: Filter crashes and related entities in main()**

Find this block:
```python
    precincts_raw = fetch_dataset(client, datasets["precincts"])
    if precincts_raw.empty:
        logger.warning("No precincts fetched — precinct_id will be NULL in locations.")
    precincts_gdf = parse_precincts_gdf(precincts_raw)

    if vehicles.empty:
        logger.warning("No vehicles found for fetched crashes.")
    if persons.empty:
        logger.warning("No persons found for fetched crashes.")
```

Replace with:
```python
    precincts_raw = fetch_dataset(client, datasets["precincts"])
    if precincts_raw.empty:
        logger.warning("No precincts fetched — precinct_id will be NULL in locations.")
    precincts_gdf = parse_precincts_gdf(precincts_raw)

    crashes = filter_locatable_crashes(crashes, precincts_gdf)
    if crashes.empty:
        logger.warning("No locatable crashes — nothing to output.")
        return
    valid_ids = set(crashes["collision_id"])
    if not vehicles.empty:
        vehicles = vehicles[vehicles["collision_id"].isin(valid_ids)]
    if not persons.empty:
        persons = persons[persons["collision_id"].isin(valid_ids)]

    if vehicles.empty:
        logger.warning("No vehicles found for fetched crashes.")
    if persons.empty:
        logger.warning("No persons found for fetched crashes.")
```

**Step 4: Run full test suite**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: 67 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add main.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "$(cat <<'EOF'
feat: wire filter_locatable_crashes in main, cascade to vehicles and persons

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

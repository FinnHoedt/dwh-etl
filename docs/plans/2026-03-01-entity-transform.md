# Entity Transform Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the single merged CSV output with 9 entity CSVs (Borough, Location, Crash, Vehicle_Type, Vehicle, Person_Type, Person, Contributing_Factor, Vehicle_Factor) that match the 3NF DB schema for MS SQL Server import.

**Architecture:** Add `transform.py` with one pure function per entity. A private `_col()` helper handles missing columns safely. `main.py` imports from `transform`, calls each builder in dependency order, and writes one CSV per entity. The existing `write_output` gets an optional `filename` parameter to support per-entity naming. `merge_datasets` and its tests are removed.

**Tech Stack:** Python 3.13, pandas, pytest, existing sodapy/yaml/dotenv stack

---

### Task 1: Create transform.py with `_col` helper and `build_borough`

**Files:**
- Create: `transform.py`
- Create: `tests/test_transform.py`

**Step 1: Write the failing tests**

Create `tests/test_transform.py`:

```python
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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

**Step 3: Create transform.py**

```python
import pandas as pd


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(index=df.index, dtype=object)


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

**Step 4: Run tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 5 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add transform.py with build_borough"
```

---

### Task 2: Add `build_location`

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append failing tests**

```python


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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "build_location" -v
```

Expected: FAIL.

**Step 3: Append `build_location` to transform.py**

```python


def build_location(crashes: pd.DataFrame, boroughs: pd.DataFrame) -> pd.DataFrame:
    cols = ["location_id", "borough_id", "street_name", "zip_code", "latitude", "longitude"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    borough_lookup = (
        {} if boroughs.empty
        else dict(zip(boroughs["borough_name"], boroughs["borough_id"]))
    )

    on_street = _col(crashes, "on_street_name")
    cross_street = _col(crashes, "cross_street_name")
    street = on_street.where(on_street.notna(), cross_street)

    return pd.DataFrame({
        "location_id": crashes["collision_id"],
        "borough_id": _col(crashes, "borough").map(borough_lookup),
        "street_name": street,
        "zip_code": _col(crashes, "zip_code"),
        "latitude": pd.to_numeric(_col(crashes, "latitude"), errors="coerce"),
        "longitude": pd.to_numeric(_col(crashes, "longitude"), errors="coerce"),
    })
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 10 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add build_location"
```

---

### Task 3: Add `build_crash`

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append failing tests**

```python


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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "build_crash" -v
```

Expected: FAIL.

**Step 3: Append `build_crash` to transform.py**

```python


def build_crash(crashes: pd.DataFrame) -> pd.DataFrame:
    cols = ["collision_id", "crash_date", "crash_time", "location_id",
            "number_of_persons_injured", "number_of_persons_killed"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "collision_id": crashes["collision_id"],
        "crash_date": _col(crashes, "crash_date"),
        "crash_time": _col(crashes, "crash_time"),
        "location_id": crashes["collision_id"],
        "number_of_persons_injured": pd.to_numeric(
            _col(crashes, "number_of_persons_injured"), errors="coerce"
        ),
        "number_of_persons_killed": pd.to_numeric(
            _col(crashes, "number_of_persons_killed"), errors="coerce"
        ),
    })
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 14 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add build_crash"
```

---

### Task 4: Add `build_vehicle_type` with category mapping

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append failing tests**

```python


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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "build_vehicle_type" -v
```

Expected: FAIL.

**Step 3: Append `VEHICLE_TYPE_CATEGORIES` dict and `build_vehicle_type` to transform.py**

```python


VEHICLE_TYPE_CATEGORIES: dict[str, str] = {
    "Sedan": "Passenger Vehicle",
    "Station Wagon/Sport Utility Vehicle": "Passenger Vehicle",
    "2-dr sedan": "Passenger Vehicle",
    "4-dr sedan": "Passenger Vehicle",
    "Van": "Passenger Vehicle",
    "Pick-up Truck": "Passenger Vehicle",
    "Convertible": "Passenger Vehicle",
    "Minivan": "Passenger Vehicle",
    "Box Truck": "Commercial",
    "Flat Bed": "Commercial",
    "Tractor Truck Diesel": "Commercial",
    "Tractor Truck Gasoline": "Commercial",
    "Bus": "Commercial",
    "Large Com Veh(6+ Tons)": "Commercial",
    "Small Com Veh(4 Tires)": "Commercial",
    "Tanker": "Commercial",
    "Garbage or Refuse": "Commercial",
    "Dump": "Commercial",
    "Carry All": "Commercial",
    "Bike": "Bicycle",
    "E-Bike": "Bicycle",
    "E-Scooter": "Bicycle",
    "Motorcycle": "Motorcycle",
    "Moped": "Motorcycle",
    "Motorbike": "Motorcycle",
}


def build_vehicle_type(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_type_id", "type_code", "type_description", "type_category"]
    if vehicles.empty or "vehicle_type_code" not in vehicles.columns:
        return pd.DataFrame(columns=cols)

    codes = (
        vehicles["vehicle_type_code"]
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .unique()
    )
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "vehicle_type_id": range(1, len(codes) + 1),
        "type_code": codes,
        "type_description": codes,
        "type_category": [VEHICLE_TYPE_CATEGORIES.get(c, "Unknown") for c in codes],
    })
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 19 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add build_vehicle_type with category mapping"
```

---

### Task 5: Add `build_vehicle`

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append failing tests**

```python


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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "build_vehicle and not type" -v
```

Expected: FAIL.

**Step 3: Append `build_vehicle` to transform.py**

```python


def build_vehicle(vehicles: pd.DataFrame, vehicle_types: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_id", "collision_id", "vehicle_type_id", "state_registration", "vehicle_year"]
    if vehicles.empty:
        return pd.DataFrame(columns=cols)

    type_lookup = (
        {} if vehicle_types.empty
        else dict(zip(vehicle_types["type_code"], vehicle_types["vehicle_type_id"]))
    )

    return pd.DataFrame({
        "vehicle_id": vehicles["unique_id"],
        "collision_id": vehicles["collision_id"],
        "vehicle_type_id": _col(vehicles, "vehicle_type_code").map(type_lookup),
        "state_registration": _col(vehicles, "state_registration"),
        "vehicle_year": pd.to_numeric(_col(vehicles, "vehicle_year"), errors="coerce"),
    })
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 23 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add build_vehicle"
```

---

### Task 6: Add `build_person_type` and `build_person`

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append failing tests**

```python


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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "person" -v
```

Expected: FAIL.

**Step 3: Append `build_person_type` and `build_person` to transform.py**

```python


def build_person_type(persons: pd.DataFrame) -> pd.DataFrame:
    cols = ["person_type_id", "type_code", "type_description"]
    if persons.empty or "person_type" not in persons.columns:
        return pd.DataFrame(columns=cols)

    codes = (
        persons["person_type"]
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .unique()
    )
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "person_type_id": range(1, len(codes) + 1),
        "type_code": codes,
        "type_description": codes,
    })


def build_person(persons: pd.DataFrame, person_types: pd.DataFrame) -> pd.DataFrame:
    cols = ["person_id", "collision_id", "vehicle_id", "person_type_id", "injury_type", "age", "sex"]
    if persons.empty:
        return pd.DataFrame(columns=cols)

    type_lookup = (
        {} if person_types.empty
        else dict(zip(person_types["type_code"], person_types["person_type_id"]))
    )

    vehicle_id = _col(persons, "vehicle_id").replace("", pd.NA)

    return pd.DataFrame({
        "person_id": persons["unique_id"],
        "collision_id": persons["collision_id"],
        "vehicle_id": vehicle_id,
        "person_type_id": _col(persons, "person_type").map(type_lookup),
        "injury_type": _col(persons, "person_injury"),
        "age": pd.to_numeric(_col(persons, "person_age"), errors="coerce"),
        "sex": _col(persons, "person_sex"),
    })
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 30 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add build_person_type and build_person"
```

---

### Task 7: Add `build_contributing_factor` and `build_vehicle_factor`

**Files:**
- Modify: `transform.py`
- Modify: `tests/test_transform.py`

**Step 1: Append failing tests**

```python


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
```

**Step 2: Run tests to confirm they fail**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -k "contributing_factor or vehicle_factor" -v
```

Expected: FAIL.

**Step 3: Append `FACTOR_CATEGORIES`, `build_contributing_factor`, `build_vehicle_factor` to transform.py**

```python


FACTOR_CATEGORIES: dict[str, str] = {
    "Driver Inattention/Distraction": "Driver Error",
    "Fatigued/Drowsy": "Driver Error",
    "Aggressive Driving/Road Rage": "Driver Error",
    "Unsafe Speed": "Driver Error",
    "Following Too Closely": "Driver Error",
    "Failure to Yield Right-of-Way": "Driver Error",
    "Passing or Lane Usage Improper": "Driver Error",
    "Unsafe Lane Changing": "Driver Error",
    "Backing Unsafely": "Driver Error",
    "Passing Too Closely": "Driver Error",
    "Turning Improperly": "Driver Error",
    "Traffic Control Disregarded": "Driver Error",
    "Lost Consciousness": "Driver Error",
    "Fell Asleep": "Driver Error",
    "Alcohol Involvement": "Driver Error",
    "Drugs (illegal)": "Driver Error",
    "Prescription Medication": "Driver Error",
    "Cell Phone (hands-free)": "Driver Error",
    "Cell Phone (hand-held)": "Driver Error",
    "Outside Car Distraction": "Driver Error",
    "Eating or Drinking": "Driver Error",
    "Brakes Defective": "Vehicle Defect",
    "Tires Defective": "Vehicle Defect",
    "Steering Failure": "Vehicle Defect",
    "Accelerator Defective": "Vehicle Defect",
    "Headlights Defective": "Vehicle Defect",
    "Windshield Inadequate": "Vehicle Defect",
    "Other Lighting Defects": "Vehicle Defect",
    "Vehicle Vandalism": "Vehicle Defect",
    "Oversized Vehicle": "Vehicle Defect",
    "Glare": "Environmental",
    "Pavement Slippery": "Environmental",
    "View Obstructed/Limited": "Environmental",
    "Pavement Defective": "Environmental",
    "Lane Marking Improper/Inadequate": "Environmental",
    "Traffic Control Device Improper/Non-Working": "Environmental",
    "Obstruction/Debris in Road": "Environmental",
}


def build_contributing_factor(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["factor_id", "factor_code", "factor_description", "factor_category"]
    if vehicles.empty:
        return pd.DataFrame(columns=cols)

    factor_col_names = [c for c in ["contributing_factor_1", "contributing_factor_2"] if c in vehicles.columns]
    if not factor_col_names:
        return pd.DataFrame(columns=cols)

    codes = (
        pd.concat([vehicles[c] for c in factor_col_names])
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .pipe(lambda s: s[s.str.lower() != "unspecified"])
        .unique()
    )
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "factor_id": range(1, len(codes) + 1),
        "factor_code": codes,
        "factor_description": codes,
        "factor_category": [FACTOR_CATEGORIES.get(c, "Unknown") for c in codes],
    })


def build_vehicle_factor(vehicles: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_factor_id", "vehicle_id", "factor_id"]
    if vehicles.empty or factors.empty:
        return pd.DataFrame(columns=cols)

    factor_col_names = [c for c in ["contributing_factor_1", "contributing_factor_2"] if c in vehicles.columns]
    if not factor_col_names:
        return pd.DataFrame(columns=cols)

    factor_lookup = dict(zip(factors["factor_code"], factors["factor_id"]))

    parts = []
    for col_name in factor_col_names:
        part = vehicles[["unique_id", col_name]].copy()
        part.columns = ["vehicle_id", "factor_code"]
        parts.append(part)

    combined = (
        pd.concat(parts, ignore_index=True)
        .dropna(subset=["factor_code"])
        .pipe(lambda df: df[df["factor_code"].str.strip() != ""])
        .pipe(lambda df: df[df["factor_code"].str.lower() != "unspecified"])
        .drop_duplicates(subset=["vehicle_id", "factor_code"])
    )
    combined["factor_id"] = combined["factor_code"].map(factor_lookup)
    combined = combined.dropna(subset=["factor_id"])

    result = combined[["vehicle_id", "factor_id"]].reset_index(drop=True)
    result.insert(0, "vehicle_factor_id", range(1, len(result) + 1))
    return result
```

**Step 4: Run all tests**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/test_transform.py -v
```

Expected: 39 PASSED.

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add transform.py tests/test_transform.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: add build_contributing_factor and build_vehicle_factor"
```

---

### Task 8: Update `main.py` — wire transform, update write_output, remove merge_datasets

**Files:**
- Modify: `main.py`
- Modify: `tests/test_pipeline.py` (remove merge_datasets tests)

**Context:** `merge_datasets` is replaced by the transform pipeline. Its 5 tests and the function itself are removed. `write_output` gets an optional `filename` parameter. `main()` imports from `transform` and calls each builder.

**Step 1: Read main.py and tests/test_pipeline.py** to understand current state.

**Step 2: Replace main.py entirely with this content**

```python
import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from sodapy import Socrata

from transform import (
    build_borough,
    build_contributing_factor,
    build_crash,
    build_location,
    build_person,
    build_person_type,
    build_vehicle,
    build_vehicle_factor,
    build_vehicle_type,
)

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_id_filter(ids: list[str]) -> str:
    quoted = ", ".join(f"'{i}'" for i in ids)
    return f"collision_id in({quoted})"


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


def write_output(df: pd.DataFrame, cfg: dict, filename: str | None = None) -> None:
    out_cfg = cfg["output"]
    directory = Path(out_cfg["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    name = filename or out_cfg["filename"]

    for fmt in out_cfg.get("formats", []):
        path = directory / f"{name}.{fmt}"
        if fmt == "csv":
            df.to_csv(path, index=False)
            logger.info("Written CSV: %s", path)
        elif fmt == "parquet":
            df.to_parquet(path, index=False)
            logger.info("Written Parquet: %s", path)
        else:
            logger.warning("Unknown output format ignored: %s", fmt)


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

    boroughs = build_borough(crashes)
    locations = build_location(crashes, boroughs)
    crash_df = build_crash(crashes)
    vehicle_types = build_vehicle_type(vehicles)
    vehicle_df = build_vehicle(vehicles, vehicle_types)
    person_types = build_person_type(persons)
    person_df = build_person(persons, person_types)
    factors = build_contributing_factor(vehicles)
    vehicle_factors = build_vehicle_factor(vehicles, factors)

    entities: dict[str, pd.DataFrame] = {
        "borough": boroughs,
        "location": locations,
        "crash": crash_df,
        "vehicle_type": vehicle_types,
        "vehicle": vehicle_df,
        "person_type": person_types,
        "person": person_df,
        "contributing_factor": factors,
        "vehicle_factor": vehicle_factors,
    }

    for name, df in entities.items():
        logger.info("%-25s %d rows", name, len(df))
        write_output(df, cfg, filename=name)


if __name__ == "__main__":
    main()
```

**Step 3: Remove the 5 merge_datasets tests from tests/test_pipeline.py**

Delete these 5 test functions from the file:
- `test_merge_datasets_joins_on_collision_id`
- `test_merge_datasets_empty_vehicles_keeps_crash_rows`
- `test_merge_datasets_empty_persons_keeps_crash_rows`
- `test_merge_datasets_multiple_vehicles_expands_rows`
- `test_merge_datasets_casts_collision_id_to_str`

Also remove the unused `import pandas as pd` line if it becomes unused after removing these tests (check first — it may still be used by fetch_dataset tests).

**Step 4: Run the full test suite**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v
```

Expected: 13 tests from test_pipeline.py + 39 from test_transform.py = 52 PASSED (no failures).

**Step 5: Commit**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl add main.py tests/test_pipeline.py
git -C /Users/finn/Studium/DataWarehouses/etl commit -m "feat: wire transform pipeline in main, remove merge_datasets"
```

---

### Task 9: Final verification

**Step 1: Run full test suite**

```bash
cd /Users/finn/Studium/DataWarehouses/etl && uv run pytest tests/ -v --tb=short
```

Expected: All tests PASSED, 0 failures.

**Step 2: Check git log**

```bash
git -C /Users/finn/Studium/DataWarehouses/etl log --oneline -12
```

Expected: Clean linear history with one commit per feature.

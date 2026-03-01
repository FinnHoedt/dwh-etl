# Entity Transform Design — NYC Collisions ETL

**Date:** 2026-03-01
**Status:** Approved

## Problem

The current pipeline produces one wide merged CSV (crashes × vehicles × persons). This is unsuitable for importing into MS SQL Server against a normalized 3NF schema. We need one CSV per entity, with correct FK relationships.

## Scope

9 entity CSVs from Socrata data only. `Precinct` and `Weather_Observation` are excluded (no Socrata source).

## Architecture

Two Python files:

```
etl/
├── main.py          # updated orchestration
├── transform.py     # NEW: one function per entity
├── config.yaml
└── output/
    ├── borough.csv
    ├── location.csv
    ├── crash.csv
    ├── vehicle_type.csv
    ├── vehicle.csv
    ├── person_type.csv
    ├── person.csv
    ├── contributing_factor.csv
    └── vehicle_factor.csv
```

## Key Decisions

- **Natural keys as PKs:** `collision_id`, `vehicle unique_id`, `person unique_id` used directly. No surrogates for fact entities.
- **location_id = collision_id:** Each crash has a unique location; reusing collision_id avoids a lookup table.
- **Dimension surrogates:** Borough, Vehicle_Type, Person_Type, Contributing_Factor get sequential integer PKs.
- **Factor categories:** Mapped via a lookup dict in code to: Driver Error / Vehicle Defect / Environmental / Unknown.

## Field Mapping

### Borough
| Column | Source |
|---|---|
| `borough_id` | sequential (1, 2, …) |
| `borough_name` | `crashes.borough` (deduplicated, non-null) |
| `borough_code` | same as `borough_name` |

### Location
| Column | Source |
|---|---|
| `location_id` | `crashes.collision_id` |
| `borough_id` | FK via borough_name |
| `street_name` | `crashes.on_street_name` (fallback: `cross_street_name`) |
| `zip_code` | `crashes.zip_code` |
| `latitude` | `crashes.latitude` |
| `longitude` | `crashes.longitude` |

### Crash
| Column | Source |
|---|---|
| `collision_id` | `crashes.collision_id` |
| `crash_date` | `crashes.crash_date` |
| `crash_time` | `crashes.crash_time` |
| `location_id` | same as `collision_id` |
| `number_of_persons_injured` | `crashes.number_of_persons_injured` |
| `number_of_persons_killed` | `crashes.number_of_persons_killed` |

### Vehicle_Type
| Column | Source |
|---|---|
| `vehicle_type_id` | sequential |
| `type_code` | `vehicles.vehicle_type_code` (deduplicated, non-null) |
| `type_description` | same as `type_code` |
| `type_category` | mapped: Passenger Vehicle / Commercial / Bicycle / Motorcycle / Unknown |

### Vehicle
| Column | Source |
|---|---|
| `vehicle_id` | `vehicles.unique_id` |
| `collision_id` | `vehicles.collision_id` |
| `vehicle_type_id` | FK via type_code |
| `state_registration` | `vehicles.state_registration` |
| `vehicle_year` | `vehicles.vehicle_year` |

### Person_Type
| Column | Source |
|---|---|
| `person_type_id` | sequential |
| `type_code` | `persons.person_type` (deduplicated, non-null) |
| `type_description` | same as `type_code` |

### Person
| Column | Source |
|---|---|
| `person_id` | `persons.unique_id` |
| `collision_id` | `persons.collision_id` |
| `vehicle_id` | `persons.vehicle_id` (nullable — pedestrians have no vehicle) |
| `person_type_id` | FK via person_type |
| `injury_type` | `persons.person_injury` |
| `age` | `persons.person_age` |
| `sex` | `persons.person_sex` |

### Contributing_Factor
| Column | Source |
|---|---|
| `factor_id` | sequential |
| `factor_code` | deduplicated from `vehicles.contributing_factor_1` + `contributing_factor_2`, excluding "Unspecified" |
| `factor_description` | same as `factor_code` |
| `factor_category` | mapped dict: Driver Error / Vehicle Defect / Environmental / Unknown |

### Vehicle_Factor (junction table)
| Column | Source |
|---|---|
| `vehicle_factor_id` | sequential |
| `vehicle_id` | `vehicles.unique_id` |
| `factor_id` | FK via factor_code |

## Transform Functions

```python
build_borough(crashes) -> DataFrame
build_location(crashes, boroughs) -> DataFrame
build_crash(crashes) -> DataFrame
build_vehicle_type(vehicles) -> DataFrame
build_vehicle(vehicles, vehicle_types) -> DataFrame
build_person_type(persons) -> DataFrame
build_person(persons, person_types) -> DataFrame
build_contributing_factor(vehicles) -> DataFrame
build_vehicle_factor(vehicles, factors) -> DataFrame
```

Each function:
- Takes raw Socrata DataFrames
- Returns a clean entity DataFrame
- Is pure (no I/O, no side effects)
- Tested in `tests/test_transform.py`

## Null / Empty Data Handling

- Rows with null `collision_id` dropped before any transform
- Dimension rows with null/empty code excluded; FK in fact tables becomes NULL
- `vehicle_id` in Person is nullable (pedestrians)
- Numeric fields coerced with `pd.to_numeric(..., errors="coerce")` — invalid strings → NaN → empty CSV cell

## main.py Changes

- Remove `merge_datasets` (replaced by transform pipeline)
- Call each `build_*` function in dependency order
- Call `write_output` once per entity (9 times total, one CSV each)
- `write_output` signature unchanged

## Testing

- `tests/test_transform.py` with unit tests per `build_*` function
- Each test constructs minimal raw DataFrames and asserts on the output shape, column names, and FK values

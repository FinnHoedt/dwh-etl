# Borough Inference Design

**Date:** 2026-03-02
**Goal:** Infer missing borough from lat/lon via precinct spatial join; drop unlocatable crashes from all output files.

---

## Problem

Many crashes have a null `borough` field in the Socrata data. Some of those crashes do have valid lat/lon coordinates, which can be used to infer the borough via the precinct polygon spatial join. Crashes with neither a borough nor lat/lon cannot be located and should be excluded from all output.

## Rules

| borough field | lat/lon | Action |
|---|---|---|
| filled | any | Keep — borough_id from field, precinct_id from spatial join if lat/lon available |
| null | valid | Infer borough from spatial join → keep |
| null | null | Drop crash and all related rows (location, vehicle, person, contributing factor, vehicle factor) |

## Design

### Approach: Filter crashes upstream (Approach A)

A single `filter_locatable_crashes` function enriches and filters crashes before any transforms run. All 8 builders receive the already-filtered DataFrame — no orphan rows possible.

---

### 1. `filter_locatable_crashes(crashes, precincts_gdf) -> pd.DataFrame`

New function in `transform.py`.

1. For crashes where `borough` is null AND lat/lon is valid: run `gpd.sjoin` against `precincts_gdf`, map precinct number → `PRECINCT_BOROUGHS` → borough name, write result back into the `borough` column.
2. Drop all rows where `borough` is still null after step 1.
3. Return the filtered DataFrame.

The spatial join here operates only on the null-borough subset for enrichment. It is separate from the `_assign_precinct_id` join used for precinct_id assignment in `build_location`.

---

### 2. `build_borough` seeding

Add module-level constant:

```python
NYC_BOROUGHS = ["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"]
```

`build_borough` always seeds all 5 boroughs before deduplicating with crash data. This guarantees borough_id exists for any borough inferred by `filter_locatable_crashes`.

---

### 3. `main.py`

After `precincts_gdf = parse_precincts_gdf(precincts_raw)`, add:

```python
crashes = filter_locatable_crashes(crashes, precincts_gdf)
if crashes.empty:
    logger.warning("No locatable crashes — nothing to output.")
    return
```

All builders already receive `crashes`, so the cascade is automatic.

---

## Testing

- `test_filter_locatable_crashes_infers_borough_from_latlon` — null borough + valid lat/lon inside polygon → borough filled in
- `test_filter_locatable_crashes_drops_null_borough_null_latlon` — both null → row dropped
- `test_filter_locatable_crashes_keeps_known_borough_no_latlon` — borough filled, no lat/lon → row kept
- `test_filter_locatable_crashes_empty` — empty input → empty output
- `test_build_borough_always_includes_all_five_nyc_boroughs` — empty crashes → still 5 rows

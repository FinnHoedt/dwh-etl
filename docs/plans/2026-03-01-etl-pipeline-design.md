# ETL Pipeline Design — NYC Crash Data

**Date:** 2026-03-01
**Status:** Approved

## Problem

The current `main.py` is a flat script that fetches NYC crash data from Socrata and joins it with persons and vehicles. It has several weaknesses:

- Hard-coded parameters (limit, dataset IDs)
- No error handling for API failures or empty results
- Uses `print` instead of structured logging
- No output persistence — data is discarded after each run
- Fragile f-string nesting for building the ID filter
- Inconsistent merge suffixes

## Architecture

Single `main.py` with typed functions. All tuneable parameters live in `config.yaml`. Credentials stay in `.env`. Output is written to an `output/` directory.

```
etl/
├── config.yaml
├── main.py
├── .env
└── output/
    ├── crashes_merged.csv
    └── crashes_merged.parquet
```

## config.yaml

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
  formats: [csv, parquet]
```

## Functions

| Function | Responsibility |
|---|---|
| `load_config(path)` | Parse `config.yaml`, return dict |
| `build_client(cfg)` | Construct Socrata client from config + env vars |
| `fetch_dataset(client, dataset_id, **kwargs)` | Fetch → DataFrame; return empty DF on empty result |
| `build_id_filter(ids)` | Produce `collision_id in('a','b',...)` filter string |
| `merge_datasets(crashes, vehicles, persons)` | Left-join with consistent suffixes; handle empty inputs |
| `write_output(df, cfg)` | Write CSV and Parquet to output dir; create dir if missing |
| `main()` | Orchestrate all steps, log summary stats |

## Empty Data Handling

- Crashes empty → log warning, exit early
- Vehicles or persons empty → log warning, proceed with empty DataFrame (left join preserves crash rows)
- Missing display columns handled safely with `reindex`

## Error Handling

- API calls wrapped in try/except for network and auth errors
- `collision_id` presence validated before building filter
- All errors logged with `logging.error(..., exc_info=True)`

## Logging

All output via Python `logging` at `INFO` level. No bare `print` calls.

## Output

- CSV for human inspection
- Parquet for downstream analytics
- Written to `output/` directory (created if missing)
- Filename configured in `config.yaml`

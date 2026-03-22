# Marimo Data Profiling Dashboard — Design

**Date:** 2026-03-22

## Goal

Build an interactive scrollable marimo dashboard (`data-profiling-slides.py`) that displays pre-computed profiling results from `docs/profiling/data_profile_summary.json`. All computation happens in the profiling script; the dashboard only loads and renders.

## Profiling Script Changes (`data_profiling.py`)

Two additions to the per-column JSON output:

1. **Histogram bins** — for columns where `numeric_parse_ratio >= 0.8`, compute a 20-bin histogram using `pd.cut` on the numeric-parsed values. Store as:
   ```json
   "histogram": [{"bin_start": 0.0, "bin_end": 5.0, "count": 12345}]
   ```
2. **Extended top values** — increase from top 5 to top 10 values for categorical bar charts.

Re-run `python data_profiling.py` once to regenerate the JSON before opening the dashboard.

## Dashboard Layout (`data-profiling-slides.py`)

**Framework:** marimo scrollable app (`mo.ui.tabs`, `mo.ui.table`, `mo.ui.dropdown`, altair charts)

### Tabs

- **Overview** — sortable `mo.ui.table` with one row per dataset: name, row count, column count, overall null coverage %
- **Crashes / Persons / Vehicles** — identical structure per tab (see below)

### Per-Dataset Tab Structure

1. **Header** — row count and column count as plain text
2. **Null ratio bar chart** — altair horizontal bar chart, all columns sorted descending by null ratio, color-encoded by null severity
3. **Column stats table** — `mo.ui.table` with columns: column name, dtype, null %, distinct count, numeric parse %, date parse %
4. **Column explorer** — `mo.ui.dropdown` to select a column, then:
   - If column has `histogram` data → altair histogram (bin_start/bin_end/count)
   - Otherwise → altair horizontal bar chart of top 10 values

## Dependencies

- `marimo` (already in pyproject.toml)
- `altair` (add to pyproject.toml)
- `pandas` (already present)

## File Changes

| File | Change |
|---|---|
| `data_profiling.py` | Add histogram bins + extend top_values to 10 |
| `docs/profiling/data_profile_summary.json` | Regenerated output |
| `data-profiling-slides.py` | New marimo app (replaces empty file) |
| `pyproject.toml` | Add `altair` dependency |

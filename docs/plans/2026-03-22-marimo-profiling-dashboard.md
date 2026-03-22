# Marimo Profiling Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an interactive marimo dashboard that loads pre-computed profiling JSON and displays sortable tables + altair charts (null ratios, top values, histograms) in a tabbed scrollable app.

**Architecture:** `data_profiling.py` is enhanced to pre-compute histogram bins and extend top_values to 10; it regenerates `docs/profiling/data_profile_summary.json`. `data-profiling-slides.py` is a marimo app that loads only that JSON — no heavy computation at view time.

**Tech Stack:** marimo, altair, pandas — all managed via `uv`

---

### Task 1: Add altair dependency

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add altair to pyproject.toml**

In the `dependencies` list, add:
```toml
"altair>=5.0",
```

**Step 2: Install it**

```bash
uv add altair
```
Expected: resolves and updates `uv.lock`

**Step 3: Verify import works**

```bash
uv run python -c "import altair; print(altair.__version__)"
```
Expected: prints a version like `5.x.x`

**Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat: add altair dependency for profiling dashboard"
```

---

### Task 2: Extend top_values to 10 in profiling script

**Files:**
- Modify: `data_profiling.py:38-40`
- Test: `tests/test_data_profiling.py` (create)

**Step 1: Write the failing test**

Create `tests/test_data_profiling.py`:

```python
import pandas as pd
import pytest
from data_profiling import top_values, column_profile


def test_top_values_returns_ten_by_default():
    s = pd.Series(list(range(20)))
    result = top_values(s, n=10)
    assert len(result) == 10


def test_column_profile_top_values_has_ten_entries():
    s = pd.Series(list(range(20)))
    profile = column_profile(s)
    assert len(profile["top_values"]) == 10
```

**Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_data_profiling.py -v
```
Expected: `test_column_profile_top_values_has_ten_entries` FAILS (currently returns 5)

**Step 3: Change default n from 5 to 10 in `top_values`**

In `data_profiling.py` line 38, change:
```python
def top_values(series: pd.Series, n: int = 5) -> list[dict[str, object]]:
```
to:
```python
def top_values(series: pd.Series, n: int = 10) -> list[dict[str, object]]:
```

**Step 4: Run tests to confirm pass**

```bash
uv run pytest tests/test_data_profiling.py -v
```
Expected: both tests PASS

**Step 5: Commit**

```bash
git add data_profiling.py tests/test_data_profiling.py
git commit -m "feat: extend top_values default to 10 for richer profiling"
```

---

### Task 3: Add histogram computation to profiling script

**Files:**
- Modify: `data_profiling.py`
- Test: `tests/test_data_profiling.py`

**Step 1: Write the failing test**

Append to `tests/test_data_profiling.py`:

```python
def test_column_profile_includes_histogram_for_numeric_column():
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0] * 100)
    profile = column_profile(s)
    assert "histogram" in profile
    assert isinstance(profile["histogram"], list)
    assert len(profile["histogram"]) > 0
    first_bin = profile["histogram"][0]
    assert "bin_start" in first_bin
    assert "bin_end" in first_bin
    assert "count" in first_bin


def test_column_profile_no_histogram_for_non_numeric_column():
    s = pd.Series(["foo", "bar", "baz"] * 100)
    profile = column_profile(s)
    assert "histogram" not in profile


def test_compute_histogram_bins_returns_correct_structure():
    from data_profiling import compute_histogram_bins
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0] * 20)
    bins = compute_histogram_bins(s, n_bins=5)
    assert len(bins) == 5
    assert all("bin_start" in b and "bin_end" in b and "count" in b for b in bins)
    assert sum(b["count"] for b in bins) == 100
```

**Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_data_profiling.py::test_compute_histogram_bins_returns_correct_structure -v
```
Expected: FAIL — `ImportError: cannot import name 'compute_histogram_bins'`

**Step 3: Add `compute_histogram_bins` function**

Add this function to `data_profiling.py` after the `top_values` function (around line 41):

```python
def compute_histogram_bins(series: pd.Series, n_bins: int = 20) -> list[dict[str, object]]:
    numeric = pd.to_numeric(series.dropna().astype(str), errors="coerce").dropna()
    if numeric.empty:
        return []
    cut, bins = pd.cut(numeric, bins=n_bins, retbins=True, include_lowest=True)
    counts = cut.value_counts(sort=False)
    return [
        {
            "bin_start": round(float(interval.left), 6),
            "bin_end": round(float(interval.right), 6),
            "count": int(count),
        }
        for interval, count in counts.items()
    ]
```

**Step 4: Hook histogram into `column_profile`**

In `column_profile`, after the `format_notes` block is built and before `return profile`, add:

```python
    if profile["format_notes"].get("numeric_parse_ratio", 0.0) >= 0.8:
        profile["histogram"] = compute_histogram_bins(non_null)
```

(This goes right before the final `return profile` at the bottom of `column_profile`.)

**Step 5: Run all profiling tests**

```bash
uv run pytest tests/test_data_profiling.py -v
```
Expected: all tests PASS

**Step 6: Commit**

```bash
git add data_profiling.py tests/test_data_profiling.py
git commit -m "feat: add histogram bins to column profiling output"
```

---

### Task 4: Regenerate profiling JSON

**Files:**
- Output: `docs/profiling/data_profile_summary.json`
- Output: `docs/profiling/data_profile_summary.md`

**Step 1: Run the profiling script**

```bash
uv run python data_profiling.py
```
Expected: completes without error. Will take a few minutes (reads ~2M rows from CSV).

**Step 2: Verify histogram data is present in JSON**

```bash
python -c "
import json
d = json.load(open('docs/profiling/data_profile_summary.json'))
cols = d['datasets']['crashes']['columns']
hist_cols = [c for c, v in cols.items() if 'histogram' in v]
print(f'Columns with histograms: {hist_cols}')
"
```
Expected: prints a list of numeric column names (e.g. `number_of_persons_injured`, etc.)

**Step 3: Commit**

```bash
git add docs/profiling/data_profile_summary.json docs/profiling/data_profile_summary.md
git commit -m "chore: regenerate profiling JSON with histogram bins and extended top values"
```

---

### Task 5: Build marimo app skeleton with tabs

**Files:**
- Modify: `data-profiling-slides.py`

The marimo file format starts with a `import marimo` + `app = marimo.App(...)` declaration, then each cell is a decorated function. Here is the full skeleton to write:

**Step 1: Write the skeleton**

Replace the contents of `data-profiling-slides.py` with:

```python
import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full")


@app.cell
def _imports():
    import json
    from pathlib import Path
    import pandas as pd
    import altair as alt
    import marimo as mo
    return alt, json, mo, pd, Path


@app.cell
def _load_data(json, Path):
    profile_path = Path("docs/profiling/data_profile_summary.json")
    with open(profile_path, encoding="utf-8") as f:
        profile = json.load(f)
    datasets = profile["datasets"]
    return datasets, profile


@app.cell
def _tabs(mo):
    tabs = mo.ui.tabs({
        "Overview": mo.md("_loading..._"),
        "Crashes": mo.md("_loading..._"),
        "Persons": mo.md("_loading..._"),
        "Vehicles": mo.md("_loading..._"),
    })
    tabs
    return (tabs,)
```

**Step 2: Run marimo to verify it opens without errors**

```bash
uv run marimo run data-profiling-slides.py
```
Expected: browser opens, tabs visible, no Python errors in terminal.

**Step 3: Commit**

```bash
git add data-profiling-slides.py
git commit -m "feat: marimo dashboard skeleton with tabs"
```

---

### Task 6: Build Overview tab

**Files:**
- Modify: `data-profiling-slides.py`

**Step 1: Replace the `_tabs` cell with a full implementation**

The overview tab shows a `mo.ui.table` with one row per dataset.

Replace the `_tabs` cell with two cells:

```python
@app.cell
def _overview_tab(datasets, mo, pd):
    rows = []
    for name, data in datasets.items():
        total_cells = data["row_count"] * data["column_count"]
        null_cells = sum(
            col["null_count"] * data["row_count"] / data["row_count"]  # already per-column count
            for col in data["columns"].values()
        )
        # null_count is count of null rows per column, not cells
        total_nulls = sum(col["null_count"] for col in data["columns"].values())
        null_pct = round(total_nulls / total_cells * 100, 2) if total_cells else 0.0
        rows.append({
            "Dataset": name,
            "Rows": data["row_count"],
            "Columns": data["column_count"],
            "Overall Null %": null_pct,
        })
    overview_df = pd.DataFrame(rows)
    overview_table = mo.ui.table(overview_df, selection=None)
    return overview_df, overview_table


@app.cell
def _tabs(mo, overview_table):
    tabs = mo.ui.tabs({
        "Overview": mo.vstack([
            mo.md("## Dataset Overview"),
            overview_table,
        ]),
        "Crashes": mo.md("_coming soon_"),
        "Persons": mo.md("_coming soon_"),
        "Vehicles": mo.md("_coming soon_"),
    })
    tabs
    return (tabs,)
```

**Step 2: Run and verify**

```bash
uv run marimo run data-profiling-slides.py
```
Expected: Overview tab shows a sortable 3-row table.

**Step 3: Commit**

```bash
git add data-profiling-slides.py
git commit -m "feat: add overview tab with dataset summary table"
```

---

### Task 7: Build per-dataset helper functions

**Files:**
- Modify: `data-profiling-slides.py`

These helper functions are used by all three dataset tabs. Add them as a new cell before the `_tabs` cell.

**Step 1: Add a `_helpers` cell**

```python
@app.cell
def _helpers(alt, mo, pd):
    def make_null_chart(data: dict) -> alt.Chart:
        rows = [
            {"column": col, "null_pct": round(meta["null_ratio"] * 100, 2)}
            for col, meta in data["columns"].items()
        ]
        df = pd.DataFrame(rows).sort_values("null_pct", ascending=False)
        return (
            alt.Chart(df)
            .mark_bar()
            .encode(
                x=alt.X("null_pct:Q", title="Null %", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("column:N", sort="-x", title=None),
                color=alt.Color(
                    "null_pct:Q",
                    scale=alt.Scale(scheme="orangered"),
                    legend=None,
                ),
                tooltip=["column:N", "null_pct:Q"],
            )
            .properties(title="Null % per Column", height=max(200, len(rows) * 18))
        )

    def make_stats_table(data: dict, mo) -> mo.ui.table:
        rows = []
        for col, meta in data["columns"].items():
            notes = meta.get("format_notes", {})
            rows.append({
                "Column": col,
                "Dtype": meta["dtype"],
                "Null %": round(meta["null_ratio"] * 100, 2),
                "Distinct": meta["distinct_non_null"],
                "Numeric Parse %": round(notes.get("numeric_parse_ratio", 0.0) * 100, 2),
                "Date Parse %": round(notes.get("date_parse_ratio", 0.0) * 100, 2),
            })
        df = pd.DataFrame(rows)
        return mo.ui.table(df, selection=None)

    def make_column_chart(col_meta: dict, col_name: str) -> alt.Chart:
        if "histogram" in col_meta:
            df = pd.DataFrame(col_meta["histogram"])
            df["bin_label"] = df["bin_start"].round(2).astype(str) + "–" + df["bin_end"].round(2).astype(str)
            return (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X("bin_start:Q", bin=False, title=col_name),
                    x2="bin_end:Q",
                    y=alt.Y("count:Q", title="Count"),
                    tooltip=["bin_start:Q", "bin_end:Q", "count:Q"],
                )
                .properties(title=f"Distribution: {col_name}", width=600, height=300)
            )
        else:
            df = pd.DataFrame(col_meta["top_values"])
            return (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X("count:Q", title="Count"),
                    y=alt.Y("value:N", sort="-x", title=None),
                    tooltip=["value:N", "count:Q"],
                )
                .properties(title=f"Top Values: {col_name}", height=max(150, len(df) * 22))
            )

    return make_column_chart, make_null_chart, make_stats_table
```

**Step 2: Run and verify no errors**

```bash
uv run marimo run data-profiling-slides.py
```
Expected: app still works (helpers defined but not yet used in tabs).

**Step 3: Commit**

```bash
git add data-profiling-slides.py
git commit -m "feat: add chart/table helper functions for per-dataset tabs"
```

---

### Task 8: Build per-dataset tab content

**Files:**
- Modify: `data-profiling-slides.py`

Each dataset tab needs: header, null chart, stats table, column dropdown, column chart. We build one reusable cell factory per dataset using marimo's reactive cell pattern.

**Step 1: Add cells for crashes tab**

Add these cells after `_helpers`:

```python
@app.cell
def _crashes_components(datasets, make_column_chart, make_null_chart, make_stats_table, mo):
    _data = datasets["crashes"]
    _col_names = list(_data["columns"].keys())
    crashes_dropdown = mo.ui.dropdown(options=_col_names, value=_col_names[0], label="Column")
    crashes_null_chart = make_null_chart(_data)
    crashes_stats_table = make_stats_table(_data, mo)
    return crashes_dropdown, crashes_null_chart, crashes_stats_table, _data


@app.cell
def _crashes_column_chart(crashes_dropdown, datasets, make_column_chart):
    _data = datasets["crashes"]
    _selected = crashes_dropdown.value
    crashes_col_chart = make_column_chart(_data["columns"][_selected], _selected)
    return (crashes_col_chart,)
```

**Step 2: Add cells for persons tab**

```python
@app.cell
def _persons_components(datasets, make_column_chart, make_null_chart, make_stats_table, mo):
    _data = datasets["persons"]
    _col_names = list(_data["columns"].keys())
    persons_dropdown = mo.ui.dropdown(options=_col_names, value=_col_names[0], label="Column")
    persons_null_chart = make_null_chart(_data)
    persons_stats_table = make_stats_table(_data, mo)
    return persons_dropdown, persons_null_chart, persons_stats_table, _data


@app.cell
def _persons_column_chart(persons_dropdown, datasets, make_column_chart):
    _data = datasets["persons"]
    _selected = persons_dropdown.value
    persons_col_chart = make_column_chart(_data["columns"][_selected], _selected)
    return (persons_col_chart,)
```

**Step 3: Add cells for vehicles tab**

```python
@app.cell
def _vehicles_components(datasets, make_column_chart, make_null_chart, make_stats_table, mo):
    _data = datasets["vehicles"]
    _col_names = list(_data["columns"].keys())
    vehicles_dropdown = mo.ui.dropdown(options=_col_names, value=_col_names[0], label="Column")
    vehicles_null_chart = make_null_chart(_data)
    vehicles_stats_table = make_stats_table(_data, mo)
    return vehicles_dropdown, vehicles_null_chart, vehicles_stats_table, _data


@app.cell
def _vehicles_column_chart(vehicles_dropdown, datasets, make_column_chart):
    _data = datasets["vehicles"]
    _selected = vehicles_dropdown.value
    vehicles_col_chart = make_column_chart(_data["columns"][_selected], _selected)
    return (vehicles_col_chart,)
```

**Step 4: Update `_tabs` cell to use real content**

Replace the `_tabs` cell:

```python
@app.cell
def _tabs(
    mo,
    overview_table,
    crashes_null_chart, crashes_stats_table, crashes_dropdown, crashes_col_chart,
    persons_null_chart, persons_stats_table, persons_dropdown, persons_col_chart,
    vehicles_null_chart, vehicles_stats_table, vehicles_dropdown, vehicles_col_chart,
    datasets,
):
    def dataset_tab(data, null_chart, stats_table, dropdown, col_chart):
        return mo.vstack([
            mo.md(f"**{data['row_count']:,} rows · {data['column_count']} columns**"),
            mo.md("### Null % per Column"),
            mo.ui.altair_chart(null_chart),
            mo.md("### Column Statistics"),
            stats_table,
            mo.md("### Column Explorer"),
            dropdown,
            mo.ui.altair_chart(col_chart),
        ])

    tabs = mo.ui.tabs({
        "Overview": mo.vstack([
            mo.md("## Dataset Overview"),
            overview_table,
        ]),
        "Crashes": dataset_tab(
            datasets["crashes"],
            crashes_null_chart, crashes_stats_table, crashes_dropdown, crashes_col_chart,
        ),
        "Persons": dataset_tab(
            datasets["persons"],
            persons_null_chart, persons_stats_table, persons_dropdown, persons_col_chart,
        ),
        "Vehicles": dataset_tab(
            datasets["vehicles"],
            vehicles_null_chart, vehicles_stats_table, vehicles_dropdown, vehicles_col_chart,
        ),
    })
    tabs
    return (tabs,)
```

**Step 5: Run and verify full dashboard**

```bash
uv run marimo run data-profiling-slides.py
```
Expected:
- Overview tab: sortable 3-row summary table
- Crashes/Persons/Vehicles tabs: null bar chart, sortable stats table, dropdown + chart (histogram or top-values depending on column)

**Step 6: Commit**

```bash
git add data-profiling-slides.py
git commit -m "feat: complete marimo profiling dashboard with all tabs"
```

---

### Task 9: Final check

**Step 1: Run in edit mode to verify reactivity**

```bash
uv run marimo edit data-profiling-slides.py
```
Expected: switching dropdown updates the column chart reactively without errors.

**Step 2: Run all tests**

```bash
uv run pytest tests/ -v
```
Expected: all tests PASS including new `tests/test_data_profiling.py`.

**Step 3: Final commit if any cleanup needed**

```bash
git add -p
git commit -m "chore: final cleanup for profiling dashboard"
```

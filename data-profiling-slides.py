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
def _overview_tab(datasets, mo, pd):
    rows = []
    for name, data in datasets.items():
        total_cells = data["row_count"] * data["column_count"]
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

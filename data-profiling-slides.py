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

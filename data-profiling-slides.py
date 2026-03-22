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

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

    return Path, alt, json, mo, pd


@app.cell
def _load_data(Path, json):
    profile_path = Path("docs/profiling/data_profile_summary.json")
    with open(profile_path, encoding="utf-8") as f:
        profile = json.load(f)
    datasets = profile["datasets"]
    return (datasets,)


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
    return (overview_table,)


@app.cell
def _helpers(alt, pd):
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
def _crashes_components(datasets, make_null_chart, make_stats_table, mo):
    _data = datasets["crashes"]
    _col_names = list(_data["columns"].keys())
    crashes_dropdown = mo.ui.dropdown(options=_col_names, value=_col_names[0], label="Column")
    crashes_null_chart = make_null_chart(_data)
    crashes_stats_table = make_stats_table(_data, mo)
    return crashes_dropdown, crashes_null_chart, crashes_stats_table


@app.cell
def _crashes_column_chart(crashes_dropdown, datasets, make_column_chart):
    _data = datasets["crashes"]
    _selected = crashes_dropdown.value
    crashes_col_chart = make_column_chart(_data["columns"][_selected], _selected)
    return (crashes_col_chart,)


@app.cell
def _persons_components(datasets, make_null_chart, make_stats_table, mo):
    _data = datasets["persons"]
    _col_names = list(_data["columns"].keys())
    persons_dropdown = mo.ui.dropdown(options=_col_names, value=_col_names[0], label="Column")
    persons_null_chart = make_null_chart(_data)
    persons_stats_table = make_stats_table(_data, mo)
    return persons_dropdown, persons_null_chart, persons_stats_table


@app.cell
def _persons_column_chart(datasets, make_column_chart, persons_dropdown):
    _data = datasets["persons"]
    _selected = persons_dropdown.value
    persons_col_chart = make_column_chart(_data["columns"][_selected], _selected)
    return (persons_col_chart,)


@app.cell
def _vehicles_components(datasets, make_null_chart, make_stats_table, mo):
    _data = datasets["vehicles"]
    _col_names = list(_data["columns"].keys())
    vehicles_dropdown = mo.ui.dropdown(options=_col_names, value=_col_names[0], label="Column")
    vehicles_null_chart = make_null_chart(_data)
    vehicles_stats_table = make_stats_table(_data, mo)
    return vehicles_dropdown, vehicles_null_chart, vehicles_stats_table


@app.cell
def _vehicles_column_chart(datasets, make_column_chart, vehicles_dropdown):
    _data = datasets["vehicles"]
    _selected = vehicles_dropdown.value
    vehicles_col_chart = make_column_chart(_data["columns"][_selected], _selected)
    return (vehicles_col_chart,)


@app.cell
def _tabs(
    crashes_col_chart,
    crashes_dropdown,
    crashes_null_chart,
    crashes_stats_table,
    datasets,
    mo,
    overview_table,
    persons_col_chart,
    persons_dropdown,
    persons_null_chart,
    persons_stats_table,
    vehicles_col_chart,
    vehicles_dropdown,
    vehicles_null_chart,
    vehicles_stats_table,
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
    return


if __name__ == "__main__":
    app.run()

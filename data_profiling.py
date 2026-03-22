import argparse
import json
import re
import warnings
from pathlib import Path

import pandas as pd
import yaml


def load_config(path: str = "config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def canonicalize_column_name(name: object) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def read_input_file(base_dir: Path, filename: str) -> pd.DataFrame:
    path = base_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, low_memory=False)
    elif path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported input format: {path.suffix}")

    canonical = {col: canonicalize_column_name(col) for col in df.columns}
    return df.rename(columns=canonical)


def top_values(series: pd.Series, n: int = 10) -> list[dict[str, object]]:
    counts = series.dropna().astype(str).value_counts().head(n)
    return [{"value": idx, "count": int(val)} for idx, val in counts.items()]


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


def column_profile(series: pd.Series) -> dict[str, object]:
    total = len(series)
    null_count = int(series.isna().sum())
    non_null = series.dropna()

    profile: dict[str, object] = {
        "dtype": str(series.dtype),
        "null_count": null_count,
        "null_ratio": round((null_count / total), 4) if total else 0.0,
        "distinct_non_null": int(non_null.nunique(dropna=True)),
        "top_values": top_values(series),
    }

    if non_null.empty:
        profile["format_notes"] = {
            "all_null": True,
        }
        return profile

    sample = non_null.head(5000)
    as_str = sample.astype(str)
    stripped = as_str.str.strip()
    whitespace_count = int((as_str != stripped).sum())

    lower = stripped.str.lower()
    upper = stripped.str.upper()
    mixed_case_ratio = round(float(((stripped != lower) & (stripped != upper)).mean()), 4)

    numeric_ratio = round(float(pd.to_numeric(stripped, errors="coerce").notna().mean()), 4)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        date_ratio = round(float(pd.to_datetime(stripped, errors="coerce").notna().mean()), 4)

    profile["format_notes"] = {
        "whitespace_trim_candidates": whitespace_count,
        "mixed_case_ratio": mixed_case_ratio,
        "numeric_parse_ratio": numeric_ratio,
        "date_parse_ratio": date_ratio,
    }
    if profile["format_notes"].get("numeric_parse_ratio", 0.0) >= 0.8:
        profile["histogram"] = compute_histogram_bins(non_null)
    return profile


def dataset_profile(df: pd.DataFrame, dataset_name: str) -> dict[str, object]:
    return {
        "dataset": dataset_name,
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": {
            col: column_profile(df[col])
            for col in df.columns
        },
    }


def to_markdown(profile_summary: dict[str, object]) -> str:
    lines: list[str] = [
        "# Data Profiling Summary",
        "",
        "This report is generated from local `data_input` files and is intended to support mapping and null/format handling decisions.",
        "",
    ]
    for name, data in profile_summary["datasets"].items():
        lines.extend([
            f"## {name}",
            "",
            f"- Rows: {data['row_count']}",
            f"- Columns: {data['column_count']}",
            "",
            "| Column | Dtype | Null % | Distinct | Numeric Parse % | Date Parse % |",
            "|---|---:|---:|---:|---:|---:|",
        ])
        for col_name, col_meta in data["columns"].items():
            notes = col_meta.get("format_notes", {})
            lines.append(
                "| {col} | {dtype} | {null_ratio:.2%} | {distinct} | {num:.2%} | {date:.2%} |".format(
                    col=col_name,
                    dtype=col_meta["dtype"],
                    null_ratio=col_meta["null_ratio"],
                    distinct=col_meta["distinct_non_null"],
                    num=notes.get("numeric_parse_ratio", 0.0),
                    date=notes.get("date_parse_ratio", 0.0),
                )
            )
        lines.append("")
    return "\n".join(lines)


def profile_data(config_path: str) -> dict[str, object]:
    cfg = load_config(config_path)
    data_input = cfg.get("data_input", {})
    files = data_input.get("files", {})
    base_dir = Path(data_input.get("directory", "data"))

    datasets: dict[str, dict[str, object]] = {}
    for dataset_name in ("crashes", "persons", "vehicles"):
        filename = files.get(dataset_name)
        if not filename:
            raise ValueError(f"Missing data_input.files.{dataset_name} in config")
        df = read_input_file(base_dir, filename)
        datasets[dataset_name] = dataset_profile(df, dataset_name)

    return {"datasets": datasets}


def write_outputs(summary: dict[str, object], cfg: dict) -> None:
    profiling_output = cfg.get("data_input", {}).get("profiling_output", {})
    markdown_path = Path(profiling_output.get("markdown", "docs/profiling/data_profile_summary.md"))
    json_path = Path(profiling_output.get("json", "docs/profiling/data_profile_summary.json"))

    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    markdown_path.write_text(to_markdown(summary), encoding="utf-8")
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile local ETL input data files")
    parser.add_argument("--config", default="config.yaml", help="Path to configuration YAML")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    summary = profile_data(args.config)
    write_outputs(summary, cfg)


if __name__ == "__main__":
    main()

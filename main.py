import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from sodapy import Socrata

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_id_filter(ids: list[str]) -> str:
    quoted = ", ".join(f"'{i}'" for i in ids)
    return f"collision_id in({quoted})"


def fetch_dataset(client: Socrata, dataset_id: str, **kwargs) -> pd.DataFrame:
    try:
        records = client.get(dataset_id, **kwargs)
    except Exception:
        logger.error("Failed to fetch dataset %s", dataset_id, exc_info=True)
        return pd.DataFrame()

    if not records:
        logger.warning("Dataset %s returned no records", dataset_id)
        return pd.DataFrame()

    return pd.DataFrame.from_records(records)


def merge_datasets(
    crashes: pd.DataFrame,
    vehicles: pd.DataFrame,
    persons: pd.DataFrame,
) -> pd.DataFrame:
    for df in [crashes, vehicles, persons]:
        if not df.empty and "collision_id" in df.columns:
            df["collision_id"] = df["collision_id"].astype(str)

    merged = pd.merge(
        crashes,
        vehicles if not vehicles.empty else pd.DataFrame(columns=["collision_id"]),
        on="collision_id",
        how="left",
        suffixes=("_crash", "_veh"),
    )
    return pd.merge(
        merged,
        persons if not persons.empty else pd.DataFrame(columns=["collision_id"]),
        on="collision_id",
        how="left",
        suffixes=("", "_pers"),
    )


def write_output(df: pd.DataFrame, cfg: dict) -> None:
    out_cfg = cfg["output"]
    directory = Path(out_cfg["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    filename = out_cfg["filename"]

    for fmt in out_cfg.get("formats", []):
        path = directory / f"{filename}.{fmt}"
        if fmt == "csv":
            df.to_csv(path, index=False)
        elif fmt == "parquet":
            df.to_parquet(path, index=False)
        logger.info("Written %s: %s", fmt.upper(), path)

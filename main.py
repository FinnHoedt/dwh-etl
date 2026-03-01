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
            logger.info("Written CSV: %s", path)
        elif fmt == "parquet":
            df.to_parquet(path, index=False)
            logger.info("Written Parquet: %s", path)
        else:
            logger.warning("Unknown output format ignored: %s", fmt)


def build_client(cfg: dict) -> Socrata:
    load_dotenv()
    return Socrata(
        cfg["socrata"]["domain"],
        os.getenv("APP_TOKEN"),
        username=os.getenv("CLIENT_ID"),
        password=os.getenv("CLIENT_SECRET"),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = load_config()
    client = build_client(cfg)

    datasets = cfg["socrata"]["datasets"]
    limit = cfg["socrata"]["limit"]

    crashes = fetch_dataset(client, datasets["crashes"], limit=limit)
    if crashes.empty:
        logger.warning("No crashes fetched — exiting.")
        return

    if "collision_id" not in crashes.columns:
        logger.error("Crashes dataset is missing 'collision_id' column — exiting.")
        return

    id_filter = build_id_filter(crashes["collision_id"].tolist())

    vehicles = fetch_dataset(client, datasets["vehicles"], where=id_filter)
    persons = fetch_dataset(client, datasets["persons"], where=id_filter)

    if vehicles.empty:
        logger.warning("No vehicles found for fetched crashes.")
    if persons.empty:
        logger.warning("No persons found for fetched crashes.")

    final = merge_datasets(crashes, vehicles, persons)

    logger.info("Crashes:     %d", len(crashes))
    logger.info("Vehicles:    %d", len(vehicles))
    logger.info("Persons:     %d", len(persons))
    logger.info("Merged rows: %d", len(final))

    write_output(final, cfg)


if __name__ == "__main__":
    main()

import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from sodapy import Socrata

from transform import (
    build_borough,
    build_contributing_factor,
    build_crash,
    build_location,
    build_person,
    build_person_type,
    build_precinct,
    build_vehicle,
    build_vehicle_factor,
    build_vehicle_type,
    parse_precincts_gdf,
)

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


def write_output(df: pd.DataFrame, cfg: dict, filename: str | None = None) -> None:
    out_cfg = cfg["output"]
    directory = Path(out_cfg["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    name = filename or out_cfg["filename"]

    for fmt in out_cfg.get("formats", []):
        path = directory / f"{name}.{fmt}"
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

    precincts_raw = fetch_dataset(client, datasets["precincts"])
    if precincts_raw.empty:
        logger.warning("No precincts fetched — precinct_id will be NULL in locations.")
    precincts_gdf = parse_precincts_gdf(precincts_raw)

    if vehicles.empty:
        logger.warning("No vehicles found for fetched crashes.")
    if persons.empty:
        logger.warning("No persons found for fetched crashes.")

    boroughs = build_borough(crashes)
    precinct_df = build_precinct(precincts_raw, boroughs)
    locations = build_location(crashes, boroughs, precincts_gdf, precinct_df)
    crash_df = build_crash(crashes)
    vehicle_types = build_vehicle_type(vehicles)
    vehicle_df = build_vehicle(vehicles, vehicle_types)
    person_types = build_person_type(persons)
    person_df = build_person(persons, person_types)
    factors = build_contributing_factor(vehicles)
    vehicle_factors = build_vehicle_factor(vehicles, factors)

    entities: dict[str, pd.DataFrame] = {
        "borough": boroughs,
        "precinct": precinct_df,
        "location": locations,
        "crash": crash_df,
        "vehicle_type": vehicle_types,
        "vehicle": vehicle_df,
        "person_type": person_types,
        "person": person_df,
        "contributing_factor": factors,
        "vehicle_factor": vehicle_factors,
    }

    for name, df in entities.items():
        logger.info("%-25s %d rows", name, len(df))
        write_output(df, cfg, filename=name)


if __name__ == "__main__":
    main()

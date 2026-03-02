import argparse
import io
import logging
import os
from pathlib import Path

import pandas as pd
import requests
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
    filter_locatable_crashes,
    parse_precincts_gdf,
)

logger = logging.getLogger(__name__)
METEOSTAT_DISABLED = False

DEFAULT_BOROUGH_COORDINATES: dict[str, dict[str, float]] = {
    "MANHATTAN": {"latitude": 40.7831, "longitude": -73.9712},
    "BRONX": {"latitude": 40.8448, "longitude": -73.8648},
    "BROOKLYN": {"latitude": 40.6782, "longitude": -73.9442},
    "QUEENS": {"latitude": 40.7282, "longitude": -73.7949},
    "STATEN ISLAND": {"latitude": 40.5795, "longitude": -74.1502},
}

DEFAULT_IEM_STATIONS: dict[str, str] = {
    "MANHATTAN": "KLGA",
    "BRONX": "KLGA",
    "BROOKLYN": "KJFK",
    "QUEENS": "KJFK",
    "STATEN ISLAND": "KEWR",
}


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def configure_logging(cfg: dict) -> None:
    log_cfg = cfg.get("logging", {})
    level_name = str(log_cfg.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_format = "%(asctime)s %(levelname)s %(name)s %(message)s"

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    file_path = log_cfg.get("file")
    if file_path:
        log_file = Path(file_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=log_format, handlers=handlers, force=True)
    if log_cfg.get("silence_meteostat_info", True):
        logging.getLogger("meteostat").setLevel(logging.WARNING)


def build_id_filter(ids: list[str]) -> str:
    quoted = ", ".join(f"'{i}'" for i in ids)
    return f"collision_id in({quoted})"


def build_crashes_date_where(cfg: dict) -> str | None:
    socrata_cfg = cfg.get("socrata", {})
    date_filter = socrata_cfg.get("crashes_date_filter", {})
    if not isinstance(date_filter, dict):
        return None

    start_date = date_filter.get("start_date")
    end_date = date_filter.get("end_date")

    clauses: list[str] = []
    if start_date:
        clauses.append(f"crash_date >= '{start_date}T00:00:00'")
    if end_date:
        clauses.append(f"crash_date <= '{end_date}T23:59:59'")

    if not clauses:
        return None
    return " AND ".join(clauses)


def chunk_ids(ids: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")
    return [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]


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


def fetch_crashes_paginated(
    client: Socrata,
    dataset_id: str,
    page_limit: int,
    where: str | None = None,
    max_pages: int | None = None,
) -> pd.DataFrame:
    if page_limit <= 0:
        raise ValueError("page_limit must be greater than 0")

    frames: list[pd.DataFrame] = []
    offset = 0
    pages_fetched = 0

    while True:
        kwargs: dict[str, object] = {
            "limit": page_limit,
            "offset": offset,
            "order": "crash_date, collision_id",
        }
        if where:
            kwargs["where"] = where

        try:
            records = client.get(dataset_id, **kwargs)
        except Exception:
            logger.error("Failed to fetch crashes page at offset %d", offset, exc_info=True)
            break

        if not records:
            break

        page_df = pd.DataFrame.from_records(records)
        frames.append(page_df)
        pages_fetched += 1
        logger.info("Fetched crashes page %d with %d rows", pages_fetched, len(page_df))

        if len(records) < page_limit:
            break
        if max_pages is not None and pages_fetched >= max_pages:
            logger.warning("Reached max crash pages limit (%d), stopping pagination.", max_pages)
            break

        offset += page_limit

    if not frames:
        return pd.DataFrame()

    crashes = pd.concat(frames, ignore_index=True)
    if "collision_id" in crashes.columns:
        crashes = crashes.drop_duplicates(subset=["collision_id"], keep="first")
    return crashes


def fetch_related_in_batches(
    client: Socrata,
    dataset_id: str,
    collision_ids: list[str],
    batch_size: int,
    limit: int,
) -> pd.DataFrame:
    if not collision_ids:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for batch in chunk_ids(collision_ids, batch_size):
        id_filter = build_id_filter(batch)
        batch_df = fetch_dataset(client, dataset_id, where=id_filter, limit=limit)
        if not batch_df.empty:
            frames.append(batch_df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def build_open_meteo_client():
    import openmeteo_requests
    import requests_cache
    from retry_requests import retry

    cache_session = requests_cache.CachedSession(".cache/open-meteo", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def _weather_date_range(crashes: pd.DataFrame) -> tuple[str, str] | None:
    if crashes.empty or "crash_date" not in crashes.columns:
        return None

    parsed_dates = pd.to_datetime(crashes["crash_date"], errors="coerce").dropna()
    if parsed_dates.empty:
        return None

    start_date = parsed_dates.min().date().isoformat()
    end_date = parsed_dates.max().date().isoformat()
    return start_date, end_date


def fetch_open_meteo_hourly(
    open_meteo_client,
    archive_url: str,
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    timezone: str,
) -> pd.DataFrame:
    responses = open_meteo_client.weather_api(
        archive_url,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": [
                "precipitation",
                "visibility",
                "wind_speed_10m",
                "temperature_2m",
                "snow_depth",
            ],
            "timezone": timezone,
        },
    )
    if not responses:
        return pd.DataFrame()

    response = responses[0]
    hourly = response.Hourly()
    hourly_index = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    ).tz_convert(timezone)

    return pd.DataFrame({
        "observation_date": hourly_index.date.astype(str),
        "observation_hour": hourly_index.hour.astype(int),
        "hourly_precip": hourly.Variables(0).ValuesAsNumpy(),
        "hourly_visibility": hourly.Variables(1).ValuesAsNumpy(),
        "hourly_wind_speed": hourly.Variables(2).ValuesAsNumpy(),
        "hourly_temperature": hourly.Variables(3).ValuesAsNumpy(),
        "snow_depth": hourly.Variables(4).ValuesAsNumpy(),
    })


def fetch_meteostat_visibility_hourly(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    timezone: str,
) -> pd.DataFrame:
    global METEOSTAT_DISABLED
    from meteostat import Point, hourly, stations

    cols = ["observation_date", "observation_hour", "hourly_visibility"]
    if METEOSTAT_DISABLED:
        return pd.DataFrame(columns=cols)

    try:
        station_df = stations.nearby(Point(latitude, longitude), limit=1)
    except Exception:
        logger.error("Meteostat station lookup failed", exc_info=True)
        return pd.DataFrame(columns=cols)
    if station_df.empty:
        return pd.DataFrame(columns=cols)

    station_id = station_df.index[0]
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(hours=23)

    # Meteostat blocks requests longer than 3 years by default.
    # Chunk into <= 3-year windows and concatenate.
    max_span = pd.Timedelta(days=365 * 3)
    chunk_start = start_ts
    parts: list[pd.DataFrame] = []
    while chunk_start <= end_ts:
        chunk_end = min(chunk_start + max_span, end_ts)
        try:
            hourly_series = hourly(station_id, chunk_start, chunk_end, timezone=timezone)
            part = hourly_series.fetch()
        except Exception as exc:
            # Some environments receive persistent HTTP 403 from Meteostat storage.
            # Disable further attempts for this run to avoid repeated slow failures.
            if "403" in str(exc):
                METEOSTAT_DISABLED = True
                logger.warning("Disabling Meteostat visibility fallback for this run due to HTTP 403.")
            else:
                logger.error("Meteostat hourly fetch failed for station %s", station_id, exc_info=True)
            return pd.DataFrame(columns=cols)
        if part is not None and not part.empty:
            parts.append(part)
        chunk_start = chunk_end + pd.Timedelta(hours=1)

    if not parts:
        return pd.DataFrame(columns=cols)
    meteostat_df = pd.concat(parts)
    meteostat_df = meteostat_df[~meteostat_df.index.duplicated(keep="first")]

    if meteostat_df is None or meteostat_df.empty or "visib" not in meteostat_df.columns:
        return pd.DataFrame(columns=cols)

    times = pd.to_datetime(meteostat_df.index, errors="coerce")
    if times.tz is None:
        times = times.tz_localize(timezone)
    else:
        times = times.tz_convert(timezone)

    # Meteostat visibility is in km; convert to meters to match Open-Meteo semantics.
    visibility_m = pd.to_numeric(meteostat_df["visib"], errors="coerce") * 1000.0

    return pd.DataFrame({
        "observation_date": pd.Series(times.date, index=meteostat_df.index).astype(str).values,
        "observation_hour": pd.Series(times.hour, index=meteostat_df.index).astype(int).values,
        "hourly_visibility": visibility_m.values,
    })


def fill_visibility_from_meteostat(
    weather: pd.DataFrame,
    boroughs: pd.DataFrame,
    cfg: dict,
    start_date: str,
    end_date: str,
    visibility_fetcher=fetch_meteostat_visibility_hourly,
) -> pd.DataFrame:
    if weather.empty or boroughs.empty:
        return weather

    weather_cfg = cfg.get("open_meteo", {})
    timezone = weather_cfg.get("timezone", "America/New_York")
    coords = weather_cfg.get("borough_coordinates", DEFAULT_BOROUGH_COORDINATES)

    frames: list[pd.DataFrame] = []
    for row in boroughs.itertuples(index=False):
        coord = coords.get(row.borough_name)
        if coord is None:
            continue
        try:
            visibility = visibility_fetcher(
                latitude=float(coord["latitude"]),
                longitude=float(coord["longitude"]),
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
            )
        except Exception:
            logger.error("Failed to fetch Meteostat visibility for borough %s", row.borough_name, exc_info=True)
            continue

        if visibility.empty:
            continue
        visibility = visibility.copy()
        visibility["borough_id"] = row.borough_id
        frames.append(visibility)

    if not frames:
        return weather

    visibility_df = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["borough_id", "observation_date", "observation_hour"]
    )
    merged = weather.merge(
        visibility_df,
        on=["borough_id", "observation_date", "observation_hour"],
        how="left",
        suffixes=("", "_meteostat"),
    )
    merged["hourly_visibility"] = merged["hourly_visibility"].fillna(
        merged["hourly_visibility_meteostat"]
    )
    return merged.drop(columns=["hourly_visibility_meteostat"])


def fetch_iem_visibility_hourly(
    station: str,
    start_date: str,
    end_date: str,
    timezone: str,
) -> pd.DataFrame:
    cols = ["observation_date", "observation_hour", "hourly_visibility"]
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if end_ts < start_ts:
        return pd.DataFrame(columns=cols)

    frames: list[pd.DataFrame] = []
    for year in range(start_ts.year, end_ts.year + 1):
        year_start = max(start_ts, pd.Timestamp(year=year, month=1, day=1))
        year_end = min(end_ts, pd.Timestamp(year=year, month=12, day=31))
        response = requests.get(
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py",
            params={
                "station": station,
                "data": "vsby",
                "year1": year_start.year,
                "month1": year_start.month,
                "day1": year_start.day,
                "year2": year_end.year,
                "month2": year_end.month,
                "day2": year_end.day,
                "tz": timezone,
                "format": "onlycomma",
                "latlon": "no",
                "elev": "no",
                "missing": "M",
                "trace": "T",
                "direct": "no",
                "report_type": ["1", "2"],
            },
            timeout=30,
        )
        response.raise_for_status()
        part = pd.read_csv(io.StringIO(response.text))
        if not part.empty:
            frames.append(part)

    if not frames:
        return pd.DataFrame(columns=cols)

    raw = pd.concat(frames, ignore_index=True)
    if "valid" not in raw.columns or "vsby" not in raw.columns:
        return pd.DataFrame(columns=cols)

    raw["valid"] = pd.to_datetime(raw["valid"], errors="coerce")
    raw["vsby"] = pd.to_numeric(raw["vsby"], errors="coerce")
    raw = raw.dropna(subset=["valid", "vsby"])
    if raw.empty:
        return pd.DataFrame(columns=cols)

    # IEM ASOS visibility is in statute miles; convert to meters.
    raw["hourly_visibility"] = raw["vsby"] * 1609.344
    raw["observation_date"] = raw["valid"].dt.date.astype(str)
    raw["observation_hour"] = raw["valid"].dt.hour

    hourly = (
        raw.groupby(["observation_date", "observation_hour"], as_index=False)["hourly_visibility"]
        .mean()
    )
    return hourly[cols]


def fill_visibility_from_iem(
    weather: pd.DataFrame,
    boroughs: pd.DataFrame,
    cfg: dict,
    start_date: str,
    end_date: str,
    iem_fetcher=fetch_iem_visibility_hourly,
) -> pd.DataFrame:
    if weather.empty or boroughs.empty:
        return weather

    weather_cfg = cfg.get("open_meteo", {})
    stations_by_borough = weather_cfg.get("visibility_iem_stations", DEFAULT_IEM_STATIONS)

    frames: list[pd.DataFrame] = []
    station_cache: dict[str, pd.DataFrame] = {}
    for row in boroughs.itertuples(index=False):
        station = stations_by_borough.get(row.borough_name)
        if not station:
            continue

        if station in station_cache:
            visibility = station_cache[station]
        else:
            try:
                visibility = iem_fetcher(
                    station=station,
                    start_date=start_date,
                    end_date=end_date,
                    timezone=weather_cfg.get("timezone", "America/New_York"),
                )
            except Exception:
                logger.error("Failed to fetch IEM visibility for borough %s station %s", row.borough_name, station, exc_info=True)
                visibility = pd.DataFrame(columns=["observation_date", "observation_hour", "hourly_visibility"])
            station_cache[station] = visibility

        if visibility.empty:
            continue
        visibility = visibility.copy()
        visibility["borough_id"] = row.borough_id
        frames.append(visibility)

    if not frames:
        return weather

    visibility_df = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["borough_id", "observation_date", "observation_hour"]
    )
    merged = weather.merge(
        visibility_df,
        on=["borough_id", "observation_date", "observation_hour"],
        how="left",
        suffixes=("", "_iem"),
    )
    merged["hourly_visibility"] = merged["hourly_visibility"].fillna(
        merged["hourly_visibility_iem"]
    )
    return merged.drop(columns=["hourly_visibility_iem"])


def build_weather_observation(
    crashes: pd.DataFrame,
    boroughs: pd.DataFrame,
    cfg: dict,
    open_meteo_client=None,
    visibility_fetcher=fetch_meteostat_visibility_hourly,
    iem_visibility_fetcher=fetch_iem_visibility_hourly,
) -> pd.DataFrame:
    cols = [
        "observation_id",
        "borough_id",
        "observation_date",
        "observation_hour",
        "hourly_precip",
        "hourly_visibility",
        "hourly_wind_speed",
        "hourly_temperature",
        "snow_depth",
    ]
    if crashes.empty or boroughs.empty:
        return pd.DataFrame(columns=cols)

    date_range = _weather_date_range(crashes)
    if date_range is None:
        logger.warning("No parseable crash_date values found; weather_observation is empty.")
        return pd.DataFrame(columns=cols)
    start_date, end_date = date_range

    weather_cfg = cfg.get("open_meteo", {})
    archive_url = weather_cfg.get("archive_url", "https://archive-api.open-meteo.com/v1/archive")
    timezone = weather_cfg.get("timezone", "America/New_York")
    coords = weather_cfg.get("borough_coordinates", DEFAULT_BOROUGH_COORDINATES)

    client = open_meteo_client or build_open_meteo_client()
    frames: list[pd.DataFrame] = []

    for row in boroughs.itertuples(index=False):
        coord = coords.get(row.borough_name)
        if coord is None:
            logger.warning("No Open-Meteo coordinates configured for borough %s", row.borough_name)
            continue

        try:
            borough_weather = fetch_open_meteo_hourly(
                client,
                archive_url=archive_url,
                latitude=float(coord["latitude"]),
                longitude=float(coord["longitude"]),
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
            )
        except Exception:
            logger.error("Failed to fetch weather for borough %s", row.borough_name, exc_info=True)
            continue

        if borough_weather.empty:
            continue

        borough_weather["borough_id"] = row.borough_id
        frames.append(borough_weather)

    if not frames:
        return pd.DataFrame(columns=cols)

    weather = pd.concat(frames, ignore_index=True)
    visibility_source = weather_cfg.get("visibility_source", "meteostat")
    if visibility_source == "meteostat":
        weather = fill_visibility_from_meteostat(
            weather,
            boroughs,
            cfg,
            start_date=start_date,
            end_date=end_date,
            visibility_fetcher=visibility_fetcher,
        )
    elif visibility_source == "iem_asos":
        weather = fill_visibility_from_iem(
            weather,
            boroughs,
            cfg,
            start_date=start_date,
            end_date=end_date,
            iem_fetcher=iem_visibility_fetcher,
        )

    numeric_cols = [
        "hourly_precip",
        "hourly_visibility",
        "hourly_wind_speed",
        "hourly_temperature",
        "snow_depth",
    ]
    for col in numeric_cols:
        weather[col] = pd.to_numeric(weather[col], errors="coerce").round(2)

    weather = weather.sort_values(
        by=["borough_id", "observation_date", "observation_hour"]
    ).reset_index(drop=True)
    weather.insert(0, "observation_id", range(1, len(weather) + 1))
    return weather[cols]


def write_output(df: pd.DataFrame, cfg: dict, filename: str | None = None) -> None:
    out_cfg = cfg["output"]
    directory = Path(out_cfg["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    name = filename or out_cfg["filename"]
    csv_prefix = out_cfg.get("csv_prefix", "")

    for fmt in out_cfg.get("formats", []):
        if fmt == "csv":
            path = directory / f"{csv_prefix}{name}.{fmt}"
            df.to_csv(path, index=False)
            logger.info("Written CSV: %s", path)
        elif fmt == "parquet":
            path = directory / f"{name}.{fmt}"
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


def _read_output_entity(cfg: dict, entity_name: str) -> pd.DataFrame:
    out_cfg = cfg["output"]
    directory = Path(out_cfg["directory"])
    preferred_formats = out_cfg.get("formats", ["csv"])

    for fmt in preferred_formats:
        path = directory / f"{entity_name}.{fmt}"
        if not path.exists():
            continue
        if fmt == "csv":
            return pd.read_csv(path)
        if fmt == "parquet":
            return pd.read_parquet(path)
    return pd.DataFrame()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC crash ETL pipeline")
    parser.add_argument(
        "--weather-only",
        action="store_true",
        help="Fetch and write only weather observations using existing output crash/borough files.",
    )
    return parser.parse_args()


def run_weather_only(cfg: dict) -> None:
    crashes = _read_output_entity(cfg, "crash")
    boroughs = _read_output_entity(cfg, "borough")

    if crashes.empty:
        logger.error("weather-only mode requires output/crash.(csv|parquet) with crash_date values.")
        return
    if boroughs.empty:
        logger.error("weather-only mode requires output/borough.(csv|parquet) with borough_id and borough_name.")
        return

    weather_observation = build_weather_observation(crashes, boroughs, cfg)
    logger.info("%-25s %d rows", "weather_observation", len(weather_observation))
    write_output(weather_observation, cfg, filename="weather_observation")


def main() -> None:
    args = parse_args()
    cfg = load_config()
    configure_logging(cfg)

    if args.weather_only:
        run_weather_only(cfg)
        return

    client = build_client(cfg)

    datasets = cfg["socrata"]["datasets"]
    limit = cfg["socrata"]["limit"]
    crashes_page_limit = cfg["socrata"].get("crashes_page_limit", limit)
    crashes_max_pages = cfg["socrata"].get("crashes_max_pages")
    related_batch_size = cfg["socrata"].get("related_batch_size", 300)
    related_limit = cfg["socrata"].get("related_limit", limit)
    crashes_where = build_crashes_date_where(cfg)
    if crashes_where:
        logger.info("Applying crash date filter: %s", crashes_where)

    crashes = fetch_crashes_paginated(
        client,
        datasets["crashes"],
        page_limit=crashes_page_limit,
        where=crashes_where,
        max_pages=crashes_max_pages,
    )
    if crashes.empty:
        logger.warning("No crashes fetched — exiting.")
        return

    if "collision_id" not in crashes.columns:
        logger.error("Crashes dataset is missing 'collision_id' column — exiting.")
        return

    collision_ids = crashes["collision_id"].astype(str).tolist()
    vehicles = fetch_related_in_batches(
        client,
        datasets["vehicles"],
        collision_ids,
        batch_size=related_batch_size,
        limit=related_limit,
    )
    persons = fetch_related_in_batches(
        client,
        datasets["persons"],
        collision_ids,
        batch_size=related_batch_size,
        limit=related_limit,
    )

    precincts_raw = fetch_dataset(client, datasets["precincts"])
    if precincts_raw.empty:
        logger.warning("No precincts fetched — precinct_id will be NULL in locations.")
    precincts_gdf = parse_precincts_gdf(precincts_raw)

    crashes = filter_locatable_crashes(crashes, precincts_gdf)
    if crashes.empty:
        logger.warning("No locatable crashes — nothing to output.")
        return
    valid_ids = set(crashes["collision_id"])
    if not vehicles.empty:
        vehicles = vehicles[vehicles["collision_id"].isin(valid_ids)]
    if not persons.empty:
        persons = persons[persons["collision_id"].isin(valid_ids)]

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
    weather_observation = build_weather_observation(crashes, boroughs, cfg)

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
        "weather_observation": weather_observation,
    }

    for name, df in entities.items():
        logger.info("%-25s %d rows", name, len(df))
        write_output(df, cfg, filename=name)


if __name__ == "__main__":
    main()

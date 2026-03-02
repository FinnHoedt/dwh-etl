import json

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(index=df.index, dtype=object)


def _location_ids(crashes: pd.DataFrame) -> pd.Series:
    if crashes.empty:
        return pd.Series(dtype="Int64")
    # Surrogate key per crash row, stable for the current transformed dataset.
    return pd.Series(range(1, len(crashes) + 1), index=crashes.index, dtype="Int64")


def _normalize_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def _casefold_key(value: object) -> str | None:
    normalized = _normalize_text(value)
    return normalized.casefold() if normalized is not None else None


def _normalize_borough(value: object) -> str | None:
    normalized = _normalize_text(value)
    return normalized.upper() if normalized is not None else None


def _normalize_vehicle_type(value: object) -> str | None:
    normalized = _normalize_text(value)
    return normalized.upper() if normalized is not None else None


def _vehicle_type_key(value: object) -> str | None:
    normalized = _normalize_vehicle_type(value)
    if normalized is None:
        return None
    cleaned = "".join(ch if ch.isalnum() else " " for ch in normalized)
    return " ".join(cleaned.split())


NYC_BOROUGHS: list[str] = ["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"]


def build_borough(crashes: pd.DataFrame) -> pd.DataFrame:
    crash_names: list[str] = []
    if not crashes.empty and "borough" in crashes.columns:
        crash_names = (
            crashes["borough"]
            .map(_normalize_borough)
            .dropna()
            .unique()
            .tolist()
        )

    names = list(dict.fromkeys(NYC_BOROUGHS + crash_names))

    return pd.DataFrame({
        "borough_id": range(1, len(names) + 1),
        "borough_name": names,
        "borough_code": names,
    })


def _assign_precinct_id(
    crashes: pd.DataFrame,
    lat: pd.Series,
    lon: pd.Series,
    precincts_gdf: gpd.GeoDataFrame | None,
    precinct_df: pd.DataFrame | None,
) -> pd.Series:
    empty = pd.Series(index=crashes.index, dtype=object)
    if (
        precincts_gdf is None or precincts_gdf.empty
        or precinct_df is None or precinct_df.empty
    ):
        return empty

    valid_mask = lat.notna() & lon.notna()
    if not valid_mask.any():
        return empty

    points = gpd.GeoDataFrame(
        index=crashes.index[valid_mask],
        geometry=gpd.points_from_xy(lon[valid_mask], lat[valid_mask]),
        crs="EPSG:4326",
    )
    clean_gdf = precincts_gdf[precincts_gdf.geometry.notna()][["precinct", "geometry"]]
    if clean_gdf.empty:
        return empty

    joined = gpd.sjoin(points, clean_gdf, how="left", predicate="intersects")
    joined = joined[~joined.index.duplicated(keep="first")]

    num_to_id = dict(zip(
        precinct_df["precinct_number"].astype(float),
        precinct_df["precinct_id"],
    ))
    return joined["precinct"].map(num_to_id).reindex(crashes.index)


def build_location(
    crashes: pd.DataFrame,
    boroughs: pd.DataFrame,
    precincts_gdf: gpd.GeoDataFrame | None = None,
    precinct_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    cols = ["location_id", "borough_id", "precinct_id", "street_name", "zip_code", "latitude", "longitude"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    borough_lookup = (
        {} if boroughs.empty
        else dict(zip(boroughs["borough_name"], boroughs["borough_id"]))
    )
    borough_normalized = _col(crashes, "borough").map(_normalize_borough)
    lat = pd.to_numeric(_col(crashes, "latitude"), errors="coerce")
    lon = pd.to_numeric(_col(crashes, "longitude"), errors="coerce")
    location_ids = _location_ids(crashes)

    return pd.DataFrame({
        "location_id": location_ids,
        "borough_id": borough_normalized.map(borough_lookup),
        "precinct_id": _assign_precinct_id(crashes, lat, lon, precincts_gdf, precinct_df),
        "street_name": _col(crashes, "on_street_name").fillna(_col(crashes, "cross_street_name")),
        "zip_code": _col(crashes, "zip_code"),
        "latitude": lat,
        "longitude": lon,
    })


def build_crash(crashes: pd.DataFrame) -> pd.DataFrame:
    cols = ["collision_id", "crash_date", "crash_time", "location_id",
            "number_of_persons_injured", "number_of_persons_killed"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    location_ids = _location_ids(crashes)
    crash_dates = pd.to_datetime(_col(crashes, "crash_date"), errors="coerce").dt.date

    return pd.DataFrame({
        "collision_id": crashes["collision_id"],
        "crash_date": crash_dates,
        "crash_time": _col(crashes, "crash_time"),
        "location_id": location_ids,
        "number_of_persons_injured": pd.to_numeric(
            _col(crashes, "number_of_persons_injured"), errors="coerce"
        ),
        "number_of_persons_killed": pd.to_numeric(
            _col(crashes, "number_of_persons_killed"), errors="coerce"
        ),
    })


VEHICLE_TYPE_CATEGORIES: dict[str, str] = {
    "Sedan": "Passenger Vehicle",
    "Station Wagon/Sport Utility Vehicle": "Passenger Vehicle",
    "2-dr sedan": "Passenger Vehicle",
    "4-dr sedan": "Passenger Vehicle",
    "Van": "Passenger Vehicle",
    "Pick-up Truck": "Passenger Vehicle",
    "Convertible": "Passenger Vehicle",
    "Minivan": "Passenger Vehicle",
    "Box Truck": "Commercial",
    "Flat Bed": "Commercial",
    "Tractor Truck Diesel": "Commercial",
    "Tractor Truck Gasoline": "Commercial",
    "Bus": "Commercial",
    "Large Com Veh(6+ Tons)": "Commercial",
    "Small Com Veh(4 Tires)": "Commercial",
    "Tanker": "Commercial",
    "Garbage or Refuse": "Commercial",
    "Dump": "Commercial",
    "Carry All": "Commercial",
    "Bike": "Bicycle",
    "E-Bike": "Bicycle",
    "E-Scooter": "Bicycle",
    "Motorcycle": "Motorcycle",
    "Moped": "Motorcycle",
    "Motorbike": "Motorcycle",
    "Taxi": "Passenger Vehicle",
    "Ambulance": "Commercial",
    "Fire Truck": "Commercial",
    "Tow Truck / Wrecker": "Commercial",
    "Tow Truck": "Commercial",
    "School Bus": "Commercial",
    "Trailer": "Commercial",
    "Semi Trail": "Commercial",
    "Concrete Mixer": "Commercial",
    "Chassis Cab": "Commercial",
    "Box Van": "Commercial",
    "Van/Truck": "Commercial",
    "Beverage Truck": "Commercial",
    "Lunch Wagon": "Commercial",
    "Pedicab": "Bicycle",
    "Scooter": "Motorcycle",
    "Motorscooter": "Motorcycle",
    "Unknown": "Unknown",
}
VEHICLE_TYPE_ALIASES: dict[str, str] = {
    "4 DR SEDAN": "SEDAN",
    "2 DR SEDAN": "SEDAN",
    "STATION WAGON SPORT UTILITY VEHICLE": "STATION WAGON/SPORT UTILITY VEHICLE",
    "PICK UP TRUCK": "PICK-UP TRUCK",
    "PICKUP": "PICK-UP TRUCK",
    "PKUP": "PICK-UP TRUCK",
    "PK": "PICK-UP TRUCK",
    "BOX VAN": "BOX VAN",
    "VAN TRUCK": "VAN/TRUCK",
    "AMBU": "AMBULANCE",
    "FDNY AMBUL": "AMBULANCE",
    "FDNY ENGIN": "FIRE TRUCK",
    "FDNY TRUCK": "FIRE TRUCK",
    "SCHOOL BUS": "SCHOOL BUS",
    "TOW TRUCK WRECKER": "TOW TRUCK / WRECKER",
    "TOW TRUCK": "TOW TRUCK",
    "SEMI TRAIL": "SEMI TRAIL",
    "MOTORSCOOTER": "SCOOTER",
    "UNK": "UNKNOWN",
    "DL": "UNKNOWN",
    "USPCS": "UNKNOWN",
}
VEHICLE_TYPE_CATEGORY_BY_KEY: dict[str, str] = {
    _vehicle_type_key(k): v for k, v in VEHICLE_TYPE_CATEGORIES.items()
}


def _canonical_vehicle_type(value: object) -> str | None:
    normalized = _normalize_vehicle_type(value)
    if normalized is None:
        return None
    key = _vehicle_type_key(normalized)
    alias = VEHICLE_TYPE_ALIASES.get(key)
    return alias if alias is not None else normalized


def build_vehicle_type(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_type_id", "type_code", "type_description", "type_category"]
    if vehicles.empty or "vehicle_type" not in vehicles.columns:
        return pd.DataFrame(columns=cols)

    normalized_types = vehicles["vehicle_type"].map(_canonical_vehicle_type).dropna()
    codes = sorted(set(normalized_types.tolist()))
    if "UNKNOWN" not in codes:
        codes.append("UNKNOWN")
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "vehicle_type_id": range(1, len(codes) + 1),
        "type_code": codes,
        "type_description": codes,
        "type_category": [VEHICLE_TYPE_CATEGORY_BY_KEY.get(_vehicle_type_key(c), "Unknown") for c in codes],
    })


def build_vehicle(vehicles: pd.DataFrame, vehicle_types: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_id", "collision_id", "vehicle_type_id", "state_registration", "vehicle_year"]
    if vehicles.empty:
        return pd.DataFrame(columns=cols)

    type_lookup = (
        {} if vehicle_types.empty
        else dict(zip(vehicle_types["type_code"], vehicle_types["vehicle_type_id"]))
    )
    unknown_type_id = type_lookup.get("UNKNOWN")

    normalized_vehicle_type = _col(vehicles, "vehicle_type").map(_canonical_vehicle_type)
    vehicle_type_id = normalized_vehicle_type.map(type_lookup)
    if unknown_type_id is not None:
        vehicle_type_id = vehicle_type_id.fillna(unknown_type_id)

    return pd.DataFrame({
        "vehicle_id": vehicles["unique_id"],
        "collision_id": vehicles["collision_id"],
        "vehicle_type_id": vehicle_type_id,
        "state_registration": _col(vehicles, "state_registration"),
        "vehicle_year": pd.to_numeric(_col(vehicles, "vehicle_year"), errors="coerce"),
    })


def build_person_type(persons: pd.DataFrame) -> pd.DataFrame:
    cols = ["person_type_id", "type_code", "type_description"]
    if persons.empty or "person_type" not in persons.columns:
        return pd.DataFrame(columns=cols)

    normalized = persons["person_type"].map(_normalize_text).dropna()
    if len(normalized) == 0:
        return pd.DataFrame(columns=cols)
    deduped = (
        pd.DataFrame({"code": normalized})
        .assign(key=lambda df: df["code"].map(_casefold_key))
        .drop_duplicates(subset=["key"], keep="first")
    )
    codes = deduped["code"].values
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "person_type_id": range(1, len(codes) + 1),
        "type_code": codes,
        "type_description": codes,
    })


def build_person(persons: pd.DataFrame, person_types: pd.DataFrame) -> pd.DataFrame:
    cols = ["person_id", "collision_id", "vehicle_id", "person_type_id", "injury_type", "age", "sex"]
    if persons.empty:
        return pd.DataFrame(columns=cols)

    type_lookup = {}
    if not person_types.empty:
        type_lookup = {
            _casefold_key(code): type_id
            for code, type_id in zip(person_types["type_code"], person_types["person_type_id"])
        }

    vehicle_id = _col(persons, "vehicle_id").replace("", pd.NA)
    normalized_person_type_key = _col(persons, "person_type").map(_casefold_key)

    return pd.DataFrame({
        "person_id": persons["unique_id"],
        "collision_id": persons["collision_id"],
        "vehicle_id": vehicle_id,
        "person_type_id": normalized_person_type_key.map(type_lookup),
        "injury_type": _col(persons, "person_injury"),
        "age": pd.to_numeric(_col(persons, "person_age"), errors="coerce"),
        "sex": _col(persons, "person_sex"),
    })


FACTOR_CATEGORIES: dict[str, str] = {
    "Driver Inattention/Distraction": "Driver Error",
    "Fatigued/Drowsy": "Driver Error",
    "Aggressive Driving/Road Rage": "Driver Error",
    "Unsafe Speed": "Driver Error",
    "Following Too Closely": "Driver Error",
    "Failure to Yield Right-of-Way": "Driver Error",
    "Passing or Lane Usage Improper": "Driver Error",
    "Unsafe Lane Changing": "Driver Error",
    "Backing Unsafely": "Driver Error",
    "Passing Too Closely": "Driver Error",
    "Turning Improperly": "Driver Error",
    "Traffic Control Disregarded": "Driver Error",
    "Lost Consciousness": "Driver Error",
    "Fell Asleep": "Driver Error",
    "Alcohol Involvement": "Driver Error",
    "Drugs (illegal)": "Driver Error",
    "Prescription Medication": "Driver Error",
    "Cell Phone (hands-free)": "Driver Error",
    "Cell Phone (hand-held)": "Driver Error",
    "Outside Car Distraction": "Driver Error",
    "Eating or Drinking": "Driver Error",
    "Brakes Defective": "Vehicle Defect",
    "Tires Defective": "Vehicle Defect",
    "Steering Failure": "Vehicle Defect",
    "Accelerator Defective": "Vehicle Defect",
    "Headlights Defective": "Vehicle Defect",
    "Windshield Inadequate": "Vehicle Defect",
    "Other Lighting Defects": "Vehicle Defect",
    "Vehicle Vandalism": "Vehicle Defect",
    "Oversized Vehicle": "Vehicle Defect",
    "Glare": "Environmental",
    "Pavement Slippery": "Environmental",
    "View Obstructed/Limited": "Environmental",
    "Pavement Defective": "Environmental",
    "Lane Marking Improper/Inadequate": "Environmental",
    "Traffic Control Device Improper/Non-Working": "Environmental",
    "Obstruction/Debris in Road": "Environmental",
}
NORMALIZED_FACTOR_CATEGORIES: dict[str, str] = {
    _casefold_key(k): v for k, v in FACTOR_CATEGORIES.items()
}


def build_contributing_factor(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["factor_id", "factor_code", "factor_description", "factor_category"]
    if vehicles.empty:
        return pd.DataFrame(columns=cols)

    factor_col_names = [c for c in ["contributing_factor_1", "contributing_factor_2"] if c in vehicles.columns]
    if not factor_col_names:
        return pd.DataFrame(columns=cols)

    normalized = pd.concat([vehicles[c] for c in factor_col_names]).map(_normalize_text).dropna()
    if len(normalized) == 0:
        return pd.DataFrame(columns=cols)
    deduped = (
        pd.DataFrame({"code": normalized})
        .assign(key=lambda df: df["code"].map(_casefold_key))
        .pipe(lambda df: df[df["key"] != "unspecified"])
        .drop_duplicates(subset=["key"], keep="first")
    )
    codes = deduped["code"].values
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "factor_id": range(1, len(codes) + 1),
        "factor_code": codes,
        "factor_description": codes,
        "factor_category": [NORMALIZED_FACTOR_CATEGORIES.get(_casefold_key(c), "Unknown") for c in codes],
    })


def build_vehicle_factor(vehicles: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_factor_id", "vehicle_id", "factor_id"]
    if vehicles.empty or factors.empty:
        return pd.DataFrame(columns=cols)

    factor_col_names = [c for c in ["contributing_factor_1", "contributing_factor_2"] if c in vehicles.columns]
    if not factor_col_names:
        return pd.DataFrame(columns=cols)

    factor_lookup = {
        _casefold_key(code): factor_id
        for code, factor_id in zip(factors["factor_code"], factors["factor_id"])
    }

    parts = []
    for col_name in factor_col_names:
        part = vehicles[["unique_id", col_name]].copy()
        part.columns = ["vehicle_id", "factor_code"]
        parts.append(part)

    combined = (
        pd.concat(parts, ignore_index=True)
        .assign(factor_code=lambda df: df["factor_code"].map(_normalize_text))
        .dropna(subset=["factor_code"])
        .assign(factor_key=lambda df: df["factor_code"].map(_casefold_key))
        .pipe(lambda df: df[df["factor_key"] != "unspecified"])
        .drop_duplicates(subset=["vehicle_id", "factor_key"])
    )
    combined["factor_id"] = combined["factor_key"].map(factor_lookup)
    combined = combined.dropna(subset=["factor_id"])

    result = combined[["vehicle_id", "factor_id"]].reset_index(drop=True)
    result.insert(0, "vehicle_factor_id", range(1, len(result) + 1))
    return result


PRECINCT_BOROUGHS: dict[int, str] = {
    # Manhattan
    1: "MANHATTAN", 5: "MANHATTAN", 6: "MANHATTAN", 7: "MANHATTAN",
    9: "MANHATTAN", 10: "MANHATTAN", 13: "MANHATTAN", 14: "MANHATTAN",
    17: "MANHATTAN", 18: "MANHATTAN", 19: "MANHATTAN", 20: "MANHATTAN",
    22: "MANHATTAN", 23: "MANHATTAN", 24: "MANHATTAN", 25: "MANHATTAN",
    26: "MANHATTAN", 28: "MANHATTAN", 30: "MANHATTAN", 32: "MANHATTAN",
    33: "MANHATTAN", 34: "MANHATTAN",
    # Bronx
    40: "BRONX", 41: "BRONX", 42: "BRONX", 43: "BRONX", 44: "BRONX",
    45: "BRONX", 46: "BRONX", 47: "BRONX", 48: "BRONX", 49: "BRONX",
    50: "BRONX", 52: "BRONX",
    # Brooklyn
    60: "BROOKLYN", 61: "BROOKLYN", 62: "BROOKLYN", 63: "BROOKLYN",
    66: "BROOKLYN", 67: "BROOKLYN", 68: "BROOKLYN", 69: "BROOKLYN",
    70: "BROOKLYN", 71: "BROOKLYN", 72: "BROOKLYN", 73: "BROOKLYN",
    75: "BROOKLYN", 76: "BROOKLYN", 77: "BROOKLYN", 78: "BROOKLYN",
    79: "BROOKLYN", 81: "BROOKLYN", 83: "BROOKLYN", 84: "BROOKLYN",
    88: "BROOKLYN", 90: "BROOKLYN", 94: "BROOKLYN",
    # Queens
    100: "QUEENS", 101: "QUEENS", 102: "QUEENS", 103: "QUEENS",
    104: "QUEENS", 105: "QUEENS", 106: "QUEENS", 107: "QUEENS",
    108: "QUEENS", 109: "QUEENS", 110: "QUEENS", 111: "QUEENS",
    112: "QUEENS", 113: "QUEENS", 114: "QUEENS", 115: "QUEENS",
    116: "QUEENS",
    # Staten Island
    120: "STATEN ISLAND", 121: "STATEN ISLAND", 122: "STATEN ISLAND",
    123: "STATEN ISLAND",
}


def parse_precincts_gdf(precincts: pd.DataFrame) -> gpd.GeoDataFrame:
    if precincts.empty or "the_geom" not in precincts.columns:
        return gpd.GeoDataFrame(columns=["precinct", "geometry"])

    def _parse(geom) -> object:
        if isinstance(geom, str):
            return shape(json.loads(geom))
        if isinstance(geom, dict):
            return shape(geom)
        return None

    return gpd.GeoDataFrame(
        {"precinct": pd.to_numeric(precincts["precinct"], errors="coerce")},
        geometry=precincts["the_geom"].apply(_parse),
        crs="EPSG:4326",
    )


def build_precinct(precincts: pd.DataFrame, boroughs: pd.DataFrame) -> pd.DataFrame:
    cols = ["precinct_id", "borough_id", "precinct_number", "precinct_name"]
    if precincts.empty or "precinct" not in precincts.columns:
        return pd.DataFrame(columns=cols)

    numbers = pd.to_numeric(precincts["precinct"], errors="coerce").dropna().astype(int)
    if len(numbers) == 0:
        return pd.DataFrame(columns=cols)

    borough_lookup = (
        {} if boroughs.empty
        else dict(zip(boroughs["borough_name"], boroughs["borough_id"]))
    )

    return pd.DataFrame({
        "precinct_id": range(1, len(numbers) + 1),
        "borough_id": [borough_lookup.get(PRECINCT_BOROUGHS.get(n)) for n in numbers],
        "precinct_number": numbers.astype(str).values,
        "precinct_name": [None] * len(numbers),
    })


def filter_locatable_crashes(
    crashes: pd.DataFrame,
    precincts_gdf: gpd.GeoDataFrame | None = None,
) -> pd.DataFrame:
    if crashes.empty:
        return crashes

    borough_col = _col(crashes, "borough").map(_normalize_borough)
    borough_null = borough_col.isna()
    lat = pd.to_numeric(_col(crashes, "latitude"), errors="coerce")
    lon = pd.to_numeric(_col(crashes, "longitude"), errors="coerce")
    has_latlon = lat.notna() & lon.notna()

    infer_mask = borough_null & has_latlon
    if infer_mask.any() and precincts_gdf is not None and not precincts_gdf.empty:
        clean_gdf = precincts_gdf[precincts_gdf.geometry.notna()][["precinct", "geometry"]]
        if not clean_gdf.empty:
            points = gpd.GeoDataFrame(
                index=crashes.index[infer_mask],
                geometry=gpd.points_from_xy(lon[infer_mask], lat[infer_mask]),
                crs="EPSG:4326",
            )
            joined = gpd.sjoin(points, clean_gdf, how="left", predicate="intersects")
            joined = joined[~joined.index.duplicated(keep="first")]
            inferred = joined["precinct"].apply(
                lambda n: PRECINCT_BOROUGHS.get(int(n)) if pd.notna(n) else None
            )
            crashes = crashes.copy()
            crashes.loc[inferred.index, "borough"] = inferred

    crashes = crashes.copy()
    crashes["borough"] = _col(crashes, "borough").map(_normalize_borough)
    borough_col = _col(crashes, "borough")
    locatable = borough_col.notna()
    return crashes[locatable].reset_index(drop=True)

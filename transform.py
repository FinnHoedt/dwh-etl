import json
from datetime import date

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


VEHICLE_TYPE_BUCKETS: dict[str, dict[str, str]] = {
    "PASSENGER_CAR": {"description": "Passenger Car", "category": "Passenger Vehicle"},
    "SUV": {"description": "Sport Utility Vehicle", "category": "Passenger Vehicle"},
    "PICKUP_TRUCK": {"description": "Pickup Truck", "category": "Passenger Vehicle"},
    "VAN_MINIVAN": {"description": "Van or Minivan", "category": "Passenger Vehicle"},
    "TAXI_FOR_HIRE": {"description": "Taxi or For-Hire Vehicle", "category": "Passenger Vehicle"},
    "BUS": {"description": "Bus", "category": "Public Transit"},
    "BOX_TRUCK": {"description": "Box Truck", "category": "Commercial"},
    "COMMERCIAL_TRUCK": {"description": "Commercial Truck", "category": "Commercial"},
    "TRAILER": {"description": "Trailer", "category": "Commercial"},
    "CONSTRUCTION_EQUIPMENT": {"description": "Construction Equipment", "category": "Commercial"},
    "MOTORCYCLE_MOPED": {"description": "Motorcycle or Moped", "category": "Two-Wheeler"},
    "BICYCLE_MICROMOBILITY": {"description": "Bicycle or Micromobility", "category": "Two-Wheeler"},
    "EMERGENCY_VEHICLE": {"description": "Emergency Vehicle", "category": "Emergency"},
    "TOW_SERVICE": {"description": "Tow / Recovery Vehicle", "category": "Service"},
    "UNKNOWN": {"description": "Unknown Vehicle Type", "category": "Unknown"},
}

VEHICLE_TYPE_EXACT_BUCKETS: dict[str, str] = {
    "SEDAN": "PASSENGER_CAR",
    "PASSENGER VEHICLE": "PASSENGER_CAR",
    "4 DR SEDAN": "PASSENGER_CAR",
    "2 DR SEDAN": "PASSENGER_CAR",
    "CONVERTIBLE": "PASSENGER_CAR",
    "SPORT UTILITY STATION WAGON": "SUV",
    "STATION WAGON SPORT UTILITY VEHICLE": "SUV",
    "SUV": "SUV",
    "JEEP": "SUV",
    "PICK UP TRUCK": "PICKUP_TRUCK",
    "PICK UP": "PICKUP_TRUCK",
    "PICKUP": "PICKUP_TRUCK",
    "PKUP": "PICKUP_TRUCK",
    "PK": "PICKUP_TRUCK",
    "VAN": "VAN_MINIVAN",
    "MINIVAN": "VAN_MINIVAN",
    "TAXI": "TAXI_FOR_HIRE",
    "LIVERY VEHICLE": "TAXI_FOR_HIRE",
    "BUS": "BUS",
    "SCHOOL BUS": "BUS",
    "BOX TRUCK": "BOX_TRUCK",
    "BOX VAN": "BOX_TRUCK",
    "TRACTOR TRUCK DIESEL": "COMMERCIAL_TRUCK",
    "TRACTOR TRUCK GASOLINE": "COMMERCIAL_TRUCK",
    "LARGE COM VEH 6 TONS": "COMMERCIAL_TRUCK",
    "SMALL COM VEH 4 TIRES": "COMMERCIAL_TRUCK",
    "TANKER": "COMMERCIAL_TRUCK",
    "GARBAGE OR REFUSE": "COMMERCIAL_TRUCK",
    "DUMP": "COMMERCIAL_TRUCK",
    "FLAT BED": "COMMERCIAL_TRUCK",
    "VAN TRUCK": "COMMERCIAL_TRUCK",
    "CARRY ALL": "COMMERCIAL_TRUCK",
    "BEVERAGE TRUCK": "COMMERCIAL_TRUCK",
    "TRAILER": "TRAILER",
    "SEMI TRAIL": "TRAILER",
    "CONCRETE MIXER": "CONSTRUCTION_EQUIPMENT",
    "CHASSIS CAB": "CONSTRUCTION_EQUIPMENT",
    "MOTORCYCLE": "MOTORCYCLE_MOPED",
    "MOPED": "MOTORCYCLE_MOPED",
    "MOTORBIKE": "MOTORCYCLE_MOPED",
    "SCOOTER": "MOTORCYCLE_MOPED",
    "MOTORSCOOTER": "MOTORCYCLE_MOPED",
    "BIKE": "BICYCLE_MICROMOBILITY",
    "E BIKE": "BICYCLE_MICROMOBILITY",
    "E SCOOTER": "BICYCLE_MICROMOBILITY",
    "PEDICAB": "BICYCLE_MICROMOBILITY",
    "AMBULANCE": "EMERGENCY_VEHICLE",
    "AMBU": "EMERGENCY_VEHICLE",
    "FDNY AMBUL": "EMERGENCY_VEHICLE",
    "FIRE TRUCK": "EMERGENCY_VEHICLE",
    "FDNY ENGIN": "EMERGENCY_VEHICLE",
    "FDNY TRUCK": "EMERGENCY_VEHICLE",
    "POLICE VEHICLE": "EMERGENCY_VEHICLE",
    "TOW TRUCK WRECKER": "TOW_SERVICE",
    "TOW TRUCK": "TOW_SERVICE",
    "TOW TRUCK WRECK": "TOW_SERVICE",
    "UNKNOWN": "UNKNOWN",
    "UNK": "UNKNOWN",
    "USPCS": "UNKNOWN",
    "DL": "UNKNOWN",
}


def _canonical_vehicle_type(value: object) -> str | None:
    normalized = _normalize_vehicle_type(value)
    if normalized is None:
        return None
    return _vehicle_type_key(normalized)


def _vehicle_type_bucket(value: object) -> str:
    key = _canonical_vehicle_type(value)
    if key is None:
        return "UNKNOWN"
    if key in VEHICLE_TYPE_EXACT_BUCKETS:
        return VEHICLE_TYPE_EXACT_BUCKETS[key]

    if "BUS" in key:
        return "BUS"
    if "TAXI" in key or "LIVERY" in key or "CAB" in key:
        return "TAXI_FOR_HIRE"
    if "PICK" in key and "TRUCK" in key:
        return "PICKUP_TRUCK"
    if "SUV" in key or "SPORT UTILITY" in key or "STATION WAGON" in key:
        return "SUV"
    if "SEDAN" in key or "PASSENGER" in key:
        return "PASSENGER_CAR"
    if "VAN" in key or "MINIVAN" in key:
        return "VAN_MINIVAN"
    if "BOX TRUCK" in key:
        return "BOX_TRUCK"
    if any(token in key for token in ["TRUCK", "TRACTOR", "TANKER", "REFUSE", "DUMP", "FLAT BED"]):
        return "COMMERCIAL_TRUCK"
    if "TRAILER" in key or "SEMI" in key:
        return "TRAILER"
    if any(token in key for token in ["MIXER", "CHASSIS"]):
        return "CONSTRUCTION_EQUIPMENT"
    if any(token in key for token in ["MOTORCYCLE", "MOPED", "SCOOTER", "MOTORBIKE"]):
        return "MOTORCYCLE_MOPED"
    if any(token in key for token in ["BIKE", "BICYCLE", "PEDICAB"]):
        return "BICYCLE_MICROMOBILITY"
    if any(token in key for token in ["AMBUL", "FDNY", "FIRE", "POLICE"]):
        return "EMERGENCY_VEHICLE"
    if "TOW" in key or "WRECKER" in key:
        return "TOW_SERVICE"
    return "UNKNOWN"


def build_vehicle_type(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_type_id", "type_code", "type_description", "type_category"]
    if vehicles.empty or "vehicle_type" not in vehicles.columns:
        return pd.DataFrame(columns=cols)

    bucketed = vehicles["vehicle_type"].map(_vehicle_type_bucket)
    codes = sorted(set(bucketed.dropna().tolist()))
    if "UNKNOWN" not in codes:
        codes.append("UNKNOWN")
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "vehicle_type_id": range(1, len(codes) + 1),
        "type_code": codes,
        "type_description": [VEHICLE_TYPE_BUCKETS.get(c, {}).get("description", c) for c in codes],
        "type_category": [VEHICLE_TYPE_BUCKETS.get(c, {}).get("category", "Unknown") for c in codes],
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

    normalized_vehicle_type = _col(vehicles, "vehicle_type").map(_vehicle_type_bucket)
    vehicle_type_id = normalized_vehicle_type.map(type_lookup)
    if unknown_type_id is not None:
        vehicle_type_id = vehicle_type_id.fillna(unknown_type_id)

    vehicle_year = pd.to_numeric(_col(vehicles, "vehicle_year"), errors="coerce")
    current_year = date.today().year
    vehicle_year = vehicle_year.where(vehicle_year <= current_year)

    return pd.DataFrame({
        "vehicle_id": vehicles["unique_id"],
        "collision_id": vehicles["collision_id"],
        "vehicle_type_id": vehicle_type_id,
        "state_registration": _col(vehicles, "state_registration"),
        "vehicle_year": vehicle_year,
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
    "Driverless/Runaway Vehicle": "Driver Error",
    "Reaction to Uninvolved Vehicle": "Driver Error",
    "Failure to Keep Right": "Driver Error",
    "Pedestrian/Bicyclist/Other Pedestrian Error/Confusion": "Pedestrian/Cyclist",
    "Passenger Distraction": "Driver Error",
    "Illnes": "Impairment",
    "Physical Disability": "Impairment",
    "Using On Board Navigation Device": "Driver Error",
    "Other Electronic Device": "Driver Error",
    "Driver Inexperience": "Driver Error",
    "Texting": "Driver Error",
    "Unsafe Backing": "Driver Error",
    "Tinted Windows": "Vehicle Defect",
    "Shoulders Defective/Improper": "Environmental",
    "Animals Action": "Environmental",
    "Windshield Inadequate": "Vehicle Defect",
}
NORMALIZED_FACTOR_CATEGORIES: dict[str, str] = {
    _casefold_key(k): v for k, v in FACTOR_CATEGORIES.items()
}


def _factor_category(value: object) -> str:
    key = _casefold_key(value)
    if key is None:
        return "Unknown"
    mapped = NORMALIZED_FACTOR_CATEGORIES.get(key)
    if mapped is not None:
        return mapped

    if any(token in key for token in ["pedestrian", "bicyclist", "cyclist"]):
        return "Pedestrian/Cyclist"
    if any(token in key for token in ["alcohol", "drug", "medication", "illnes", "fell asleep", "fatigued", "drowsy"]):
        return "Impairment"
    if any(token in key for token in ["brake", "tire", "steering", "accelerator", "headlight", "windshield", "defect", "vandalism"]):
        return "Vehicle Defect"
    if any(token in key for token in ["glare", "pavement", "obstructed", "debris", "lane marking", "traffic control device improper", "weather", "snow", "ice"]):
        return "Environmental"
    if any(token in key for token in ["driver", "yield", "speed", "following", "lane", "backing", "turning", "disregarded", "distraction", "texting", "electronic"]):
        return "Driver Error"
    return "Unknown"


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
        "factor_category": [_factor_category(c) for c in codes],
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

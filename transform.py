import pandas as pd


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(index=df.index, dtype=object)


def build_borough(crashes: pd.DataFrame) -> pd.DataFrame:
    cols = ["borough_id", "borough_name", "borough_code"]
    if crashes.empty or "borough" not in crashes.columns:
        return pd.DataFrame(columns=cols)

    names = (
        crashes["borough"]
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .unique()
    )
    if len(names) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "borough_id": range(1, len(names) + 1),
        "borough_name": names,
        "borough_code": names,
    })


def build_location(crashes: pd.DataFrame, boroughs: pd.DataFrame) -> pd.DataFrame:
    cols = ["location_id", "borough_id", "street_name", "zip_code", "latitude", "longitude"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    borough_lookup = (
        {} if boroughs.empty
        else dict(zip(boroughs["borough_name"], boroughs["borough_id"]))
    )

    street = _col(crashes, "on_street_name").fillna(_col(crashes, "cross_street_name"))

    return pd.DataFrame({
        "location_id": crashes["collision_id"],
        "borough_id": _col(crashes, "borough").map(borough_lookup),
        "street_name": street,
        "zip_code": _col(crashes, "zip_code"),
        "latitude": pd.to_numeric(_col(crashes, "latitude"), errors="coerce"),
        "longitude": pd.to_numeric(_col(crashes, "longitude"), errors="coerce"),
    })


def build_crash(crashes: pd.DataFrame) -> pd.DataFrame:
    cols = ["collision_id", "crash_date", "crash_time", "location_id",
            "number_of_persons_injured", "number_of_persons_killed"]
    if crashes.empty:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "collision_id": crashes["collision_id"],
        "crash_date": _col(crashes, "crash_date"),
        "crash_time": _col(crashes, "crash_time"),
        "location_id": crashes["collision_id"],
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
}


def build_vehicle_type(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_type_id", "type_code", "type_description", "type_category"]
    if vehicles.empty or "vehicle_type_code" not in vehicles.columns:
        return pd.DataFrame(columns=cols)

    codes = (
        vehicles["vehicle_type_code"]
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .unique()
    )
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "vehicle_type_id": range(1, len(codes) + 1),
        "type_code": codes,
        "type_description": codes,
        "type_category": [VEHICLE_TYPE_CATEGORIES.get(c, "Unknown") for c in codes],
    })

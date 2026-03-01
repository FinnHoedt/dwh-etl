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


def build_vehicle(vehicles: pd.DataFrame, vehicle_types: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_id", "collision_id", "vehicle_type_id", "state_registration", "vehicle_year"]
    if vehicles.empty:
        return pd.DataFrame(columns=cols)

    type_lookup = (
        {} if vehicle_types.empty
        else dict(zip(vehicle_types["type_code"], vehicle_types["vehicle_type_id"]))
    )

    return pd.DataFrame({
        "vehicle_id": vehicles["unique_id"],
        "collision_id": vehicles["collision_id"],
        "vehicle_type_id": _col(vehicles, "vehicle_type_code").map(type_lookup),
        "state_registration": _col(vehicles, "state_registration"),
        "vehicle_year": pd.to_numeric(_col(vehicles, "vehicle_year"), errors="coerce"),
    })


def build_person_type(persons: pd.DataFrame) -> pd.DataFrame:
    cols = ["person_type_id", "type_code", "type_description"]
    if persons.empty or "person_type" not in persons.columns:
        return pd.DataFrame(columns=cols)

    codes = (
        persons["person_type"]
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .unique()
    )
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

    type_lookup = (
        {} if person_types.empty
        else dict(zip(person_types["type_code"], person_types["person_type_id"]))
    )

    vehicle_id = _col(persons, "vehicle_id").replace("", pd.NA)

    return pd.DataFrame({
        "person_id": persons["unique_id"],
        "collision_id": persons["collision_id"],
        "vehicle_id": vehicle_id,
        "person_type_id": _col(persons, "person_type").map(type_lookup),
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


def build_contributing_factor(vehicles: pd.DataFrame) -> pd.DataFrame:
    cols = ["factor_id", "factor_code", "factor_description", "factor_category"]
    if vehicles.empty:
        return pd.DataFrame(columns=cols)

    factor_col_names = [c for c in ["contributing_factor_1", "contributing_factor_2"] if c in vehicles.columns]
    if not factor_col_names:
        return pd.DataFrame(columns=cols)

    codes = (
        pd.concat([vehicles[c] for c in factor_col_names])
        .dropna()
        .pipe(lambda s: s[s.str.strip() != ""])
        .pipe(lambda s: s[s.str.lower() != "unspecified"])
        .unique()
    )
    if len(codes) == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "factor_id": range(1, len(codes) + 1),
        "factor_code": codes,
        "factor_description": codes,
        "factor_category": [FACTOR_CATEGORIES.get(c, "Unknown") for c in codes],
    })


def build_vehicle_factor(vehicles: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
    cols = ["vehicle_factor_id", "vehicle_id", "factor_id"]
    if vehicles.empty or factors.empty:
        return pd.DataFrame(columns=cols)

    factor_col_names = [c for c in ["contributing_factor_1", "contributing_factor_2"] if c in vehicles.columns]
    if not factor_col_names:
        return pd.DataFrame(columns=cols)

    factor_lookup = dict(zip(factors["factor_code"], factors["factor_id"]))

    parts = []
    for col_name in factor_col_names:
        part = vehicles[["unique_id", col_name]].copy()
        part.columns = ["vehicle_id", "factor_code"]
        parts.append(part)

    combined = (
        pd.concat(parts, ignore_index=True)
        .dropna(subset=["factor_code"])
        .pipe(lambda df: df[df["factor_code"].str.strip() != ""])
        .pipe(lambda df: df[df["factor_code"].str.lower() != "unspecified"])
        .drop_duplicates(subset=["vehicle_id", "factor_code"])
    )
    combined["factor_id"] = combined["factor_code"].map(factor_lookup)
    combined = combined.dropna(subset=["factor_id"])

    result = combined[["vehicle_id", "factor_id"]].reset_index(drop=True)
    result.insert(0, "vehicle_factor_id", range(1, len(result) + 1))
    return result

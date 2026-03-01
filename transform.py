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

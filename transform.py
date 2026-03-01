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

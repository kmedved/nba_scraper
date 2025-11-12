import pandas as pd


def backfill_coords_with_shotchart(
    df: pd.DataFrame, shotchart_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Backfill x/y/shot_distance for shots missing coordinates using a shot chart
    dataframe containing ['game_id','eventnum','x','y','shot_distance'].
    """
    if df.empty or shotchart_df.empty:
        return df

    key = ["game_id", "eventnum"]
    right = (
        shotchart_df[key + ["x", "y", "shot_distance"]]
        .dropna(subset=["x", "y"], how="any")
        .drop_duplicates(key, keep="last")
    )
    if right.empty:
        return df

    merged = df.merge(right, on=key, how="left", suffixes=("", "_sc"))
    for col in ("x", "y", "shot_distance"):
        sc_col = f"{col}_sc"
        if sc_col not in merged.columns:
            continue
        merged[col] = merged[col].where(merged[col].notna(), merged[sc_col])
        merged.drop(columns=[sc_col], inplace=True)
    return merged

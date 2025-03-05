import os
import logging

import pandas as pd

from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

def get_exact_dst_transitions(year, timezone="Europe/Amsterdam"):
    """Fix timezones. Niet in Repo want specifiek voor WatchE- behalve als generiek"""
    tz = ZoneInfo(timezone)
    start_of_year = datetime(year, 1, 1, tzinfo=tz)
    end_of_year = datetime(year + 1, 1, 1, tzinfo=tz)

    current_time = start_of_year
    transitions = []

    # Check hour by hour for the exact transition
    while current_time < end_of_year:
        next_time = current_time + timedelta(hours=1)
        if current_time.utcoffset() != next_time.utcoffset():
            transition_type = (
                "Start of DST"
                if next_time.utcoffset() > current_time.utcoffset()
                else "End of DST"
            )
            transitions.append(
                {
                    "transition": transition_type,
                    "exact_time": next_time.astimezone(tz),
                    "previous_offset": current_time.utcoffset(),
                    "new_offset": next_time.utcoffset(),
                }
            )

        current_time = next_time

    return transitions

def correct_summer_time_with_timezone_check_watch_e(
    df, datetime_col, timezone="Europe/Amsterdam"
):
    # Check if the datetime column is timezone-aware
    if isinstance(df[datetime_col].dtype, pd.DatetimeTZDtype):
        detected_timezone = df[datetime_col].dt.tz.zone

        # Ensure the timezone matches the one passed to the function
        if detected_timezone != timezone:
            raise ValueError(
                f"Detected timezone '{detected_timezone}' does not match the expected timezone '{timezone}'"
            )

        print(
            f"Timezone '{detected_timezone}' detected. Removing timezone information."
        )
        df[datetime_col] = df[datetime_col].dt.tz_localize(None)

    # Get the unique years in the dataset
    years = pd.to_datetime(df[datetime_col]).dt.year.unique()

    # Adjust for each year's DST transition period
    for year in years:
        transitions = get_exact_dst_transitions(year, timezone)
        start_of_dst = transitions[0]["exact_time"].replace(tzinfo=None)
        # Watch-E for some reason has the time shift an hour earlier than standard:
        end_of_dst = transitions[1]["exact_time"].replace(tzinfo=None) - timedelta(
            hours=1
        )

        # Subtract 1 hour during the DST period
        dst_mask = (df[datetime_col] >= start_of_dst) & (df[datetime_col] < end_of_dst)
        df.loc[dst_mask, datetime_col] = df.loc[dst_mask, datetime_col] - timedelta(
            hours=1
        )

    return df


def list_files_watch_e(folder_path):
    return {f[:-8]: f for f in os.listdir(folder_path) if f.endswith(".parquet")}

def combine_inside_outside_heatpumps(watche_df, watch_e_mapping_dict, model_column_type, huis_code, file_name):
    for column in watch_e_mapping_dict.values():
        if column not in watche_df.columns:
            if column in model_column_type.keys():
                if column == "ElektriciteitsgebruikWarmtepomp":
                    logging.info(
                        f"{huis_code}/{file_name} does not have column {column}. Combining binnen and buiten columns."
                    )
                    binnen = (
                        "ElektriciteitsgebruikWarmtepomp_binnen" in watche_df.columns
                    )
                    buiten = (
                        "ElektriciteitsgebruikWarmtepomp_buiten" in watche_df.columns
                    )
                    if binnen and buiten:
                        logging.info(
                            f"{huis_code}/{file_name} does not have column {column}. Combining binnen and buiten columns."
                        )
                        watche_df["ElektriciteitsgebruikWarmtepomp"] = (
                            watche_df["ElektriciteitsgebruikWarmtepomp_binnen"]
                            + watche_df["ElektriciteitsgebruikWarmtepomp_buiten"]
                        )
                        watche_df.drop(
                            columns=[
                                "ElektriciteitsgebruikWarmtepomp_binnen",
                                "ElektriciteitsgebruikWarmtepomp_buiten",
                            ],
                            inplace=True,
                        )
                    elif buiten:
                        logging.info(
                            f"{huis_code}/{file_name} does not have column {column}. Using buiten column."
                        )
                        watche_df["ElektriciteitsgebruikWarmtepomp"] = watche_df[
                            "ElektriciteitsgebruikWarmtepomp_buiten"
                        ]
                        watche_df.drop(
                            columns=["ElektriciteitsgebruikWarmtepomp_buiten"],
                            inplace=True,
                        )
                    elif binnen:
                        logging.info(
                            f"{huis_code}/{file_name} does not have column {column}. Using binnen column."
                        )
                        watche_df["ElektriciteitsgebruikWarmtepomp"] = watche_df[
                            "ElektriciteitsgebruikWarmtepomp_binnen"
                        ]
                        watche_df.drop(
                            columns=["ElektriciteitsgebruikWarmtepomp_binnen"],
                            inplace=True,
                        )
                    else:
                        logging.error(
                            f"{huis_code}/{file_name} has no heat pump columns."
                        )
                        watche_df[column] = pd.Series(
                            pd.NA,
                            dtype=model_column_type[column],
                            index=watche_df.index,
                        )
                elif model_column_type[column] == "float64":
                    watche_df[column] = pd.Series(
                        pd.NA, dtype=model_column_type[column], index=watche_df.index
                    )
                else:
                    watche_df[column] = pd.Series(
                        pd.NA, dtype=model_column_type[column], index=watche_df.index
                    )

    return watche_df
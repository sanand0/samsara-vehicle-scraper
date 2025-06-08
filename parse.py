# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "tqdm",
# ]
# ///
import argparse
import datetime as dt
import json
import pandas as pd
import tomllib
from collections import defaultdict
from pathlib import Path
from tqdm import tqdm

config = tomllib.load(open("config.toml", "rb"))
stat_types = config["stat_types"]
cache_dir = Path(".cache")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Samsara vehicle data to CSV")
    parser.add_argument(
        "--seconds", type=int, default=60, help="Time interval in seconds"
    )
    args = parser.parse_args()

    files = list(sorted(cache_dir.glob("*.json")))
    dates = [f.stem[:10] for f in files]
    # Using Pandas, create timestamps from min(dates) start of day to max(dates) EOD UTC
    end_date = pd.to_datetime(max(dates)) + pd.Timedelta(days=1)
    timestamps = pd.date_range(
        min(dates), end_date, freq=f"{args.seconds}s"
    ).to_pydatetime()

    data = []
    for file in tqdm(files):
        date_str, stat = file.stem[:10], file.stem[11:].split("-")[0]
        for row in json.loads(file.read_text()):
            vin = row["externalIds"]["samsara.vin"]
            for entry in row[stat]:
                data.append({"vin": vin, "stat": stat, **entry})

    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"], format='ISO8601').dt.floor(f"{args.seconds}s")
    # Replace invalid values like '<NA>' with NaN
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # wide form: 1 row per (time, vin) with one column per stat
    stats = df.pivot_table(index=["time", "vin"], columns="stat", values="value", aggfunc="last")
    stats = stats.reset_index().sort_values(["time", "vin"])
    # guaranteed column order & presence
    stats = stats.reindex(columns=["time", "vin", *stat_types], fill_value=pd.NA)
    stats.to_csv("vehicle_stats.csv", index=False)

# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "tqdm",
# ]
# ///
import argparse
import json
import pandas as pd
import tomllib
from pathlib import Path
from tqdm import tqdm

config = tomllib.load(open("config.toml", "rb"))
stat_types = config["stat_types"]
cache_dir = Path(".cache")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Samsara vehicle data to CSV")
    parser.add_argument("--seconds", type=int, default=60, help="Time interval in seconds")
    args = parser.parse_args()

    files = list(sorted(cache_dir.glob("*.json")))
    dates = [f.stem[:10] for f in files]
    # Using Pandas, create timestamps from min(dates) start of day to max(dates) EOD UTC
    end_date = pd.to_datetime(max(dates)) + pd.Timedelta(days=1)
    timestamps = pd.date_range(min(dates), end_date, freq=f"{args.seconds}s").to_pydatetime()

    # Group files by stat type. Processing stat-by-stat reduces memory usage
    files_by_stat = {}
    for file in files:
        stat = file.stem[11:].split("-")[0]
        if stat not in files_by_stat:
            files_by_stat[stat] = []
        files_by_stat[stat].append(file)

    stat_dfs = []
    for stat in tqdm(stat_types, desc="Processing stats"):
        if stat not in files_by_stat:
            continue

        data = []
        for file in tqdm(files_by_stat[stat], desc=f"Files for {stat}", leave=False):
            for row in json.loads(file.read_text()):
                vin = row["externalIds"]["samsara.vin"]
                for entry in row[stat]:
                    if "value" in entry:
                        data.append({"vin": vin, "time": entry["time"], "value": entry["value"]})

        if not data:
            continue
        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["time"], format="ISO8601").dt.floor(f"{args.seconds}s")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        pivot = df.pivot_table(index=["time", "vin"], values="value", aggfunc="last").reset_index()
        stat_dfs.append(pivot.rename(columns={"value": stat}))

    # Merge all stat dataframes
    stats = stat_dfs[0]
    for df in stat_dfs[1:]:
        stats = stats.merge(df, on=["time", "vin"], how="outer")

    # Sort by time then VIN. Add _all_ columns in stat_types (NA if missing) and save
    stats = stats.sort_values(["time", "vin"])
    stats = stats.reindex(columns=["time", "vin", *stat_types], fill_value=pd.NA)
    stats.to_csv("vehicle_stats.csv", index=False)

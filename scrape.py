# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "httpx",
#     "rich",
# ]
# ///
import argparse
import datetime as dt
import httpx
import json
import os
import tomllib
from pathlib import Path
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)

def fetch_stat_data(client, stat_types, date_str, start_time, end_time):
    params_common = {
        "startTime": start_time.isoformat(timespec="seconds"),
        "endTime": end_time.isoformat(timespec="seconds"),
    }

    for i, stat_type in enumerate(stat_types, 1):
        cache_file = cache_dir / f"{date_str}-{stat_type}.json"
        if cache_file.exists():
            continue

        all_data = []
        params = params_common | {"types": stat_type}
        page_count = 0

        with Progress(
            SpinnerColumn(),
            TextColumn(f"{date_str} ({i}/{len(stat_types)}) [bold blue]{stat_type}[/bold blue]"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("pages: {task.completed}"),
            transient=True,
        ) as progress:
            task = progress.add_task("", total=None)

            while True:
                r = client.get("/fleet/vehicles/stats/history", params=params)
                if not r.is_success:
                    raise Exception(r.text)
                payload = r.json()
                all_data.extend(payload["data"])
                page_count += 1
                progress.update(task, completed=page_count)

                if not payload["pagination"]["hasNextPage"]:
                    break
                params["after"] = payload["pagination"]["endCursor"]

        cache_file.write_text(json.dumps(all_data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Samsara vehicle historical stats")
    yesterday = (dt.datetime.now(dt.UTC) - dt.timedelta(days=1)).date().isoformat()
    parser.add_argument("--date", default=yesterday, help="YYYY-MM-DD (default: yesterday UTC)")
    parser.add_argument("--ndays", type=int, default=1, help="# of days to fetch, backwards")
    args = parser.parse_args()

    config = tomllib.load(open("config.toml", "rb"))
    stat_types = config["stat_types"]

    cache_dir = Path(".cache")
    cache_dir.mkdir(exist_ok=True)

    headers = {"Authorization": f"Bearer {os.getenv('SAMSARA_TOKEN')}"}
    with httpx.Client(base_url="https://api.samsara.com", headers=headers, timeout=60) as client:
        start_date = dt.datetime.fromisoformat(args.date).replace(tzinfo=dt.UTC)

        for day_offset in range(args.ndays):
            current_date = start_date - dt.timedelta(days=day_offset)
            date_str = current_date.date().isoformat()
            start_time = current_date
            end_time = current_date + dt.timedelta(days=1)

            fetch_stat_data(client, stat_types, date_str, start_time, end_time)

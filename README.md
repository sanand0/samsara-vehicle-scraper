# Samsara Vehicle Stats Scraper

Fetch **historical telematics data** for every vehicle in your Samsara fleet, cache the raw JSON, and convert it into analytics-ready CSV, all from the command line.

## Why this project exists

Samsara exposes rich vehicle telemetry through the _Vehicle Stats APIs_.
While the `/fleet/vehicles/stats/history` endpoint can deliver every odometer reading, GPS location, or engine-runtime value you need, calling it efficiently (respecting pagination, rate limits, and time windows) still takes work.

This repo automates that work:

- [`scrape.py`](scrape.py)
  _Downloads_ JSON for any set of stat types and date ranges, handling cursor-based pagination until `hasNextPage` is false.
  Results are cached under `.cache/` as `<YYYY-MM-DD>-<stat>.json`.

- [`parse.py`](parse.py)
  Reads the cached JSON, resamples it to a uniform interval (default 60 s), pivots to wide format (`1 row per timestamp * VIN`), guarantees a fixed column order, and writes `vehicle_stats.csv`.

## Usage

Set up by cloning the repository and defining environment variables:

```bash
git clone https://github.com/sanand0/samsara-vehicle-scraper.git
cd samsara-vehicle-scraper
export SAMSARA_TOKEN="..."
```

Optionally, update [`config.toml`](config.toml) to include the stat types you want to fetch:

```toml
stat_types = [
  "gpsOdometerMeters",
  "obdOdometerMeters",
  "gpsDistanceMeters",
  "engineSeconds"
]
```

Scrape and generate CSV using [`uv`](https://github.com/astral-sh/uv):

```bash
# clear cache
rm -rf .cache/
# fetch yesterday only
uv run scrape.py
# or fetch the last 30 days
uv run scrape.py --date 2025-06-08 --ndays 30

# Generate the CSV
uv run parse.py --seconds 60
```

This creates `vehicle_stats.csv` with these columns:

- `time`: datetime. Timestamp (UTC)
- `vin`: string. Vehicle Identification Number
- `<stat>`: float. One column per entry in `stat_types`

Missing or non-numeric values are written as empty cells (NaN).

## Notes

How it works:

1. **Day-by-day pagination**. For each date, the script requests 24-hour slices (`startTime` > `endTime`) to reduce payload size.
2. **Cursor loop**. It re-queries `/fleet/vehicles/stats/history` with the returned `endCursor` until `hasNextPage` is `false`. ([developers.samsara.com][2])
3. **Local cache**. Each date-stat pair is saved once; re-runs skip existing files.
4. **Wide pivot**. `parse.py` pivots the long JSON into an analytics-friendly table.

Suggestions:

- **Stat granularity**: History API stores data at its native sampling rate; use `--seconds` to down-sample evenly.
- **Feed vs. History**: For real-time streaming, Samsara suggests the `/fleet/vehicles/stats/feed` endpoint; this project targets _backfills_ and _bulk exports_.
- **Data gaps**: Cellular dead zones can create missing rows; always handle NaNs downstream.

## License

[MIT](LICENSE)

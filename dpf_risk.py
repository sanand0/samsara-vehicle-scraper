#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "rich",
# ]
# ///

"""
Stream large telemetry CSVs and rank VINs by DPF-clog risk.

uv run dpi_risk.py telemetry.csv.xz
uv run dpi_risk.py telemetry.csv.xz --start 2024-01-01 --end 2024-06-30
uv run dpi_risk.py telemetry.csv.xz --chunk 150000 --target june_risk.csv
"""

from __future__ import annotations
import sys
import argparse
import pandas as pd
from collections import defaultdict
from datetime import datetime as dt, date
from rich.console import Console
from rich.table import Table
from rich.live import Live

# --------------------------------------------------------------------
# Thresholds driving the DPF-risk formula
COOLANT_COLD_C = 60_000  # milli-°C
LOAD_LOW_PCT = 40  # %
SPEED_IDLE = 1  # mph
SPEED_LOW = 20  # mph
HIGHWAY_MPH = 60  # normalisation for speed penalty
# --------------------------------------------------------------------
console = Console()


# --------------------------- CLI ------------------------------------
def cli(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="dpi_risk.py", description="Stream a large CSV and rank DPF-clog risk per VIN."
    )
    p.add_argument("csv", help="Path to telemetry CSV (.csv or .csv.xz)")
    p.add_argument("--start", type=str, metavar="YYYY-MM-DD", help="Start date (inclusive)")
    p.add_argument("--end", type=str, metavar="YYYY-MM-DD", help="End date (inclusive)")
    p.add_argument("--chunk", type=int, default=300_000, help="Rows per chunk (default 300 000)")
    p.add_argument("--target", type=str, help="Output CSV (default dpi_risk-{start}-{end}.csv)")
    return p.parse_args(argv)


def parse_date(d: str | None) -> date | None:
    return dt.strptime(d, "%Y-%m-%d").date() if d else None


# ------------------------ risk helpers ------------------------------
def update_store(store: dict[str, list[int]], frame: pd.DataFrame) -> None:
    rpm, spd = frame["engineRpm"], frame["ecuSpeedMph"]
    load, cool = frame["engineLoadPercent"], frame["engineCoolantTemperatureMilliC"]

    frame["idle"] = ((rpm > 600) & (rpm < 1000) & (spd < SPEED_IDLE)).astype("uint8")
    frame["low_speed"] = (spd < SPEED_LOW).astype("uint8")
    frame["low_load"] = (load < LOAD_LOW_PCT).astype("uint8")
    frame["cold_engine"] = (cool < COOLANT_COLD_C).astype("uint8")
    frame["spd_sum"] = spd.fillna(0)

    agg = frame.groupby("vin").agg(
        total=("vin", "size"),
        idle=("idle", "sum"),
        low_spd=("low_speed", "sum"),
        low_load=("low_load", "sum"),
        cold=("cold_engine", "sum"),
        spd_sum=("spd_sum", "sum"),
    )
    for vin, row in agg.iterrows():
        slot = store.setdefault(vin, [0] * 6)
        for i, k in enumerate(("total", "idle", "low_spd", "low_load", "cold", "spd_sum")):
            slot[i] += row[k]


def store_to_df(store: dict[str, list[int]]) -> pd.DataFrame:
    rows = []
    for vin, (tot, idl, ls, ll, ce, ss) in store.items():
        if not tot:
            continue
        idle_r, low_s_r, low_l_r, cold_r = idl / tot, ls / tot, ll / tot, ce / tot
        avg_spd = ss / tot
        spd_pen = max(0, 1 - avg_spd / HIGHWAY_MPH)
        risk = 0.2 * (idle_r + low_s_r + low_l_r + cold_r + spd_pen)
        row = dict(
            VIN=vin,
            Risk=risk,
            IdleP=idle_r * 100,
            LowSpeedP=low_s_r * 100,
            LowLoadP=low_l_r * 100,
            ColdP=cold_r * 100,
            AvgKph=avg_spd * 1.60934,
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values("Risk", ascending=False).reset_index(drop=True)


def render_table(df: pd.DataFrame, id: int, *, top_n: int = 5, title: str | None = None) -> Table:
    tbl = Table(title=title or f"Chunk {id}: Top-{top_n} DPF Risk", expand=True)
    for col in ("Rank", "VIN", "Risk", "Idle %", "Low-Spd %", "Low-Load %", "Cold %", "Avg kph"):
        tbl.add_column(col, justify="right" if col != "VIN" else "left")
    for rank, row in enumerate(df.head(top_n).itertuples(index=False), 1):
        tbl.add_row(
            str(rank),
            row.VIN,
            f"{row.Risk:.4f}",
            f"{row.IdleP:.2f}",
            f"{row.LowSpeedP:.2f}",
            f"{row.LowLoadP:.2f}",
            f"{row.ColdP:.2f}",
            f"{row.AvgKph:.1f}",
        )
    return tbl


# --------------------------- main -----------------------------------
def main(argv: list[str] | None = None) -> None:
    args = cli(argv or sys.argv[1:])
    start_d, end_d = parse_date(args.start), parse_date(args.end)

    # Build default output filename if missing
    if args.target:
        target_csv = args.target
    else:
        s = args.start or "begin"
        e = args.end or "end"
        target_csv = f"dpi_risk-{s}-{e}.csv"

    store = defaultdict(list)
    with Live(console=console, screen=True, refresh_per_second=4) as live:
        for cidx, chunk in enumerate(
            pd.read_csv(args.csv, compression="infer", chunksize=args.chunk), 1
        ):
            # optional date filter
            if start_d or end_d:
                ts = pd.to_datetime(chunk["time"], utc=True)
                if start_d:
                    chunk = chunk[ts.dt.date >= start_d]
                if end_d:
                    chunk = chunk[ts.dt.date <= end_d]
                if chunk.empty:
                    continue

            update_store(store, chunk)
            df_now = store_to_df(store)
            df_now.to_csv(target_csv, index=False)
            live.update(render_table(df_now, cidx, top_n=5))

    console.rule("[bold green]Final Top-10[/bold green]")
    console.print(render_table(df_now, cidx, top_n=10, title="Final Top-10 DPF-Risk"))


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()

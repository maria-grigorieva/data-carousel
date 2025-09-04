from pathlib import Path
from datetime import date
from dateutil import rrule

import pandas as pd
import cx_Oracle
from configparser import ConfigParser

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
config = ConfigParser()
config.read("config.ini")

conn_str = config["database"]["conn_str"]

QUERY_FILE = "queries/prodrequests.sql"

OUTPUT_FILE = "data/request_datasets_staged.csv"

PRODSYS_CSV = "data/prodsyslogs_2022_august2025.csv"

START_DATE = date(2022, 1, 1)
END_DATE = date(2025, 5, 20)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def load_sql() -> str:
    """Load SQL query from external file."""
    return QUERY_FILE.read_text()


def assign_periods(df: pd.DataFrame, time_col: str = "task_submit_time", max_gap_days: int = 90) -> pd.DataFrame:
    """
    Assign period numbers per pr_id based on gaps in time_col.
    """
    df = df.sort_values(["pr_id", time_col])
    df[time_col] = pd.to_datetime(df[time_col])

    def detect_periods(subdf: pd.DataFrame) -> pd.DataFrame:
        subdf = subdf.sort_values(time_col).copy()
        diffs = subdf[time_col].diff().dt.days.fillna(0)
        subdf["period"] = (diffs > max_gap_days).cumsum() + 1
        return subdf

    return df.groupby("pr_id").apply(detect_periods).reset_index(drop=True)


def fetch_for_date(conn: cx_Oracle.Connection, sql: str, dt: date, staged_datasets: set[str]) -> pd.DataFrame:
    """
    Fetch data for a given month and filter against staged_datasets.
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"dt": dt})
        cols = [col[0].lower() for col in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)

    return df[df["dataset"].isin(staged_datasets)]


# -----------------------------------------------------------------------------
# Main script
# -----------------------------------------------------------------------------
def main() -> None:
    sql = load_sql()

    # Load enriched data
    prodsys_df = pd.read_csv(PRODSYS_CSV)
    prodsys_df["asctime"] = pd.to_datetime(prodsys_df["asctime"])
    prodsys_df["dataset"] = prodsys_df["dataset"].astype(str)
    staged_datasets = set(prodsys_df["dataset"].unique())
    print(f"Loaded {len(staged_datasets)} enriched dataset names")

    # Connect to Oracle
    with cx_Oracle.connect(conn_str) as conn:
        all_dfs = []
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=START_DATE, until=END_DATE):
            md = dt.date()
            print(f"Fetching {md:%Y-%m}...", end=" ")
            df = fetch_for_date(conn, sql, md, staged_datasets)
            print(f"{len(df)} rows filtered")
            all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined rows: {len(combined)}")

    # Join with enriched to get asctime
    enriched_combined = combined.merge(prodsys_df, on="dataset", how="inner")

    # Request-level start and end times
    pr_times = (
        enriched_combined.groupby("pr_id")
        .agg(
            request_start=("task_submit_time", "min"),
            request_end=("task_end_time", "max"),
        )
        .reset_index()
    )

    # Merge back to get time window
    final_df = enriched_combined.merge(pr_times, on="pr_id")

    # Keep only rows where asctime is within the request time window
    filtered_df = final_df[
        (final_df["asctime"] >= final_df["task_submit_time"])
        & (final_df["asctime"] <= final_df["task_end_time"])
    ]

    print(f"Filtered rows: {len(filtered_df)}")
    filtered_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Results written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

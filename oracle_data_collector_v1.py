import pandas as pd
import cx_Oracle
from datetime import date
from dateutil import rrule
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

conn_str = config["database"]["conn_str"]

# conn_str = "atlas_datapopularity_r/dpp_ADCmon21@itrac54104-v.cern.ch:10121/adcr_pandamon.cern.ch"

# SQL query and parameters
sql = """
SELECT
  r.pr_id,
  r.description,
  r.provenance,
  r.request_type,
  r.campaign,
  r.phys_group,
  r.project,
  r.energy_gev,
  t.project || ':' || t.inputdataset AS dataset,
  REGEXP_SUBSTR(t.inputdataset, '[^\\.]+', 1, 5) AS input_format,
  t.output_formats,
  t.submit_time           AS task_submit_time,
  t.timestamp             AS task_timestamp,
  NVL(t.endtime, t.timestamp) AS task_end_time,
  t.simulation_type,
  t.total_events
FROM ATLAS_DEFT.T_PRODMANAGER_REQUEST r
INNER JOIN ATLAS_DEFT.T_PRODUCTION_TASK t
  ON t.pr_id = r.pr_id
WHERE
  t.prodsourcelabel = 'managed'
  AND TRUNC(t.submit_time, 'MM') = :dt
  AND (
    t.inputdataset LIKE 'mc%' 
    OR t.inputdataset LIKE 'data%'
  ) 
  AND t.inputdataset not LIKE '%.DAOD_%' 
  AND r.request_type in ('MC', 'REPROCESSING', 'GROUP', 'HLT', 'TIER0')
"""

start_date = date(2022, 1, 1)
end_date = date(2025, 5, 20)

output_file = "request_datasets_staged_v3.csv"
prodsys_csv = "enriched_results_v1.csv"

def assign_periods(df, time_col='task_submit_time', max_gap_days=90):
    """
    Given a DataFrame with 'pr_id' and 'task_submit_time', assign period numbers.
    """
    df = df.sort_values(['pr_id', time_col])
    df[time_col] = pd.to_datetime(df[time_col])  # ensure datetime

    def detect_periods(subdf):
        subdf = subdf.sort_values(time_col).copy()
        diffs = subdf[time_col].diff().dt.days.fillna(0)
        subdf['period'] = (diffs > max_gap_days).cumsum() + 1
        return subdf

    df = df.groupby('pr_id').apply(detect_periods).reset_index(drop=True)
    return df

def fetch_for_date(conn, dt, staged_datasets):
    cur = conn.cursor()
    cur.execute(sql, { "dt": dt })
    cols = [col[0].lower() for col in cur.description]
    data = cur.fetchall()
    cur.close()
    df = pd.DataFrame(data, columns=cols)
    return df[df['dataset'].isin(staged_datasets)]

def main():
    # Load enriched data
    prodsys_df = pd.read_csv(prodsys_csv)
    prodsys_df['asctime'] = pd.to_datetime(prodsys_df['asctime'])
    prodsys_df['dataset'] = prodsys_df['dataset'].astype(str)
    staged_datasets = set(prodsys_df['dataset'].unique())
    print(f"{len(staged_datasets)} enriched dataset names loaded")

    # Connect to Oracle
    conn = cx_Oracle.connect(conn_str)

    all_dfs = []
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        md = dt.date()
        print(f"Fetching {md:%Y-%m}…", end=" ")
        df = fetch_for_date(conn, md, staged_datasets)
        print(f"{len(df)} rows filtered")
        all_dfs.append(df)

    conn.close()
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined rows: {len(combined)}")

    # Join with enriched to get asctime
    enriched_combined = combined.merge(
        prodsys_df,
        on='dataset',
        how='inner'
    )

    # Join with period bounds
    pr_times = (
        enriched_combined
        .groupby('pr_id')
        .agg(
            request_start=('task_submit_time', 'min'),
            request_end=('task_end_time', 'max')
        )
        .reset_index()
    )

    # Merge back to get pr_id's time window for each row
    final_df = enriched_combined.merge(pr_times, on='pr_id')

    # Keep only rows where asctime is within the request time window
    filtered_df = final_df[
        (final_df['asctime'] >= final_df['task_submit_time']) &
        (final_df['asctime'] <= final_df['task_end_time'])
    ]

    print(f"Filtered rows: {len(filtered_df)}")
    filtered_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()

import cx_Oracle
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil import rrule
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

conn_str = config["database"]["conn_str"]

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


# 3) Date range over which to run it
start_date = date(2022, 1, 1)
end_date   = date(2025, 5, 20)

# 4) Output directory / file pattern
#    e.g. each day → write 'data_2025-01-01.csv'
output_file = "request_datasets.csv"
# output_file_filtered = "filtered_all_data.csv"

# 5) Enriched dataset list file
enriched_csv = "enriched_results_v1.csv"
enriched_col = "dataset"

# ─── SCRIPT ───────────────────────────────────────────────────────────────────

def fetch_for_date(conn, dt, staged_datasets):
    cur = conn.cursor()
    # pass dt in a dict so :dt is correctly substituted
    cur.execute(sql, { "dt": dt })
    cols = [col[0].lower() for col in cur.description]
    data = cur.fetchall()
    cur.close()
    df = pd.DataFrame(data, columns=cols)
    print(f'{df.shape[0]} rows in total')
    return df[df['dataset'].isin(staged_datasets)]


def main():
    # Load enriched list
    enriched = pd.read_csv(enriched_csv)[enriched_col].astype(str).unique()
    staged_datasets = set(enriched)
    print(f"{len(staged_datasets)} enriched dataset names loaded")

    # Open Oracle connection
    conn = cx_Oracle.connect(conn_str)

    all_dfs = []
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        md = dt.date()
        print(f"Fetching {md:%Y-%m}…", end=" ")
        df = fetch_for_date(conn, md, staged_datasets)
        print(f"{len(df)} rows filtered")

        all_dfs.append(df)

    conn.close()

    # Combine, filter, and write one CSV
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined rows: {len(combined)}")
    combined.to_csv(output_file, index=False)
    #
    # filtered = combined[combined['dataset'].isin(enriched_set)]
    # print(f"Rows after filtering: {len(filtered)}")
    #
    # filtered.to_csv(output_file_filtered, index=False)
    # print(f"Wrote filtered data to {output_file}")

if __name__ == "__main__":
    main()
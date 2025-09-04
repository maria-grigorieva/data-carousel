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
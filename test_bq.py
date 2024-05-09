import pandas as pd
from google.cloud import bigquery


client = bigquery.Client()
sql = """
SELECT
  DISTINCT data_sync_job_id,
  sr.id as packager_id
FROM
  `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
JOIN
  `corp-bi-us-prod.rldb.service_requests` sr
ON
  SAFE_CAST(external_identifier AS int64) = data_sync_job_id
WHERE
  events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 day)
  AND events.status = 3
  AND step =4
  AND application_identifier = 2"""
print('query complete')
df = client.query(sql).to_dataframe()
print(df)
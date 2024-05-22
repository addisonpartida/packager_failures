import pandas as pd
from google.cloud import bigquery

multiple_fails_30d = """WITH failures_count AS (
  SELECT data_sync_job_id, COUNT(*) AS number_of_failures
  FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
  WHERE status = 3 AND step = 4 AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
  GROUP BY data_sync_job_id
)

SELECT fc.data_sync_job_id, sr.id as packager_id, fc.number_of_failures AS events_count
FROM failures_count fc
JOIN  `corp-bi-us-prod.rldb.service_requests` sr ON fc.data_sync_job_id = sr.id
WHERE fc.number_of_failures > 1;"""""

def get_failed_ids_30d():
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
    events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 day)
    AND events.status = 3
    AND step =4
    AND application_identifier = 2"""
    print('query complete')
    df = client.query(sql).to_dataframe()
    packager_id_list = df['packager_id'].values.tolist()
    print(packager_id_list, len(packager_id_list))
    return packager_id_list


def get_30d_10Fails():
    client = bigquery.Client()
    sql = """
    SELECT t1.data_sync_job_id, t2.deliverer_id as packager_id
    FROM `liveramp-ts-bigquery.partida_addison.10fails_30d_dsj` t1 
    JOIN `liveramp-ts-bigquery.partida_addison.dsj_to_packager_30d` t2
    ON t1.data_sync_job_id = t2.data_sync_job_id;"""
    print('query complete')
    df = client.query(sql).to_dataframe()
    packager_id_list = df['packager_id'].values.tolist()
    print(packager_id_list, len(packager_id_list))
    return packager_id_list

#pass in the desired query table MUST have a row called packager_id (in BQ this may be called deliverer_id )
def get_packager_id(query):
    client = bigquery.Client()
    sql = query
    print('query complete')
    df = client.query(sql).to_dataframe()
    packager_id_list = df['packager_id'].values.tolist()
    print(packager_id_list, len(packager_id_list))
    return packager_id_list
    


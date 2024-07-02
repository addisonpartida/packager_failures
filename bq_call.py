import pandas as pd
from google.cloud import bigquery

#pass in the desired query. table MUST have a row called packager_id (in BQ this may be called deliverer_id or request id/req_id)
def get_packager_id(query):
    client = bigquery.Client()
    sql = query
    print('query complete')
    df = client.query(sql).to_dataframe()
    packager_id_list = df['packager_id'].values.tolist()
    print(packager_id_list, len(packager_id_list))
    return packager_id_list

def get_dsj_id(query):
    client = bigquery.Client()
    sql = query
    print('query complete')
    df = client.query(sql).to_dataframe()
    dsj_id_list = df['data_sync_job_id'].values.tolist()
    print(dsj_id_list, len(dsj_id_list))
    return dsj_id_list
    


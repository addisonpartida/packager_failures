from google.cloud import bigquery
from pandas_gbq import to_gbq

def write_to_bq(df, project_id, dataset_id, table_name):
    # Upload the DataFrame to BigQuery
    to_gbq(df, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')

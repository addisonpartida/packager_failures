import requests
import json
from read_data import parse_json
from bq_call import get_packager_id
import pandas as pd

def query_loki_api(token, query):
    headers = {
    'Authorization': f'Basic {token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    }
    params = {
        'query': query,
        'limit': 5000,
        'start': '1713717420' ,
        'end': '1716309420' ,
    }
    url = 'https://logs-prod3.grafana.net/loki/api/v1/query_range'  # Replace 'example.com' with your Loki API URL
    response = requests.get(url, headers=headers, params=params)
    # breakpoint*
    if response.status_code == 200:
        # with open('data.json', 'w') as f:
        #     json.dump(response.json(), f)
        data = response.json()
        results = data['data']['result']
        # print(f"results: {results}")
        return results
       
    else:
        print(f"Failed to query Loki API. Status code: {response.status_code}")
        return None

# test 1 4740683361 test 2 4758116471
#query the pacakger ids from service request table to be passed into parser and output df containing ids, dates, errors. 

def parse_packager_logs():
    # packager_ids_test = [4758116471, 4740683361]
    # insert BQ SQL query here, as id_query to be passed into get_packager_id
    id_query = """SELECT 
    COUNT(DISTINCT sr.id) AS dist_pack_id,
    COUNT(DISTINCT CASE WHEN dsj.step = 12 AND dsj.status = 2 THEN dsj.id END) AS success_flag,
    COUNT(DISTINCT CASE WHEN e.status = 3 AND e.step = 4 THEN e.data_sync_job_id END) AS failed_pack,
    COUNT(DISTINCT CASE WHEN e.step = 4 THEN e.data_sync_job_id END) AS entered_pack,
    COUNT(DISTINCT CASE WHEN e.status = 2 AND e.step = 4 THEN e.data_sync_job_id END) AS completed_pack,
    -- Retrieve data_sync_job_id and request_id for failed jobs
    e.data_sync_job_id,
    sr.id as packager_id
FROM `corp-bi-us-prod.rldb.data_sync_job_events` e
JOIN `corp-bi-us-prod.rldb.data_sync_jobs` dsj ON e.data_sync_job_id = dsj.id
JOIN `corp-bi-us-prod.rldb.service_requests` sr ON SAFE_CAST(sr.external_identifier AS INT64) = e.data_sync_job_id
WHERE 
    DATE(dsj.created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND sr.application_identifier = 2
    AND e.status = 3 AND e.step = 4 -- Filter for failed jobs
GROUP BY e.data_sync_job_id, sr.id
ORDER BY failed_pack DESC
LIMIT 10;
"""""
    

    packager_ids = get_packager_id(id_query)
    main_df = pd.DataFrame()
    error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"}' 
    token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
    for id in packager_ids:
        new_query = error_query + f" |= `{id}` | json "
        print(new_query)
        new_result = query_loki_api(token, new_query)
        # print(new_result)
        if new_result:
            new_df = parse_json(new_result)
            main_df = pd.concat([main_df, new_df])
        else:
            pass
    main_df.to_csv('to_bq.csv', index=False)
    print(main_df)
    return main_df

parse_packager_logs()
import requests
import json
from read_data import parse_json
from bq_call import get_packager_id, get_dsj_id
import pandas as pd
## this file calls the API, par

def query_loki_api(token, query):
    headers = {
    'Authorization': f'Basic {token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    }
    params = {
        'query': query,
        'limit': 5000,
        'start': '1716944461' 
        # 'end': '1718195649' ,
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
    id_query1 = """SELECT 
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
    DATE(dsj.created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND sr.application_identifier = 2
    AND e.status = 3 AND e.step = 4 -- Filter for failed jobs
    
GROUP BY e.data_sync_job_id, sr.id
ORDER BY failed_pack DESC
LIMIT 10;"""""
    id_query2 = """WITH FailedPackagerJobs AS (
  -- Subquery to get jobs that failed the packager
  SELECT data_sync_job_id
  FROM  `corp-bi-us-prod.rldb.data_sync_job_events` events
  WHERE step = 4 AND status = 3
  AND events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 day)
),

MultipleConfigurations AS (
  -- Subquery to get jobs with more than one configuration completed step
  SELECT data_sync_job_id
  FROM `corp-bi-us-prod.rldb.data_sync_job_events` events
  WHERE step = 27 AND status = 4 AND events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 day)
  GROUP BY data_sync_job_id
  HAVING COUNT(*) > 2
),

SuccessfulPackagerJobs as (
  SELECT data_sync_job_id
  FROM  `corp-bi-us-prod.rldb.data_sync_job_events` events
  WHERE step = 4 AND status = 2
  AND events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 day)
)

-- Main query to find jobs that passed in the packager after failing and having more than one configuration completed step
SELECT DISTINCT(sr.id) as packager_id, e.data_sync_job_id
FROM  FailedPackagerJobs e
JOIN MultipleConfigurations mc ON e.data_sync_job_id = mc.data_sync_job_id
LEFT JOIN SuccessfulPackagerJobs s on s.data_sync_job_id = e.data_sync_job_id
JOIN `corp-bi-us-prod.rldb.service_requests` sr ON SAFE_CAST(external_identifier AS int64) = e.data_sync_job_id
where s.data_sync_job_id is null AND sr.application_identifier = 2"""
    
    id_query3 = """

with failed_dsj AS(
SELECT *
FROM `corp-bi-us-prod.rldb.data_sync_jobs` dsj
WHERE step = 4 AND status = 3  AND created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
)
SELECT DISTINCT sr.id as packager_id, sr.created_at as pack_created, failed_dsj.id as dsj_id, failed_dsj.created_at as dsj_created
FROM failed_dsj
JOIN `corp-bi-us-prod.rldb.service_requests` sr ON SAFE_CAST(external_identifier AS int64) = failed_dsj.id
WHERE sr.application_identifier = 2
"""
    batch1 = """WITH failures_count AS (
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 0;
"""
    batch2 = """WITH failures_count AS (
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 5000;
"""
    batch3 = """WITH failures_count AS (
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 10000;
"""
    batch4 = """WITH failures_count AS (
    
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 15000;
"""
    batch5 = """WITH failures_count AS (
    
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 20000;
"""
    batch6 = """WITH failures_count AS (
    
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 25000;
"""
    batch7 = """WITH failures_count AS (
    
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 30000;
"""
    batch8 = """WITH failures_count AS (
    
    SELECT 
        data_sync_job_id,
        COUNT(*) AS number_of_failures
    FROM `corp-bi-us-prod.rldb.data_sync_job_events` AS events
    WHERE status = 3 
        AND step = 4 
        AND updated_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY data_sync_job_id
),

packager_ids AS (
    SELECT 
        sr.id AS packager_id,
        SAFE_CAST(sr.external_identifier AS INT64) AS data_sync_job_id,
        events.created_at
    FROM `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
    JOIN `corp-bi-us-prod.rldb.service_requests` sr
        ON SAFE_CAST(sr.external_identifier AS INT64) = events.data_sync_job_id
    WHERE events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND events.status = 3
        AND events.step = 4
        AND sr.application_identifier = 2
)

SELECT 
    p.packager_id,
    p.data_sync_job_id,
    f.number_of_failures,
    p.created_at
FROM packager_ids p
JOIN failures_count f ON p.data_sync_job_id = f.data_sync_job_id
WHERE f.number_of_failures >= 10
ORDER BY p.data_sync_job_id
LIMIT 5000 OFFSET 35000;
"""
    opi_request= """WITH dsj_ids AS (
    SELECT dsjs.id
    FROM `corp-bi-us-prod.rldb.data_sync_jobs` dsjs
    WHERE dsjs.external_client_identifier IN (
        SELECT CAST(ssas.id AS STRING)
        FROM `corp-bi-us-prod.rldb.server_side_accounts` ssas
        WHERE ssas.destination_account_id IN (
            SELECT das.id
            FROM `corp-bi-us-prod.rldb.destination_accounts` das
            WHERE das.customer_id = 557216
        )
    )
    AND dsjs.created_at BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

SELECT 
    DISTINCT sr.id AS packager_id,
    events.data_sync_job_id,
    events.created_at
FROM 
    `corp-bi-us-prod.rldb.stg_rldb_data_sync_job_events` events
JOIN 
    `corp-bi-us-prod.rldb.service_requests` sr
ON 
    SAFE_CAST(sr.external_identifier AS int64) = events.data_sync_job_id
JOIN 
    dsj_ids
ON 
    dsj_ids.id = events.data_sync_job_id
WHERE 
    events.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND events.status = 3
    AND events.step = 4
    AND sr.application_identifier = 2;
"""


    packager_ids = get_packager_id(opi_request)
    # dsj_ids = get_dsj_id(id_query2)
    # packager_ids = ['']
    main_df = pd.DataFrame()
    error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon.*", cluster_name="opi-prod-2", namespace="default", level="ERROR"}' 
    token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
    counter = 0
    for id in packager_ids:
        new_query = error_query + f" |= `{id}` | json "
        counter += 1
        print(new_query, counter)
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
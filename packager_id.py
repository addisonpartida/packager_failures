import requests, sys
import json
from read_data import parse_json
from bq_call import get_packager_id, get_dsj_id
import pandas as pd

user_query_filepath = input("Give the path to the txt file containing the SQL query to BQ. This query must contain a column named packager_id to run successfully: ")
user_timestamp = input("Desired start date of query, in epoch time, up to the last 30 days, per Loki API: ")

def query_loki_api(token, query):
    headers = {
    'Authorization': f'Basic {token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    }
    params = {
        'query': query,
        'limit': 5000,
        'start': user_timestamp 
        # 'end': '1718195649' ,
    }
    url = 'https://logs-prod3.grafana.net/loki/api/v1/query_range'
    response = requests.get(url, headers=headers, params=params)
    # breakpoint*
    if response.status_code == 200:
        # with open('data.json', 'w') as f:
        #     json.dump(response.json(), f)
        data = response.json()
        results = data['data']['result']
        return results
       
    else:
        print(f"Failed to query Loki API. Status code: {response.status_code}")
        return Non

def parse_packager_logs():
    try:
        with open(user_query_filepath, 'r') as file:
                query_text = file.read()
                print(query_text)
    except FileNotFoundError: 
        print("Invalid file path. Please check the query. ")
        sys.exit(1)
        breakpoint()
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


    packager_ids = get_packager_id(query_text)

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

"""
/Users/aparti/test_projects/user_query.txt
1718039043
"""
import requests
import json
from read_data import parse_json
from bq_call import get_ids
import pandas as pd

def query_loki_api(token, query):
    headers = {
    'Authorization': f'Basic {token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    }
    params = {
        'query': query,
        'limit': 1000,
        'start': '1712768410' ,
        'end': '1715259600' ,
    }
    url = 'https://logs-prod3.grafana.net/loki/api/v1/query_range'  # Replace 'example.com' with your Loki API URL
    response = requests.get(url, headers=headers, params=params)
    # breakpoint
    if response.status_code == 200:
        with open('data.json', 'w') as f:
            json.dump(response.json(), f)
        data = response.json()
        stats = data['data']['stats']['summary']['totalLinesProcessed']
        print(f"STATS: {stats}")
        # print(data)
    else:
        print(f"Failed to query Loki API. Status code: {response.status_code}")
        return None


# if __name__ == "__main__":
#     token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
#     query_test = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"}'
#     error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"}'
    # result = query_loki_api(token, error_query)

    # if result:
        # print("Success!")
    #     print("Query Result:")
    #     print(result)

# test 1 4740683361 test 2 4758116471
packager_ids_test = [4740683361, 4758116471]
main_df = pd.DataFrame()
error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"} | json' 

token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
for id in packager_ids_test:
    new_query = error_query + f" |= `{id}` | json "
    print(new_query)
    new_result = query_loki_api(token, new_query)
    new_df = parse_json(new_result)
    print(new_df)
    main_df = pd.concat([main_df, new_df])

print(main_df)
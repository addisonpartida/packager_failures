import requests
import json

def query_loki_api(token, query):
    headers = {
    'Authorization': f'Basic {token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    }
    params = {
        'query': query,
        'limit': 10,
        'start': '1711832441' ,
        'end': '1714348841' ,
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
        print(data)
    else:
        print(f"Failed to query Loki API. Status code: {response.status_code}")
        return None
    
if __name__ == "__main__":
    token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
    query_test = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"}'
    error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"} | json'
    result = query_loki_api(token, error_query)

    if result:
        print("Success!")
    #     print("Query Result:")
    #     print(result)

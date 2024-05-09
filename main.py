from read_data import parse_json
# from read_data import write_to_csv
from call_loki import query_loki_api

def main():
    #call API
    token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
    # query_test = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"}'
    error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"} | json'

    loki_logs = query_loki_api(token, error_query)
    parsed_data_frame = parse_json(loki_logs)
    # write_to_csv(parsed_data_frame)


if __name__ == "__main__":
    main()

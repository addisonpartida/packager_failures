from read_data import parse_json
from write_data_bq import write_to_bq
from call_loki import query_loki_api

def main():
    #call API
    token = 'MjQ4ODczOmdsY19leUp2SWpvaU5qYzRNRGt3SWl3aWJpSTZJbXh2YTJrdFlYQnBMWEpsWVdRdGJHOXJhUzFoY0drdGNtVmhaQ0lzSW1zaU9pSTNNMkp6T0RZMVFqZ3lVelpqY0VsbmRUbFNkMVV6YlZBaUxDSnRJanA3SW5JaU9pSjFjeUo5ZlE9PQo='
    # query_test = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"}'
    error_query = '{team="opi", region="us", environment="prod", app=~"packager-daemon", cluster_name="opi-prod-2", namespace="default", level="ERROR"} | json'
    result = query_loki_api(token, error_query)

    if result:
        print("Success!")
        print("Query Result:")
        print(result)

    # Parse JSON and get DataFrame
    file_path = 'data.json'
    df = parse_json(file_path)
    breakpoint()
    # Specify BigQuery details
    project_id = 'your-project-id'
    dataset_id = 'your-dataset-id'
    table_name = 'your-table-name'
    
    # Write DataFrame to BigQuery
    write_to_bq(df, project_id, dataset_id, table_name)

if __name__ == "__main__":
    main()

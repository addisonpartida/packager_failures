#this file handles the imported json data from api query and grabs desired information from logs including date, req_id, and error message

import sqlite3
import json
import time
from datetime import datetime
import re
import pandas as pd
import csv

def timestamp_to_epoch(timestamp):
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch_time = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(epoch_time)

def categorize_errors(error_message):
    if not isinstance(error_message, str):
        return 'other_error_not_string'
    error_patterns = {
        'Invalid Path': [
            r'.* Cannot generate a package summary because an invalid package path was given: .* is neither a directory nor a file path.',
            r'.*java.lang.IllegalStateException: instance must be started before calling this method.*',
            r'.*Path does not exist.*',
            r'.* Cannot write empty metadata file.*'
        ],
        'Null Pointer Exception': [
            r'.*java.lang.NullPointerException.*',
            r'.* Null pointer at line .*'
        ],
        'Too Many Requests': [
            r'.*Exceeded the maximum number of output streams.*'
        ],
        'OOM': [
            r'.* OutOfMemory .*',
            r'.*Java heap space.*'
        ],
        'Connection Error': [
            r'.*KeeperErrorCode = ConnectionLoss.*',
            r'.*Lost Connection while trying to acquire lock.*',
            r'.*Connection reset.*'
    
        ],
        'GCS Errors': [
            r'.*java.lang.RuntimeException: com.google.cloud.storage.StorageException: Unknown Error.*'
            r'.*exception_message": ".*com.google.cloud.storage.StorageException: We encountered an internal error.*',
            r'.*com.google.cloud.storage.StorageException: Read timed out.*',
            r'.* exception while processing: com.google.cloud.storage.StorageException: .* Please reduce your request rate',
            r'.*com.liveramp.daemon_lib.utils.DaemonException: com.google.cloud.storage.StorageException: Read timed out.*',
            r'.*com.google.cloud.storage.StorageException.*',
            r'.*encountered an internal error.*',
            r'.*GC overhead limit exceeded.*',
            r'.*Your request-rate on the bucket has ramped up too fast.*'
        ],
        'Timed Out': [
            r'.* message": "ShutdownHookManager shutdown forcefully after 30 seconds .*',
            r'.* org.apache.avro.AvroRuntimeException: java.io.IOException .*',
            r'.*Read timed out.*'
        ],
        'Misc' : [
            r'.* java.io.IOException: Lost connection while trying to acquire lock .*',
            r'.*ShutdownHookManager.*',
            r'.*Invalid sync.*',
            r'.*Unable to delete config.*',
            r'.*instance must be started before calling this method.*',
            r'.*IndexOutOfBoundsException.*',
            r'.*Unable to persist.*'
        ],
        'Undefined Variables/Methods':[
            r'.*instance must be started before calling this method.*',
            r'.*undefined local variable or method.*',
            r'.*instance must be started.*'

        ],

        'Zookeeper Errors':[
            r'.*ZooKeeper.*',
            r'.*ZK connection.*'
            
        ]
}
    # for error_type, pattern in error_patterns.items():
    #     if re.search(pattern, error_message, re.IGNORECASE):
    #         return error_type
    
    for error_type, pattern_list in error_patterns.items():
        for pattern in pattern_list:
            # Ensure pattern is a string before using it
            if re.search(pattern, error_message, re.IGNORECASE):
                return error_type    
    return 'Uncategorized error'



data = None
with open('data.json') as json_data:
     d = json.load(json_data)

results = d['data']['result']



def parse_json(results):
#grab information from logs that I want:
# add each line to dictionary where key is req_id
# value is list of dictionaries that contatin time, id, error
# req_id: [{ts1, id1, error1}, {ts2,id2,error2}, etc...]
    
    table_dictionary = {}
    entry_meta_list = []
    for entry in results:
        #get request ID
        if entry['stream'].get('attributes_mdc_request_id'):
            stream_reqid = entry['stream']['attributes_mdc_request_id']
        else:
            stream_reqid = None
        #get timestamp
        if  entry['stream'].get('attributes__timestamp'):
            stream_time = entry ['stream']['attributes__timestamp'] 
            epoch_time = timestamp_to_epoch(stream_time)
        else: 
            stream_time = None
        #get error message
        if entry['stream'].get('attributes_exception_exception_message'):
            stream_error = entry['stream']['attributes_exception_exception_message']
            s1= '\n'
            s2= " "
            stream_error = stream_error.replace(s1, s2)
        elif entry['stream'].get('attributes_message'):
            stream_error = entry['stream']['attributes_message']
            s1= '\n'
            s2= " "
            stream_error = stream_error.replace(s1, s2)
        
        else:
            stream_error = None

    #categorizes the error messages with helper from above, using patterns listed above too
        error_type = categorize_errors(stream_error)

        #if key is already in dict, append a new dictionary to the list value
        entry_meta = {}
        if stream_reqid in entry_meta:
            entry_meta.append({"date": stream_time, "epoch_time": epoch_time, "Error_Message": stream_error, "Error Category": error_type})
        else: 
            entry_meta[stream_reqid] = [{"date": stream_time, "epoch_time": epoch_time, "Error_Message": stream_error, "Error Category": error_type}]
        entry_meta_list.append(entry_meta)
    # print(entry_meta_list)
    # output_file_path = 'errors_to_bq.jsonl'
    # for i in entry_meta_list:
    #     entry_meta_list[stream_error].replace('\n', '\\n')

    df_data = []
    for entry_meta in entry_meta_list:
        for req_id, entries in entry_meta.items():
            for entry in entries:
                df_data.append({
                    "ID": req_id,
                    "timestamp": entry["date"],
                    "epoch_time": entry["epoch_time"],
                    "error_message": entry["Error_Message"],
                    "error_category": entry["Error Category"]
                })

    df = pd.DataFrame(df_data)
    return df

parsed_df = parse_json(results)
print(parsed_df)

parsed_df.to_csv('to_bq.csv', index=False)

# def preprocess_csv(input_file, output_file):
#     with open('to_bq.csv', 'r') as f_in, open(output_file, 'w') as f_out:
#         reader = csv.reader(f_in)
#         writer = csv.writer(f_out)

#         for row in reader:
#             # Check if the row contains an error message
#             if len(row) > 0 and 'ERROR' in row[0]:
#                 # Replace newline characters with a space
#                 error_message = ' '.join(row)
#                 writer.writerow([error_message])
#             else:
#                 writer.writerow(row)
# preprocess_csv('to_bq.csv', 'to_bq_pretty.csv')                



######### BELOW IS FOR EXPORTING AS JSONL to "errors_to_bq.jsonl"
    # with open(output_file_path, "w") as jsonl_file:
    #     for entry_meta in entry_meta_list:
    #         for req_id, entries in entry_meta.items():
    #             for entry in entries:
    #                 data = {req_id: {
    #                     "date": entry["date"],
    #                     "epoch_time": entry["epoch_time"],
    #                     "Error_Message": entry["Error_Message"],
    #                     "Error Category": entry["Error Category"]
    #                 }}
    #                 jsonl_file.write(json.dumps(data) + "\n")

    # # print("Data exported to", output_file_path)
    # print("Data exported to", 'errors_to_bq.json')
            
parse_json(results)

#try to send to bq from here

# from google.cloud import bigquery
# from pandas_gbq import to_gbq

# project_id = 'liveramp-ts-bigquery.partida_addison'
# table_name = 'liveramp-ts-bigquery.partida_addison.TEST'

# to_gbq(parsed_df, table_name, project_id=project_id, if_exists='replace')
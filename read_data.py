#this file handles the imported json data from api query and grabs desired information from logs including date, req_id, and error message

import sqlite3
import json
import time
from datetime import datetime
import re

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

#grab information from logs that I want:
# add each line to dictionary where key is req_id
# value is list of dictionaries that contatin time, id, error
# req_id: [{ts1, id1, error1}, {ts2,id2,error2}, etc...]

table_dictionary = {}
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
    elif entry['stream'].get('attributes_message'):
        stream_error = entry['stream']['attributes_message']
    
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
   
  
    print(entry_meta)

    # print(stream_reqid, epoch_time, stream_error)





# ####### writing now to database
# # Example dictionary
# my_dict= entry_meta
# # Connect to SQLite database (create one if it doesn't exist)
# conn = sqlite3.connect('my_database.db')
# cursor = conn.cursor()

# # Create a table
# cursor.execute('''CREATE TABLE IF NOT EXISTS error_logs (
#                     req_id INT,
#                     date TEXT,
#                     error TEXT,
#                 )''')

# # Insert data from dictionary into the table
# for key, value in my_dict.items():
#     for entry in value:
#         cursor.execute("INSERT INTO error_logs (req_id, date, error, error_type) VALUES (?, ?, ?)", (key, entry['date'], entry['error']))

# # Commit changes and close connection
# conn.commit()
# conn.close()
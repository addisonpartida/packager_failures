#this file handles the imported json data from api query and grabs desired information from logs including date, req_id, and error message


import json
import time
from datetime import datetime

def timestamp_to_epoch(timestamp):
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch_time = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(epoch_time)


data = None
with open('data.json') as json_data:
     d = json.load(json_data)

results = d['data']['result']
count = 0
#grab information from logs that I want:
# add each line to dictionary where key is req_id
# value is list of dictionaries that contatin time, id, error
# req_id: [{ts1, id1, error1}, {ts2,id2,error2}, etc...]

table_dictionary = {}
for entry in results:
    if entry['stream'].get('attributes_mdc_request_id'):
        stream_reqid = entry['stream']['attributes_mdc_request_id']
    else:
        stream_reqid = None
    if  entry['stream'].get('attributes__timestamp'):
        stream_time = entry ['stream']['attributes__timestamp'] 
        epoch_time = timestamp_to_epoch(stream_time)
    else: 
        stream_time = None
    if entry['stream'].get('attributes_exception_exception_message'):
        stream_error = entry['stream']['attributes_exception_exception_message']
    else:
        stream_error = None
    #if key is already in dict, append a new dictionary to the list value
    entry_meta = {}
    if stream_reqid in entry_meta:
        entry_meta.append({"date": stream_time, "epoch_time": epoch_time, "Error_Message": stream_error})
    else: 
        entry_meta[stream_reqid] = [{"date": stream_time, "epoch_time": epoch_time, "Error_Message": stream_error}]

    print(stream_reqid, epoch_time, stream_error)
     
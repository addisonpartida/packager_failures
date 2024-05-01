import re
import sqlite3
import os 
import requests

# API TOKEN: glc_eyJvIjoiNjc4MDkwIiwibiI6ImRlZmF1bHQtbG9ncy1yZWFkZXItZGVmYXVsdCIsImsiOiJHNzVvdjJTN0g1Szczb3RTenhqNmM1UDEiLCJtIjp7InIiOiJ1cyJ9fQ
# Can grafana differentiate between failed and completed logs, or 
# should I get job IDs from Distr Dashboard, and use it to get the ID, adn 
# then query the corresponding logs from grafana/whatever API gives me access 
# to them?

#api_url = "https://logs-prod3.grafana.net"


# Dictionary to store error types to error messages.
# Add new error messages/rules here. 
regex_patterns = {
    'Invalid Path': [
        '().* Cannot generate a package summary because an invalid package path was given: .* is neither a directory nor a file path.',
        '.* Another invalid path: .*'
        '"java.lang.IllegalStateException: instance must be started before calling this method'
    ],
    'Null Pointer Exception': [
        '.* exception while processing: java.lang.NullPointerException',
        '.* Null pointer at line .*'
    ],
    'Error 429: Too Many Requests': [
        '.* exception while processing: com.google.cloud.storage.StorageException: .* Please reduce your request rate',
        '.* Too many requests error: .*'
    ],
    'OOM': [
        '.* OutOfMemory .*'
    ],
    'GCS Errors': [
        '.* exception_message": "com.google.cloud.storage.StorageException: We encountered an internal error. Please try again. .*'
        '.* "com.google.cloud.storage.StorageException: Read timed out .*'
        '.* com.liveramp.daemon_lib.utils.DaemonException: com.google.cloud.storage.StorageException: Read timed out .*'
    ],
    'Timed Out': [
        '.* message": "ShutdownHookManager shutdown forcefully after 30 seconds .*',
        '.* org.apache.avro.AvroRuntimeException: java.io.IOException .*'
    ],
    'Misc' : [
         ' .* java.io.IOException: Lost connection while trying to acquire lock .*',

        ],

}
matched_line_information= {
    '(.*) packager.*packages(.*)is .*/gm'
}

#gets files from API & stores into a list to be passed
#passed into process_multiple_logs

def fetch_logs(api_url):
    response = requests.get(api_url)
    if response.status_code == (200):
        list_of_logs = response.json() #assuming API gives JSON list of paths
    else:
        print("Failed to fetch log files from API")

#takes list_of_logs from API call and individually processes each
#want to call fxn to save to db either here or in log_parser
def process_multiple_logs(list_of_logs):
    for log_file in list_of_logs:
        if os.path.isfile(list_of_logs):
            print(f"Processing log file: {log_file}")
            log_parser(log_file)
        else:
            print(f"Error: {log_file} is not a valid file path.")

#store count of errors.
error_counts = {category: 0 for category in regex_patterns}

# given a line, return the category that the error falls in 
def error_type(line):
    for category, patterns in compiled_patterns.items():
        for pattern in patterns:
            if pattern.search(line):
                return category
    

# Compile the regex patterns for efficiency
compiled_patterns = {category: [re.compile(pattern) for pattern in patterns] for category, patterns in regex_patterns.items()}

#check a line for any patterns that match the regex dictionary
def check_line(line):
    line = str(line)
    for category, patterns in compiled_patterns.items():
        for pattern in patterns:
            if pattern.search(line):
                return category
    return None

def log_parser(log_file):
    # Open log file
    with open(log_file, 'r') as file:
        for line in file:
            # Check to see if line matches any regex patterns
            matched_category = error_type(line)
            if matched_category:
                print(f"Line matched category '{matched_category}': {line.strip()}")
                if matched_category == 'Invalid Path':
                    invalid_path_parse(line)
                    print("HERE IS WHERE YOU WANT TO CAPTURE INFO.")
                elif matched_category == 'Null Pointer Excpetion':
                    NullPointerParse(line)
                elif matched_category == 'Error 429: Too Many Requests':
                    TooManyRequestsParse(line)
                elif matched_category == 'Timed Out':
                    pass
                error_counts[matched_category] += 1


def extract_info(line):
    # Regular expression pattern to match date, time, and packager ID
    regex_pattern = r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}.\d{3}).*packager-daemon.*working_directory/(\d+)/'
    
    match = re.match(regex_pattern, line)
    if match:
        date = match.group(1)
        time = match.group(2)
        packager_id = match.group(3)
        return date, time, packager_id
    else:
        return None

# Example usage
line = "2024-04-18 16:31:30.564   packager-daemon INFO exception while processing: com.google.cloud.storage.StorageException: The object liveramp-eng-dist-deliverer-input/working_directory/4718911001/qc_p-LnfGw1PLDjjyp_1713481381_876926_incr.info exceeded the rate limit for object mutation operations (create, update, and delete). Please reduce your request rate. See https://cloud.google.com/storage/docs/gcs429."
date, time, packager_id = extract_info(line)
print("Date:", date)
print("Time:", time)
print("Packager ID:", packager_id)


log_file_path = "/Users/aparti/test_projects/sample_logs/log_sample1.txt"
log_parser(log_file_path)

# print("Error counts:")
# for category, count in error_counts.items():
#     print(f"{category}: {count}")
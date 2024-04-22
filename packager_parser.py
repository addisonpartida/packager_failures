import re

# Dictionary to store error types to error messages
# Add new error messages/rules here
regex_patterns = {
    'Invalid Path': [
        '.* Cannot generate a package summary because an invalid package path was given: .* is neither a directory nor a file path.',
        '.* Another invalid path: .*'
    ],
    'Null Pointer Exception': [
        '.* exception while processing: java.lang.NullPointerException',
        '.* Null pointer at line .*'
    ],
    'Error 429: Too Many Requests': [
        '.* exception while processing: com.google.cloud.storage.StorageException: .* Please reduce your request rate',
        '.* Too many requests error: .*'
    ]
}

def error_type(line):
    for category, patterns in compiled_patterns.items():
        for pattern in patterns:
            if pattern.search(line):
                return category
    

# Compile the regex patterns for efficiency
compiled_patterns = {category: [re.compile(pattern) for pattern in patterns] for category, patterns in regex_patterns.items()}

def check_line(line):
    line = str(line)
    for category, patterns in compiled_patterns.items():
        for pattern in patterns:
            if pattern.search(line):
                return category
    return None

def parser_log(log_file):
    # Open log file
    with open(log_file, 'r') as file:
        for line in file:
            # Check to see if line matches any regex patterns
            matched_category = error_type(line)
            if matched_category:
                print(f"Line matched category '{matched_category}': {line.strip()}")

       

log_file_path = "/Users/aparti/test_projects/sample_logs/log_sample3.txt"
parser_log(log_file_path)

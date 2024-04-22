import re

def path_errors():
    pass

#errors
no_path_error = '.* Cannot generate a package summary because an invalid package path was given: .* is neither a directory nor a file path.'
null_pointer_error = '.* exception while processing: java.lang.NullPointerException'
too_many_requests = '.* exception while processing: com.google.cloud.storage.StorageException: .* Please reduce your request rate'

def parser_log(log_file):
    #open log file
    with open(log_file, 'r') as file:
        #append each error line that matches to a list that will be returned
        match_list = []
        for line_number, line in enumerate(file, start = 1):
            #check if each line can fall into any of the rules

            for match in re.finditer(no_path_error, line, re.S):
                match_text = match.group()
                match_list.append(match_text)
    for match in match_list:
        print(match)

log_file_path = "/Users/aparti/test_projects/sample_logs/log_sample1.txt"
parser_log(log_file_path)
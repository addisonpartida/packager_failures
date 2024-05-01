import sqlite3
import read_data.py 

# Example dictionary
my_dict= entry_meta
# Connect to SQLite database (create one if it doesn't exist)
conn = sqlite3.connect('my_database.db')
cursor = conn.cursor()

# Create a table
cursor.execute('''CREATE TABLE IF NOT EXISTS error_logs (
                    req_id INT,
                    date TEXT,
                    error TEXT,
                    error_type TEXT,
                )''')

# Insert data from dictionary into the table
for key, value in my_dict.items():
    for entry in value:
        cursor.execute("INSERT INTO error_logs (req_id, date, error, error_type) VALUES (?, ?, ?)", (key, entry['date'], entry['error']))

# Commit changes and close connection
conn.commit()
conn.close()

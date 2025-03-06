import pandas as pd
import pyodbc
import os

# Define the source CSV file
source_csv_file = 'C:/Users/valentinosko/Downloads/Etoro_trade_reports_from_3rd_Feb_new1.csv'

# Define the SQL connection string
conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=AZR-WE-BI-02;'
    r'DATABASE=RTS;'
    r'Trusted_Connection=yes;'
)

# Define the log file to keep track of the last processed batch
log_file = 'batch_progress.log'

# Function to read the last processed batch index
def read_last_processed_index():
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            return int(file.read().strip())
    return -1

# Function to write the last processed batch index
def write_last_processed_index(index):
    with open(log_file, 'w') as file:
        file.write(str(index))

# Attempt to read data from CSV file with different encodings if UTF-8 fails
try:
    data = pd.read_csv(source_csv_file, delimiter='\t', encoding='utf-8')
except UnicodeDecodeError:
    try:
        data = pd.read_csv(source_csv_file, delimiter='\t', encoding='utf-16')
    except UnicodeDecodeError:
        data = pd.read_csv(source_csv_file, delimiter='\t', encoding='ISO-8859-1')

# Print column names to debug
print("Column names from CSV:")
print(data.columns)

# Inspect the data types
print("Data types:")
print(data.dtypes)

# Convert the SENDING_TIME and TRANSACT_TIME columns to datetime
data['SENDING_TIME'] = pd.to_datetime(data['SENDING_TIME'], format='%m/%d/%y %H:%M')
data['TRANSACT_TIME'] = pd.to_datetime(data['TRANSACT_TIME'], format='%Y%m%d-%H:%M:%S.%f')

# Prepare data for batch insertion
insert_data = data.values.tolist()

# Function to insert a batch and handle errors
def insert_batch(batch):
    try:
        cursor.executemany("""
            INSERT INTO TradEcho_Reports_1 (
                SENDING_TIME,
                MSG_TYPE,
                VIRT_CLIENT_ID,
                FIRM_TRADE_ID,
                SECURITY_ID,
                CURRENCY,
                LAST_QTY,
                LAST_PX,
                NOTIONAL_AMOUNT,
                TRANSACT_TIME,
                VIRT_FIX_PAYLOAD_ASCII
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()
    except Exception as e:
        print(f"Error inserting batch: {e}")
        conn.rollback()
        raise

# Create a connection to the SQL Server
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Determine the last processed batch index
last_index = read_last_processed_index()

# Generate and execute batch INSERT statements
batch_size = 1000  # Adjust batch size as needed

try:
    for i in range((last_index + 1) * batch_size, len(insert_data), batch_size):
        batch = insert_data[i:i+batch_size]
        insert_batch(batch)
        # Update the log file with the last successful batch index
        write_last_processed_index(i // batch_size)
finally:
    # Close the connection
    conn.close()
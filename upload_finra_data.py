import pandas as pd
import pyodbc
from datetime import datetime

# Step 1: Load and Process the Data
try:
    url = "https://files.catnmsplan.com/symbol-master/FINRACATReportableEquitySecurities_EOD.txt"
    df = pd.read_csv(
        url, sep="|", header=None, names=["Symbol", "IssueName", "ListingExchange", "TestIssueFlag"]
    )
    # Drop the first row
    df.drop(0, axis=0, inplace=True)
    # Insert column "ReportDate" using the current date
    df.insert(0, 'ReportDate', datetime.now().strftime('%Y-%m-%d'))
    print(f"Data loaded and processed. Total rows: {df.shape[0]}")
except Exception as e:
    print(f"Error processing data: {e}")
    exit()

# Step 2: Upload Data to SQL Server
try:
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=AZR-WE-BI-02;'
        'DATABASE=RTS;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # Insert data into SQL Server
    for index, row in df.iterrows():
        cursor.execute("""
                INSERT INTO FINRACATReportableEquitySecurities_EOD (ReportDate, Symbol, IssueName, ListingExchange, TestIssueFlag)
                VALUES (?, ?, ?, ?, ?)
            """, str(row.ReportDate), str(row.Symbol), str(row.IssueName), str(row.ListingExchange),
                       str(row.TestIssueFlag))

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Data uploaded to SQL Server successfully.")
except Exception as e:
    print(f"Error uploading data to SQL Server: {e}")

import pandas as pd
import os

# Define file paths
file_paths = {
    "IG Margin Activity Report": "/mnt/data/IG Margin Activity Report.csv",
    "MAREX Margin Activity Report": "/mnt/data/MAREX Margin Activity Report.csv",
    "IG Margin State Report": "/mnt/data/IG Margin State Report.csv",
    "MAREX Margin State Report": "/mnt/data/MAREX Margin State Report.csv",
    "IG Reconciliation Report": "/mnt/data/IG Reconciliation Report.csv",
    "MAREX Reconciliations Report": "/mnt/data/MAREX Reconciliations Report.csv",
    "IG Reconciliations Report Immediate Feedback": "/mnt/data/IG Reconciliations Report Immediate Feedback.csv",
    "MAREX Reconciliations Report Immediate Feedback": "/mnt/data/MAREX Reconciliations Report Immediate Feedback.csv",
    "MAREX Rejection Statistics Report": "/mnt/data/MAREX Rejection Statistics Report.csv",
    "IG Trade State Report": "/mnt/data/IG Trade State Report.csv",
    "MAREX Trade State Report": "/mnt/data/MAREX Trade State Report.csv",
    "IG Warnings Report": "/mnt/data/IG Warnings Report.csv",
    "MAREX Warnings Report": "/mnt/data/MAREX Warnings Report.csv"
}

# Initialize a summary list
report_summaries = []

# Read each file and extract column names and general information
for report_name, path in file_paths.items():
    try:
        df = pd.read_csv(path, nrows=5)  # Read only first 5 rows for quick analysis
        column_names = df.columns.tolist()
        num_columns = len(column_names)
        num_rows = len(df)

        # Check for common patterns in field names
        common_patterns = []
        if any("LEI" in col for col in column_names):
            common_patterns.append("Contains LEI fields (Legal Entity Identifiers)")
        if any("Amount" in col or "Valuation" in col for col in column_names):
            common_patterns.append("Contains financial/valuation data")
        if any("Date" in col or "Timestamp" in col for col in column_names):
            common_patterns.append("Contains date/time-related fields")
        if any("ID" in col or "Transaction" in col for col in column_names):
            common_patterns.append("Contains transaction identifiers")

        report_summaries.append({
            "Report Name": report_name,
            "Number of Columns": num_columns,
            "Sample Columns": ", ".join(column_names[:5]),  # Show first 5 column names
            "Detected Patterns": ", ".join(common_patterns) if common_patterns else "No clear patterns detected"
        })

    except Exception as e:
        report_summaries.append({
            "Report Name": report_name,
            "Number of Columns": "Error reading file",
            "Sample Columns": str(e),
            "Detected Patterns": "N/A"
        })

# Convert summary to DataFrame and display
report_summary_df = pd.DataFrame(report_summaries)
import ace_tools as tools

tools.display_dataframe_to_user(name="Unavista EOD Reports Summary", dataframe=report_summary_df)

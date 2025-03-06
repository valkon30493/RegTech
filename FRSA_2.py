import os
import pandas as pd

# Folder path where your CSV files are located
folder_path = 'C:/Users/valentinosko/frsa'

# List to store data from each file
dataframes = []

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)

        # Read each CSV file, handling encoding if needed
        df = pd.read_csv(file_path, header=None, encoding='UTF-16')  # Adjust encoding if necessary

        # Add a new row at the top with the phrase in the first cell (optional)
        insert_phrase = "INSERT INTO @temp_cid_list (CID) VALUES"
        new_row = pd.DataFrame([[insert_phrase]], columns=df.columns)
        df = pd.concat([new_row, df], ignore_index=True)

        # Append the DataFrame to the list
        dataframes.append(df)

# Concatenate all dataframes into a single DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined DataFrame to a new CSV file
output_file = 'C:/Users/valentinosko/frsa/output_combined_file.csv'
combined_df.to_csv(output_file, index=False, header=False, encoding='UTF-16')  # Ensure consistent encoding

print("All CSV files have been concatenated and saved to:", output_file)

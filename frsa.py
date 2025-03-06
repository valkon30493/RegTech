import os
import pandas as pd

# Folder path where your CSV files are located
folder_path = 'C:/Users/valentinosko/frsa'

# Phrase to insert in the first cell of the new row
insert_phrase = "INSERT INTO @temp_cid_list (CID) VALUES"

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path, header=None, encoding='UTF-16')

        # Insert a new row at the top with the phrase in the first cell
        new_row = pd.DataFrame([[insert_phrase]], columns=df.columns)  # Create a DataFrame with the phrase
        df = pd.concat([new_row, df], ignore_index=True)  # Concatenate the new row at the top

        # Save the modified DataFrame back to the CSV file
        df.to_csv(file_path, index=False, header=False, encoding='UTF-16')

print("New row added with the phrase in the first cell of all CSV files in the folder.")
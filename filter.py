# Description: Filter rows from a CSV file based on a condition
import pandas as pd

# Load the CSV file
input_file = 'merged_data.csv'  # replace with your input CSV file path
output_file = 'Smithfield_notifier.csv'  # output file for filtered rows

# Read the CSV file into a DataFrame
df = pd.read_csv(input_file)

# Filter rows where 'Workflow' column is 'ready_to_pickup'
filtered_df = df[df['Workflow'] == 'notifier']

# Write the filtered rows to a new CSV file
filtered_df.to_csv(output_file, index=False)

# print(f"Filtered rows saved to {output_file}")

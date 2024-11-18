# Column 'NOTES/COMMENTS' is updated based on the condition of the 'Workflow' column.
import pandas as pd

# Load the CSV file
file_path = 'match2.csv'  # Replace with the path to your CSV file
df = pd.read_csv(file_path)

# Update 'NOTES/COMMENTS' based on 'Workflow' column condition
df['NOTES/COMMENTS'] = df.apply(
    lambda row: 'Email sent and response processed' if row['Workflow_y'] == 'shipment_update' else row['NOTES/COMMENTS'],
    axis=1
)

# Save the updated DataFrame back to CSV
df.to_csv(file_path, index=False)

print("CSV file updated successfully.")

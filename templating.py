

import pandas as pd
from openpyxl import load_workbook

def replace_master_data(file_path, df, output_file_path=None):
    """
    Replace all data in the 'Master Data' sheet with a pandas DataFrame, while keeping other sheets intact.

    :param file_path: Path to the existing Excel file.
    :param df: Pandas DataFrame containing the new data to replace in the 'Master Data' sheet.
    :param output_file_path: Path to save the updated file. If None, overwrites the original file.
    """
    try:
        save_path = output_file_path if output_file_path else file_path
        
        # Load the existing workbook
        workbook = load_workbook(file_path)
        
        # Check if 'master data' sheet exists and remove it
        if 'master data' in workbook.sheetnames:
            del workbook['master data']
        
        # Create a new sheet for 'Master Data'
        master_data_sheet = workbook.create_sheet('master data')
        
        # Write DataFrame content to the new sheet
        for row_idx, row_data in enumerate(df.itertuples(index=False, name=None), start=1):
            for col_idx, value in enumerate(row_data, start=1):
                master_data_sheet.cell(row=row_idx + 1, column=col_idx, value=value)
        
        # Add headers
        for col_idx, column_name in enumerate(df.columns, start=1):
            master_data_sheet.cell(row=1, column=col_idx, value=column_name)
        
        # Save the workbook
        workbook.save(save_path)
        
        print(f"Data in 'Master Data' replaced successfully in '{save_path}'.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
file_path = "workflow-template.xlsx"  # Input Excel file
output_file_path = None  # Optional: Path to save the updated file (None means overwrite the original)

# Create DataFrame from a CSV file
csv_file_path = "workflow.csv"  # Path to the CSV file
df = pd.read_csv(csv_file_path)

replace_master_data(file_path, df, output_file_path)

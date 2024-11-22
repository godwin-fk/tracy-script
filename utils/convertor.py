import pandas as pd

def clean_excel(file_path: str, output_path: str):
    """
    Cleans the Excel file by dropping rows where the first column has specific unwanted values.
    
    Parameters:
    - file_path: str - Path to the input Excel file.
    - output_path: str - Path to save the cleaned Excel file.
    """
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Get the name of the first column
    first_col = df.columns[0]

    # Drop rows where the first column is empty or contains specific unwanted values
    cleaned_df = df[
        ~(df[first_col].isna() | df[first_col].str.strip().isin(['PLANT:', 'SHIPMENT NUMBER']))
    ]

    # Save the cleaned DataFrame to a new Excel file
    cleaned_df.to_excel(output_path, index=False)
    print(f"Cleaned Excel file saved to: {output_path}")

# Example usage
input_file = "/Users/bhanu.teja/tracy-script/utils/merged_data.xlsx" 
output_file = "/Users/bhanu.teja/tracy-script/utils/output_data.xlsx"  
clean_excel(input_file, output_file)

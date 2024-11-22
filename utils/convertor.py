import pandas as pd

def clean_excel(file_path: str, output_path: str):
    """
    Cleans the Excel file by dropping rows where the first column value cannot be converted to an integer.
    
    Parameters:
    - file_path: str - Path to the input Excel file.
    - output_path: str - Path to save the cleaned Excel file.
    """
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Get the name of the first column
    first_col = df.columns[0]

    def is_convertible(value):
        """
        Checks if the value can be converted to an integer.
        Returns True if convertible, False otherwise.
        """
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    # Filter rows where the first column value is convertible to int
    cleaned_df = df[df[first_col].apply(is_convertible)]

    # Save the cleaned DataFrame to a new Excel file
    cleaned_df.to_excel(output_path, index=False)
    print(f"Cleaned Excel file saved to: {output_path}")

# Example usage
input_file = "/Users/bhanu.teja/tracy-script/utils/Merged file (8).xlsx" 
output_file = "/Users/bhanu.teja/tracy-script/utils/output_data.xlsx"  
clean_excel(input_file, output_file)

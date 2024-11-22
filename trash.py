import pandas as pd

def format_date_column(input_file: str, output_file: str):
    """
    Reads a CSV file, updates the 'date' column to display in 'DD-MMM' format,
    and saves the updated CSV.

    :param input_file: Path to the input CSV file
    :param output_file: Path to save the updated CSV file
    """
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)

        # Ensure the required column exists
        if 'date' not in df.columns:
            raise ValueError("The required column 'date' is not present in the CSV.")

        # Update the 'date' column to 'DD-MMM' format
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%d-%b')

        # Save the updated DataFrame to a new CSV
        df.to_csv(output_file, index=False)
        print(f"Updated CSV saved successfully to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Specify the input and output file paths
    input_csv = "input.csv"  # Replace with your input CSV file path
    output_csv = "data2.csv"  # Replace with your desired output file path

    # Process the CSV
    format_date_column(input_csv, output_csv)

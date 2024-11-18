import pandas as pd

def join_csv_files(file1, file2, output_file, join_column='Load Number', how='inner'):
    # Load the CSV files into DataFrames
    df1 = pd.read_csv(file1,dtype={join_column: 'Int64'})
    df2 = pd.read_csv(file2,dtype={join_column: 'Int64'})
    df2 = df2.drop_duplicates(subset='Load Number', keep='first')

    # Perform the join on the specified column
    result = pd.merge(df1, df2, on=join_column, how=how)

    # Save the result to a new CSV file
    result.to_csv(output_file, index=False)
    print(f"Joined file saved as: {output_file}")

# Replace 'file1.csv' and 'file2.csv' with the actual file names
join_csv_files('Smithfield_notifier.csv', 'Smithfield_shipment_update.csv', 'notifier_JOIN_shipment_update.csv')

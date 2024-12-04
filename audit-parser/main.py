import os
import json
from audit_parser import Parser

def process_json_files(directory_path):
    
    output_directory = os.path.join(directory_path, "output")
    os.makedirs(output_directory, exist_ok=True)
    
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.json'):
            file_path = os.path.join(directory_path, file_name)
            print(f"Processing: {file_path}")

            parser = Parser(file_path)
            parsed_output = parser.parse_workflow()

            output_file = os.path.join(output_directory, f"{os.path.splitext(file_name)[0]}_output.json")
            with open(output_file, 'w') as output:
                json.dump(parsed_output, output, indent=4)

            print(f"Processed: {file_path}, Output saved to: {output_file}")

if __name__ == "__main__":

    directory = "logs"
    process_json_files(directory)

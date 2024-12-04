import os
import json
from parser import Parser

def process_json_files(log_dir):
    output_dir = os.path.join("output")
    os.makedirs(output_dir, exist_ok=True)

    for file_name in os.listdir(log_dir):
        if file_name.endswith('.json'):
            file_path = os.path.join(log_dir, file_name)
            print(f"\nProcessing: {file_path}")

            parser = Parser(file_path)
            parsed_output = parser.parse_workflow()

            output_file = os.path.join(output_dir, f"{os.path.splitext(file_name)[0]}_output.json")
            with open(output_file, 'w') as output:
                json.dump(parsed_output, output, indent=4)

            print(f"Output Saved: {output_file}\n")

if __name__ == "__main__":
    directory = "logs"
    process_json_files(directory)

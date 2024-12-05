import os
import json
import unittest
from parser import Parser

class ParserTestCase(unittest.TestCase):
    # output will show as Ran 1 test but will actually run multiple tests
    # if any test fails, it will show as x failed test
    def test_parse_followup_classifier(self):
        self.maxDiff = None
        input_directory = os.path.join("test", "run-logs")
        output_directory = os.path.join("test", "expected-output")

        # List all files in the input directory
        input_files = [f for f in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, f))]

        def normalize_data(data):
            if isinstance(data, dict):
                return {k: normalize_data(v) for k, v in data.items() if k not in {"id",}}
            elif isinstance(data, list):
                return [normalize_data(item) for item in data]
            else:
                return data

        # Iterate through all input files
        for input_file in input_files:
            print(f"\nProcessing: {input_file}")
            input_file_path = os.path.join(input_directory, input_file)

            base_name, _ = os.path.splitext(input_file)
            output_file_name = f"{base_name}_output.json"
            output_file_path = os.path.join(output_directory, output_file_name)

            with self.subTest(input_file=input_file):
                parser = Parser(input_file_path)
                parsed_output = parser.parse_workflow()

                with open(output_file_path, "r") as file:
                    expected_result = json.load(file)

                self.assertEqual(
                    normalize_data(parsed_output),
                    normalize_data(expected_result),
                    msg=f"Mismatch for file: {input_file}"
                )

if __name__ == "__main__":
    unittest.main()

import os
import json
import unittest
from parser import Parser

class ParserTestCase(unittest.TestCase):
    
    def test_parse_followup_classifier(self):
        self.maxDiff = None
        input_file_path = os.path.join("test-input", "run-logs", "followup-classifier.json")
        
        output_file_path = os.path.join("test-input", "expected-output", "followup-classifier_output.json")
        parser = Parser(input_file_path)
        parsed_output = parser.parse_workflow()
        
        with open(output_file_path, "r") as file:
            expected_result = json.load(file)
        
        def normalize_data(data):
            if isinstance(data, dict):
                return {k: normalize_data(v) for k, v in data.items() if k not in {"id", "timestamp"}}
            elif isinstance(data, list):
                return [normalize_data(item) for item in data]
            else:
                return data

        self.assertEqual(normalize_data(parsed_output), normalize_data(expected_result))

if __name__ == "__main__":
    unittest.main()
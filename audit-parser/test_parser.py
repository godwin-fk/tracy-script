import os
import json
import unittest
from parser import Parser

class ParserTestCase(unittest.TestCase):

    def test_parse_followup_classifier(self):
        input_file_path = os.path.join("audit-parser", "test-input", "run-logs", "followup-classifier.json")
        output_file_path = os.path.join("audit-parser", "test-input", "expected-output", "followup-classifier_output.json")
        parser = Parser(input_file_path)

        with open(output_file_path, "r") as file:
            expected_result = json.load(file)

        self.assertEqual(parser.parse_workflow(), expected_result)

if __name__ == "__main__":
    unittest.main()
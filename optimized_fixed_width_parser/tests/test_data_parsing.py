import unittest
import os
import tempfile
import csv
import logging
from data_parsing import parse_fixed_width_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class TestParseFixedWidthFile(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory and file for testing."""
        self.test_dir = tempfile.TemporaryDirectory()
        self.config = {
            "ColumnNames": ["f1", "f2", "f3", "f4"],
            "Offsets": ["5", "10", "5", "5"],  # Adjusted offsets based on your data
            "FixedWidthEncoding": "utf-8",
            "DelimitedEncoding": "utf-8",
            "IncludeHeader": "True",
            "input_file": os.path.join(self.test_dir.name, "input.txt"),
            "output_file": os.path.join(self.test_dir.name, "output.csv")
        }

    def tearDown(self):
        """Clean up the temporary directory after tests."""
        self.test_dir.cleanup()

    def test_parse_fixed_width_file(self):
        """Test parsing a standard fixed-width file."""
        try:
            # Prepare a sample fixed-width file
            with open(self.config['input_file'], 'w', encoding=self.config['FixedWidthEncoding']) as f:
                f.write("ABCDE1234567890123456\n")  # 5 + 10 + 5 + (last 5 will be ignored)
                f.write("XYZ  1234567890123456\n")  # Matching the offsets

            parse_fixed_width_file(self.config['input_file'], self.config['output_file'], self.config)

            # Validate the output CSV
            with open(self.config['output_file'], 'r', encoding=self.config['DelimitedEncoding']) as f:
                reader = csv.reader(f)
                rows = list(reader)
                self.assertEqual(rows[0], ["f1", "f2", "f3", "f4"])  # Header validation
                self.assertEqual(rows[1], ["ABCDE", "1234567890", "12345", "6"])  # Adjusted based on correct offsets
                self.assertEqual(rows[2], ["XYZ", "1234567890", "12345", "6"])  # Adjusted for second row

        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    def test_empty_file(self):
        """Test parsing an empty input file."""
        try:
            # Create an empty file
            open(self.config['input_file'], 'w').close()

            parse_fixed_width_file(self.config['input_file'], self.config['output_file'], self.config)

            # Validate that the output file has only the header
            with open(self.config['output_file'], 'r', encoding=self.config['DelimitedEncoding']) as f:
                reader = csv.reader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 1)  # Only header should be present
                self.assertEqual(rows[0], ["f1", "f2", "f3", "f4"])  # Check header

        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    def test_large_file(self):
        """Test parsing a large fixed-width file."""
        try:
            # Generate a large file
            with open(self.config['input_file'], 'w', encoding=self.config['FixedWidthEncoding']) as f:
                for i in range(100000):  # Write 100,000 lines
                    f.write("ABCDE1234567890123456\n")

            parse_fixed_width_file(self.config['input_file'], self.config['output_file'], self.config)

            # Check if the file was parsed correctly
            with open(self.config['output_file'], 'r', encoding=self.config['DelimitedEncoding']) as f:
                reader = csv.reader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 100001)  # 100,000 records + 1 header

        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    def test_incorrect_encoding(self):
        """Test how the parser handles incorrect encoding."""
        try:
            # Write some non-ASCII characters using 'latin-1' encoding
            with open(self.config['input_file'], 'w', encoding='latin-1') as f:
                f.write("ABCDE12345678901234é6\n")  # The 'é' character will cause a decoding issue in utf-8

            with self.assertRaises(UnicodeDecodeError):
                parse_fixed_width_file(self.config['input_file'], self.config['output_file'], self.config)

        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    def test_missing_columns(self):
        """Test with a file that does not match the expected columns."""
        try:
            # Write a file with missing columns
            with open(self.config['input_file'], 'w', encoding=self.config['FixedWidthEncoding']) as f:
                f.write("ABC123456\n")  # Less data than expected

            parse_fixed_width_file(self.config['input_file'], self.config['output_file'], self.config)

            # Check if the parser handled the missing columns gracefully
            with open(self.config['output_file'], 'r', encoding=self.config['DelimitedEncoding']) as f:
                reader = csv.reader(f)
                rows = list(reader)
                # Ensure the output has empty strings for the missing columns
                self.assertEqual(len(rows), 2)  # 1 row + 1 header
                self.assertEqual(rows[1], ["ABC12", "3456", "", ""])  # Adjusted based on input and offsets

        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

if __name__ == "__main__":
    unittest.main()
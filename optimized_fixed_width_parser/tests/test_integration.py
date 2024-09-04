import unittest
import logging
import os
from data_generation import generate_fixed_width_file
from data_parsing import parse_fixed_width_file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_integration.log"),
        logging.StreamHandler()
    ]
)

class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = {
            "Offsets": ["5", "12", "3", "2", "13", "7", "10", "13", "20", "13"],
            "FixedWidthEncoding": "windows-1252",
            "DelimitedEncoding": "utf-8",
            "ColumnNames": ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"],
            "IncludeHeader": "True"
        }
        cls.input_file = 'test_fixed_width_data.txt'
        cls.output_file = 'test_output_data.csv'

    def test_full_process(self):
        try:
            # Generate the fixed-width file
            generate_fixed_width_file(self.input_file, self.config, num_records=100)
            # Parse the fixed-width file
            parse_fixed_width_file(self.input_file, self.output_file, self.config)
            self.assertTrue(os.path.exists(self.output_file))
            file_size = os.path.getsize(self.output_file)
            self.assertGreater(file_size, 0, "The CSV file should not be empty.")
            logging.info("Integration test passed: Generation and parsing executed successfully.")
        except Exception as e:
            logging.error(f"Integration test failed: {str(e)}")
            self.fail(f"Integration test failed due to {str(e)}")
        finally:
            if os.path.exists(self.input_file):
                os.remove(self.input_file)
            if os.path.exists(self.output_file):
                os.remove(self.output_file)

if __name__ == "__main__":
    unittest.main()

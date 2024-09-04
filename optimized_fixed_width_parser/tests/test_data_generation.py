import unittest
import os
import logging
from data_generation import generate_fixed_width_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class TestGeneration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = {
            "Offsets": ["5", "12", "3", "2", "13"],
            "FixedWidthEncoding": "utf-8",
            "input_file": "generated_data.txt",
            "num_records": 10
        }

    def test_generate_file_success(self):
        try:
            generate_fixed_width_file(self.config["input_file"], self.config, self.config["num_records"])
            self.assertTrue(os.path.exists(self.config["input_file"]), "Generated file should exist")

            with open(self.config["input_file"], 'r') as f:
                lines = f.readlines()
                self.assertEqual(len(lines), self.config["num_records"], "Generated file should contain the correct number of records")
        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    def test_generate_file_large_records(self):
        try:
            large_num_records = 100000
            large_config = self.config.copy()
            large_config["num_records"] = large_num_records

            generate_fixed_width_file(self.config["input_file"], large_config, large_num_records)
            self.assertTrue(os.path.exists(self.config["input_file"]), "Generated file should exist")

            with open(self.config["input_file"], 'r') as f:
                lines = f.readlines()
                self.assertEqual(len(lines), large_num_records, "Generated file should contain the correct number of records")
        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    def test_generate_file_no_records(self):
        try:
            no_records_config = self.config.copy()
            no_records_config["num_records"] = 0

            generate_fixed_width_file(self.config["input_file"], no_records_config, 0)
            self.assertTrue(os.path.exists(self.config["input_file"]), "Generated file should exist")

            with open(self.config["input_file"], 'r') as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 0, "Generated file should be empty")
        except Exception as e:
            logging.error(f"Test failed: {e}")
            self.fail(f"Test failed due to an exception: {e}")

    @classmethod
    def tearDownClass(cls):
        try:
            if os.path.exists(cls.config["input_file"]):
                os.remove(cls.config["input_file"])
        except Exception as e:
            logging.error(f"Failed to clean up file: {e}")

if __name__ == "__main__":
    unittest.main()

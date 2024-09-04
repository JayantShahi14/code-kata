# test/test_data_anonymization.py

import unittest
import logging
from pyspark.sql import SparkSession
from src.data_anonymizer import DataAnonymizer
from src.config import *
import os

# Set up logging for testing
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestDataAnonymization(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("Test Data Anonymization") \
            .getOrCreate()
        self.anonymizer = DataAnonymizer(self.spark)

        # Create a sample input file for testing
        with open(FILENAME, 'w') as f:
            f.write("id,name\n1,John Doe\n2,Jane Doe\n")

    def tearDown(self):
        self.spark.stop()
        # Remove files if they exist
        for file in [FILENAME, OUTPUT_FILE]:
            try:
                if os.path.exists(file):
                    os.remove(file)
            except PermissionError as e:
                logging.error(f"Error during teardown: {e}")

    def test_successful_generation_and_anonymization(self):
        self.anonymizer.anonymize_data(FILENAME, OUTPUT_FILE)
        self.assertTrue(os.path.exists(OUTPUT_FILE))
        logging.info("Data generation and anonymization successful.")

    def test_error_handling_for_non_existent_input_file(self):
        with self.assertLogs(level='ERROR') as cm:
            self.anonymizer.anonymize_data('non_existent_file.csv', OUTPUT_FILE)
        self.assertIn('Input file non_existent_file.csv does not exist.', cm.output[0])
        logging.error("Handled non-existent input file error.")

    def test_error_handling_for_insufficient_disk_space(self):
        # Simulate insufficient disk space by attempting to write to a file that already exists
        with open(OUTPUT_FILE, 'wb') as f:
            f.write(b'0' * (1 * 1024 * 1024))  # Create a 1 MB file

        with self.assertLogs(level='ERROR') as cm:
            self.anonymizer.anonymize_data(FILENAME, OUTPUT_FILE)
        self.assertIn('An error occurred during data anonymization:', cm.output[0])
        logging.error("Handled insufficient disk space error.")

    def test_error_handling_for_output_directory(self):
        # Create a directory with the same name as the output file
        os.makedirs(OUTPUT_FILE, exist_ok=True)

        with self.assertLogs(level='ERROR') as cm:
            self.anonymizer.anonymize_data(FILENAME, OUTPUT_FILE)
        self.assertIn(f"Output file path {OUTPUT_FILE} is a directory.", cm.output[0])
        logging.error("Handled output directory error.")

if __name__ == "__main__":
    unittest.main()
# test/test_data_generation.py

import unittest
import logging
from pyspark.sql import SparkSession
from src.data_generator import DataGenerator
from src.config import *
import os
import shutil

# Set up logging for testing
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestDataGeneration(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("Test Data Generation") \
            .getOrCreate()
        self.generator = DataGenerator(self.spark)

    def tearDown(self):
        self.spark.stop()
        if os.path.exists(FILENAME):
            shutil.rmtree(FILENAME)  # Remove directory if exists

    def test_data_generation_with_zero_target_size(self):
        with self.assertLogs(level='INFO') as cm:
            self.generator.create_large_csv(FILENAME, 0)
        self.assertIn('No data generated with target size of 0MB.', cm.output[0])
        logging.info("Handled zero target size successfully.")

    def test_data_generation_with_negative_target_size(self):
        with self.assertLogs(level='ERROR') as cm:
            self.generator.create_large_csv(FILENAME, -100)
        self.assertIn('No data generated with target size of -100MB.', cm.output[0])
        logging.error("Handled negative target size error.")

    def test_data_generation_with_very_small_batch_size(self):
        global BATCH_SIZE
        original_batch_size = BATCH_SIZE
        try:
            BATCH_SIZE = 1  # Set to a very small value
            self.generator.create_large_csv(FILENAME, TARGET_SIZE_MB)
            logging.info("Data generation with very small batch size successful.")
        finally:
            BATCH_SIZE = original_batch_size  # Restore original value

    def test_data_generation_with_empty_dataframe(self):
        # Modify the generate_records method to return an empty list
        self.generator.generate_records = lambda num_records: []

        with self.assertLogs(level='ERROR') as cm:
            self.generator.create_large_csv(FILENAME, TARGET_SIZE_MB)
        self.assertIn('Generated DataFrame is empty.', cm.output[0])
        logging.error("Handled empty DataFrame scenario.")

    def test_error_handling_for_invalid_configuration_values(self):
        global ESTIMATED_RECORD_SIZE, BATCH_SIZE
        original_estimated_record_size = ESTIMATED_RECORD_SIZE
        original_batch_size = BATCH_SIZE

        try:
            # Set invalid configuration values
            ESTIMATED_RECORD_SIZE = -1
            BATCH_SIZE = -1

            with self.assertLogs(level='ERROR') as cm:
                self.generator.create_large_csv(FILENAME, TARGET_SIZE_MB)
            self.assertIn('Invalid configuration values.', cm.output[0])
            logging.error("Handled invalid configuration values error.")
        finally:
            # Restore original values
            ESTIMATED_RECORD_SIZE = original_estimated_record_size
            BATCH_SIZE = original_batch_size

    def test_data_generation_with_very_large_target_size(self):
        with self.assertLogs(level='INFO') as cm:
            self.generator.create_large_csv(FILENAME, 1000 * 1024)  # 1000 GB
        self.assertIn('Handling very large target size.', cm.output[0])
        logging.info("Handled very large target size gracefully.")

    def test_data_generation_with_very_large_batch_size(self):
        global BATCH_SIZE
        original_batch_size = BATCH_SIZE
        try:
            BATCH_SIZE = 10000  # Set to a very large value
            self.generator.create_large_csv(FILENAME, TARGET_SIZE_MB)
            logging.info("Data generation with very large batch size successful.")
        finally:
            BATCH_SIZE = original_batch_size  # Restore original value

if __name__ == "__main__":
    unittest.main()
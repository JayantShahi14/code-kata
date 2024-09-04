# src/data_generator.py

from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import Row
import logging
from config import *  # Importing configuration values

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataGenerator:
    def __init__(self, spark):
        self.fake = Faker()
        self.spark = spark

    def random_date_of_birth(self, min_age=18, max_age=80):
        """Generate a random date of birth within the specified age range."""
        today = datetime.today()
        start_date = today - timedelta(days=max_age * 365)
        end_date = today - timedelta(days=min_age * 365)
        return self.fake.date_between(start_date=start_date, end_date=end_date).isoformat()

    def generate_records(self, num_records):
        """Generate a batch of random data records using PySpark."""
        rows = []
        for _ in range(num_records):
            rows.append(Row(
                first_name=self.fake.first_name(),
                last_name=self.fake.last_name(),
                address=self.fake.address().replace('\n', ', '),
                birth_date=self.random_date_of_birth()
            ))
        return rows

    def create_large_csv(self, file_name, target_size_mb):
        """Create a large CSV file with random data using PySpark."""
        if target_size_mb < 0:
            logging.error(f"No data generated with target size of {target_size_mb}MB.")
            return

        if target_size_mb == 0:
            logging.info(f"No data generated with target size of {target_size_mb}MB.")
            return

        if target_size_mb > 1024 * 1024:  # More than 1 TB
            logging.info("Handling very large target size.")

        if ESTIMATED_RECORD_SIZE <= 0 or BATCH_SIZE <= 0:
            logging.error("Invalid configuration values.")
            return

        num_records = (target_size_mb * 1024 * 1024) // ESTIMATED_RECORD_SIZE

        try:
            # Generate data in batches and create a DataFrame
            all_rows = []
            for i in range(0, num_records, BATCH_SIZE):
                batch_size = min(BATCH_SIZE, num_records - i)
                rows = self.generate_records(batch_size)
                all_rows.extend(rows)  # Collect all rows

            # Create a DataFrame from the collected rows
            if not all_rows:  # Check if all_rows is empty
                logging.error("Generated DataFrame is empty.")
                return

            df = self.spark.createDataFrame(all_rows)

            # Write the DataFrame to a CSV file
            df.write.mode("overwrite").csv(file_name, header=True)
            logging.info(f"Generated approximately {target_size_mb}MB of data in {file_name}")

        except IOError as e:
            logging.error(f"Failed to write CSV file: {e}")
        except Exception as e:
            logging.error(f"An error occurred during CSV generation: {e}")
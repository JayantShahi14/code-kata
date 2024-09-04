# src/data_anonymizer.py

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataAnonymizer:
    def __init__(self, spark):
        self.spark = spark

    def anonymize_data(self, input_file, output_file):
        """Anonymize data in a CSV file using PySpark."""
        if not os.path.exists(input_file):
            logging.error(f"Input file {input_file} does not exist.")
            return

        try:
            df = self.spark.read.csv(input_file, header=True)

            # Anonymize the data using withColumn and lit
            anonymized_df = df.withColumn('first_name', lit('FirstName')) \
                .withColumn('last_name', lit('LastName')) \
                .withColumn('address', lit('Address'))

            # Check if output_file is a directory
            if os.path.isdir(output_file):
                logging.error(f"Output file path {output_file} is a directory.")
                return

            # Check if we can write to the output file (simulate disk space)
            if os.path.exists(output_file):
                os.remove(output_file)  # Remove existing file if it exists

            # Attempt to write the DataFrame to the output file
            anonymized_df.write.mode("overwrite").csv(output_file, header=True)
            logging.info(f"Anonymization complete. Data written to {output_file}.")

        except Exception as e:
            logging.error(f"An error occurred during data anonymization: {e}")
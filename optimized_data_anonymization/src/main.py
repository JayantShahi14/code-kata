# src/main.py

import argparse
import logging
from pyspark.sql import SparkSession
from data_generator import DataGenerator
from data_anonymizer import DataAnonymizer
from config import *  # Importing configuration values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_processing.log"),
        logging.StreamHandler()
    ]
)

def main():
    parser = argparse.ArgumentParser(description='Data Generator and Anonymizer')
    parser.add_argument('--target_size_mb', type=int, default=TARGET_SIZE_MB, help='Target size of the CSV file in MB')
    parser.add_argument('--output_file', type=str, default=OUTPUT_FILE, help='Output CSV file name')
    args = parser.parse_args()

    try:
        logging.info("Starting Spark session")
        spark = SparkSession.builder \
            .appName("Data Generator and Anonymizer") \
            .getOrCreate()

        logging.info("Initializing DataGenerator")
        generator = DataGenerator(spark)

        logging.info(f"Creating large CSV file with target size: {args.target_size_mb} MB")
        generator.create_large_csv(FILENAME, args.target_size_mb)

        logging.info("Initializing DataAnonymizer")
        anonymizer = DataAnonymizer(spark)

        logging.info(f"Anonymizing data and saving to output file: {args.output_file}")
        anonymizer.anonymize_data(FILENAME, args.output_file)

        logging.info("Data generation and anonymization completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        logging.info("Stopping Spark session")
        spark.stop()

if __name__ == "__main__":
    main()

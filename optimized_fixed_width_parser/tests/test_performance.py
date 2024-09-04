import unittest
import logging
import os
import time
from data_generation import generate_fixed_width_file
from data_parsing import parse_fixed_width_file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_performance.log"),
        logging.StreamHandler()
    ]
)

class TestPerformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = {
            "Offsets": ["5", "12", "3", "2", "13", "7", "10", "13", "20", "13"],
            "FixedWidthEncoding": "windows-1252",
            "DelimitedEncoding": "utf-8",
            "ColumnNames": ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"],
            "IncludeHeader": "True"
        }
        cls.results = []

    def measure_performance(self, num_records):
        try:
            input_file = f'test_fixed_width_data_{num_records}.txt'
            output_file = f'test_output_data_{num_records}.csv'

            # Measure generation time
            start_time = time.time()
            generate_fixed_width_file(input_file, self.config, num_records=num_records, num_processes=4)
            gen_time = time.time() - start_time

            # Measure parsing time
            start_time = time.time()
            parse_fixed_width_file(input_file, output_file, self.config)
            par_time = time.time() - start_time

            # Record the results
            self.results.append({
                'num_records': num_records,
                'gen_time': gen_time,
                'par_time': par_time
            })
            logging.info(f"Performance test for {num_records} records: Generation took {gen_time:.2f} seconds, Parsing took {par_time:.2f} seconds.")

            # Clean up files
            if os.path.exists(input_file):
                os.remove(input_file)
            if os.path.exists(output_file):
                os.remove(output_file)

        except Exception as e:
            logging.error(f"Performance test failed for {num_records} records: {str(e)}")
            self.fail(f"Performance test failed due to {str(e)}")

    def test_performance_small(self):
        self.measure_performance(10000)

    def test_performance_large(self):
        self.measure_performance(100000)

    def test_performance_extra_large(self):
        self.measure_performance(1000000)

    @classmethod
    def tearDownClass(cls):
        # Log the performance results
        with open('performance_results.log', 'w') as f:
            for result in cls.results:
                f.write(f"Records: {result['num_records']}, Generation Time: {result['gen_time']:.2f} seconds, Parsing Time: {result['par_time']:.2f} seconds\n")
        logging.info("Performance tests completed. Results logged in 'performance_results.log'.")

if __name__ == "__main__":
    unittest.main()

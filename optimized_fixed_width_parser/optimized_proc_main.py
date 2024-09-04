import argparse
import json
import logging
from data_generation import generate_fixed_width_file
from data_parsing import parse_fixed_width_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("file_processing.log"),
        logging.StreamHandler()
    ]
)

def load_config(config_file):
    """Load configuration from a JSON file."""
    with open(config_file, 'r') as f:
        config = json.load(f)
    logging.info(f"Configuration loaded successfully from '{config_file}'.")
    return config

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Generate and parse fixed-width files.')
    parser.add_argument('--config', required=True, help='Path to the configuration file (JSON).')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Generate fixed-width file
    logging.info("Starting file generation...")
    generate_fixed_width_file(
        'fixed_width_data.txt',
        config,
        num_records=config['num_records'],
        num_processes=config.get('num_processes', None)
    )

    # Parse fixed-width file
    logging.info("Starting file parsing...")
    # Parse fixed-width file
    parse_fixed_width_file(config['input_file'], config['output_file'], config)

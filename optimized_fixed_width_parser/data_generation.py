import random
import logging
import time
from multiprocessing import Pool, cpu_count

def generate_random_field(width):
    """Generate a random string of fixed width."""
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ', k=width)).strip()

def generate_random_record(width_spec):
    """Generate a single random record based on width specifications."""
    return ''.join(generate_random_field(width) for width in width_spec)

def generate_fixed_width_file(file_name, spec, num_records, num_processes=None):
    """Generate a fixed-width file based on the provided specifications using multiprocessing."""
    start_time = time.time()

    # Use the number of CPU cores if num_processes is not provided
    if num_processes is None:
        num_processes = cpu_count()

    width_spec = list(map(int, spec['Offsets']))

    # Use multiprocessing to generate records in parallel
    with Pool(num_processes) as pool:
        records = pool.map(generate_random_record, [width_spec] * num_records)

    # Write the generated records to the file
    with open(file_name, 'w', encoding=spec['FixedWidthEncoding']) as f:
        for record in records:
            f.write(record + '\n')

    elapsed_time = time.time() - start_time
    logging.info(f"File '{file_name}' generated successfully with {num_records} records in {elapsed_time:.2f} seconds using {num_processes} processes.")

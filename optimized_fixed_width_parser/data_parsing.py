import csv
import logging
import time

def parse_fixed_width_file(input_file, output_file, spec):
    """Parse a fixed-width file and generate a delimited file (CSV)."""
    start_time = time.time()
    width_spec = list(map(int, spec['Offsets']))
    total_width = sum(width_spec)

    try:
        with open(input_file, 'r', encoding=spec['FixedWidthEncoding']) as f_in:
            lines = f_in.readlines()

        with open(output_file, 'w', newline='', encoding=spec['DelimitedEncoding']) as f_out:
            writer = csv.writer(f_out)

            if spec['IncludeHeader'].lower() == 'true':
                writer.writerow(spec['ColumnNames'])

            for line in lines:
                line = line.rstrip('\n')
                if len(line) < total_width:
                    line = line.ljust(total_width)
                elif len(line) > total_width:
                    line = line[:total_width]

                start = 0
                row = []
                for width in width_spec:
                    field = line[start:start + width].strip()  # Ensure we strip whitespace
                    row.append(field)
                    start += width

                writer.writerow(row)

    except UnicodeDecodeError as e:
        logging.error(f"Unicode decoding error encountered: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

    elapsed_time = time.time() - start_time
    logging.info(f"File '{input_file}' parsed successfully to '{output_file}' in {elapsed_time:.2f} seconds.")
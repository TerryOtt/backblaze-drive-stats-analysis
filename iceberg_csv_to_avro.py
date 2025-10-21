import argparse
import csv
import time
import typing

import fastavro


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Convert Iceberg CSV to Avro')
    parser.add_argument('input_csv', help='Path to CSV created from Iceberg table')
    parser.add_argument('output_avro', help='Path to output Avro file')
    return parser.parse_args()


def _update_row_to_avro_schema(csv_row: dict[str, str]) -> dict[str, str | int | bool]:
    avro_row: dict[str, str | int | bool] = {
        'date'              : csv_row['date'],
        'serial_number'     : csv_row['serial_number'],
        'model'             : csv_row['model'],
        'capacity_bytes'    : int(csv_row['capacity_bytes']),
    }

    # Need to turn failure column from in to bool to meet our Avro schema
    if int(csv_row['failure']) == 1:
        avro_row['failure'] = True
    else:
        avro_row['failure'] = False

    return avro_row


def _csv_row_iter(args: argparse.Namespace) -> typing.Generator[dict[str, str | int | bool], None, None]:
    with open(args.input_csv, newline='') as csvfile:
        backblaze_reader = csv.DictReader(csvfile)
        for row in backblaze_reader:
            yield _update_row_to_avro_schema(row)


def _write_avro_data(args: argparse.Namespace) -> None:
    avro_schema = {
        'type': 'record',
        'name': 'BackblazeDailyDriveStatsRecord',
        'fields': [
            {
                'name': 'date',
                'type': 'string',
            },

            {
                'name': 'serial_number',
                'type': 'string',
            },

            {
                'name': 'model',
                'type': 'string',
            },

            {
                'name': 'capacity_bytes',
                'type': 'long',
            },

            {
                'name': 'failure',
                'type': 'boolean',
            },
        ]
    }

    avro_schema = fastavro.parse_schema(avro_schema)
    with open(args.output_avro, 'wb') as avro_handle:
        fastavro.writer(avro_handle, avro_schema, _csv_row_iter(args), codec='snappy')

def _main() -> None:
    args: argparse.Namespace = _parse_args()

    print( f"\nCreating Avro file...")
    print( f"\tOutput Avro file: \"{args.input_csv}\"")
    operation_start: float = time.perf_counter()
    operation_end: float = time.perf_counter()
    _write_avro_data( args )
    print(f"\tWrote Avro data in {operation_end - operation_start:.03f} seconds")


if __name__ == '__main__':
    _main()
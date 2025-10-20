import argparse
import csv
import json
import pyarrow.parquet

import parquet_to_pandas


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Convert Parquet to CSV')
    parser.add_argument('input_parquet', help='Path to Parquet created from Iceberg table')
    parser.add_argument('output_csv', help='Path to output CSV file')
    return parser.parse_args()


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    parquet_handle: pyarrow.parquet.ParquetFile = pyarrow.parquet.ParquetFile(args.input_parquet)
    #print(parquet_handle.schema)
    columns: list[str] = ['model', 'date', 'failure']
    batch_size: int = 4 * 1024 * 1024
    parquet_data: dict[str, dict[str, dict[str, int]]] = {}
    for curr_dataframe in parquet_to_pandas.iterate_parquet(parquet_handle, batch_size=batch_size, columns=columns):
        #print(curr_dataframe.head())
        for row in curr_dataframe.itertuples():
            row_model: str = row.model
            row_date: str = row.date.isoformat()
            row_failure: int = row.failure
            if row_model not in parquet_data:
                parquet_data[row_model] = {}
            if row_date not in parquet_data[row_model]:
                parquet_data[row_model][row_date] = {
                    'drive_count': 0,
                    'failure_count': 0,
                }

            parquet_data[row_model][row_date]['drive_count'] += 1
            parquet_data[row_model][row_date]['failure_count'] += row_failure

        break
    csv_columns: list[str] = ['model', 'date', 'drive_count', 'failure_count']
    with open(args.output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=csv_columns)

        csv_writer.writeheader()
        for curr_model in sorted(parquet_data):
            for curr_date in sorted(parquet_data[curr_model]):
                csv_row: dict[str, str | int ] = {
                    'model': curr_model,
                    'date': curr_date,
                    'drive_count': parquet_data[curr_model][curr_date]['drive_count'],
                    'failure_count': parquet_data[curr_model][curr_date]['failure_count'],
                }

                csv_writer.writerow(csv_row)

if __name__ == "__main__":
    _main()
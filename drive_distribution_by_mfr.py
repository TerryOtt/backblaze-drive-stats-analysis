import argparse
import json
import re

import pandas
import pyarrow.parquet


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Drive distribution by mfr')
    parser.add_argument('drive_model_to_mfr_json', help='Path to JSON file with drive model -> mfr data')
    parser.add_argument('parquet_file', help='Path to Parquet file')
    return parser.parse_args()


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    with open(args.drive_model_to_mfr_json, 'r') as mfr_handle:
        model_patterns_to_mfr:dict[str, str] = json.load(mfr_handle)
    parquet_handle: pyarrow.parquet.ParquetFile = pyarrow.parquet.ParquetFile(args.parquet_file)
    #print(parquet_handle.schema)
    columns: list[str] = ['date', 'model']
    batch_size: int = 4 * 1024 * 1024
    mfr_count_per_date: dict[str, dict[str, int]] = {}
    ignored_models: set[str] = set()
    drive_model_to_mfr_cache: dict[str, str] = {}
    for i, data_batch in enumerate(parquet_handle.iter_batches(batch_size=batch_size, columns=columns)):
        batch_dataframe: pandas.DataFrame = data_batch.to_pandas()
        #print( f"Processing batch {i+1:3d} with {len(batch_dataframe):9,} rows" )
        for row in batch_dataframe.itertuples():
            row_date: str = row.date.isoformat()
            row_model: str = row.model
            row_mfr: str | None = None

            # If we already know to skip it
            if row_model in ignored_models:
                continue

            # Have we found this exact model before?
            if row_model in drive_model_to_mfr_cache:
                row_mfr = drive_model_to_mfr_cache[row_model]
            else:
                # Iterate through match patterns
                got_pattern_match: bool = False
                for curr_pattern in model_patterns_to_mfr:
                    if re.search(curr_pattern, row_model):
                        got_pattern_match = True
                        row_mfr = model_patterns_to_mfr[curr_pattern]
                        # add to cache
                        drive_model_to_mfr_cache[row_model] = row_mfr
                        break
                if not got_pattern_match:
                    print(f"Skipping {row_model}")
                    ignored_models.add(row_model)
                    continue

            if row_date not in mfr_count_per_date:
                mfr_count_per_date[row_date] = {}
            if row_mfr not in mfr_count_per_date[row_date]:
                mfr_count_per_date[row_date][row_mfr] = 0

            mfr_count_per_date[row_date][row_mfr] += 1

    print(json.dumps(mfr_count_per_date, indent=4, sort_keys=True))




if __name__ == "__main__":
    _main()
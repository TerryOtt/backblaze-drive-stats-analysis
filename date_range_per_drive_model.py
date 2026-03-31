import argparse
import datetime
import json
import time
import polars

import backblaze_drive_stats_data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get first/most recent dates a drive has been seen")

    default_s3_endpoint: str = "https://s3.us-west-004.backblazeb2.com"
    default_b2_bucket_name: str = "drivestats-iceberg"
    default_b2_region: str = "us-west-004"
    default_table_path: str = "drivestats"

    parser.add_argument("--s3-endpoint",
                        default=default_s3_endpoint,
                        help=f"S3 Endpoint (default: \"{default_s3_endpoint}\")")

    parser.add_argument("--b2-region",
                        default=default_b2_region,
                        help=f"B2 Region (default: \"{default_b2_region}\")")

    parser.add_argument("--bucket-name",
                        default=default_b2_bucket_name,
                        help=f"B2 Bucket Name (default: \"{default_b2_bucket_name}\")")

    parser.add_argument("--table-path",
                        default=default_table_path,
                        help=f"B2 Bucket Table Path (default: \"{default_table_path}\")")

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')

    #parser.add_argument("input_parquet_file", help="Path to parquet file we read from")

    parser.add_argument("b2_access_key",
                        help="Backblaze B2 Access Key")
    parser.add_argument("b2_secret_access_key",
                        help="Backblaze B2 Secret Access Key")

    return parser.parse_args()


def _get_source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    # Read in drive model regexes and use them to filter rows coming in from Iceberg in the lazyframe query plan
    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\nRetrieved {len(drive_model_patterns):,} regexes for SMART drive model names from "
        f"\"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    lf: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args).select(
        polars.col("model").alias("model_name"),
        polars.col("date")
    ).filter(
        polars.col("model_name").str.contains(multi_regex_pattern)
    )

    return lf


def _get_date_ranges_per_drive_model(original_source_lazyframe: polars.LazyFrame) -> polars.DataFrame:

    print("\nCreating dataframe with date ranges per drive model...")

    operation_start: float = time.perf_counter()

    drive_dates: polars.DataFrame = original_source_lazyframe.group_by(
        "model_name",
    ).agg(
        polars.col("date").min().alias("first_seen_date"),
        polars.col("date").max().alias("last_seen_date"),

    # Add normalized drive model column
    ).with_columns(
        polars.col(
            "model_name"
        ).str.replace(
            r"^.*(ST\d{2}000NM[0-9A-Z]{4})$", r"Seagate $1"
        ).str.replace(
            r"^.*([HW]U[HS]72\d{4}[A-Z0-9]{6})$", r"WDC/HGST $1"
        ).str.replace(
            r"^\s*(?:Toshiba|TOSHIBA)\s+(MG\d{2}[A-Z0-9]{7})$", r"Toshiba $1"
        ).alias("drive_model_name_normalized")

    # Regroup by normalized name
    ).group_by(
            "drive_model_name_normalized",
    ).agg(
        polars.col("first_seen_date").min().alias("first_seen_date_norm"),
        polars.col("last_seen_date").max().alias("last_seen_date_norm"),

    # Add columns for min/max quarter
    ).with_columns(
        polars.concat_str(
            [
                polars.col("first_seen_date_norm").dt.year(),
                polars.col("first_seen_date_norm").dt.quarter(),
            ],
            separator=" Q"
        ).alias("first_seen"),

        polars.concat_str(
            [
                polars.col("last_seen_date_norm").dt.year(),
                polars.col("last_seen_date_norm").dt.quarter(),
            ],
            separator=" Q"
        ).alias("last_seen")

    # Filter down to just the columns we care about
    ).select(
        polars.col("drive_model_name_normalized").alias("drive_model"),
        "first_seen",
        "last_seen",
        polars.col("last_seen_date_norm").alias("last_seen_date"),
    ).sort(
        "first_seen",
        "drive_model",
        descending=[True, False],
    ).collect()

    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tMaterialized data in {operation_duration:.01f} seconds")

    # print(drive_dates)

    return drive_dates


def _main() -> None:
    processing_start: float = time.perf_counter()

    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _get_source_lazyframe(args)

    date_ranges_per_drive_model: polars.DataFrame = _get_date_ranges_per_drive_model(
        original_source_lazyframe)

    print("\nDrives by date introduced:")

    # Find the latest seen date (i.e., drive is still actively deployed)
    drive_still_deployed_date: datetime.date = date_ranges_per_drive_model.select(
        polars.col("last_seen_date").max()
    ).item()

    prev_first_seen: str | None = None
    for curr_drive_model_row in date_ranges_per_drive_model.iter_rows(named=True):
        if curr_drive_model_row['first_seen'] != prev_first_seen:
            print(f"\n\t{curr_drive_model_row['first_seen']}")
            prev_first_seen = curr_drive_model_row['first_seen']

        if curr_drive_model_row['last_seen_date'] == drive_still_deployed_date:
            drive_last_seen_str = "(currently deployed)"
        else:
            drive_last_seen_str = f"(removed from use: {curr_drive_model_row['last_seen']})"

        mfr, model = curr_drive_model_row['drive_model'].split(" ")

        print(f"\t\t{mfr:8} {model:16} {drive_last_seen_str}")

    processing_duration: float = time.perf_counter() - processing_start
    print(f"\nETL pipeline total processing time: {processing_duration:.01f} seconds\n")


if __name__ == "__main__":
    _main()

import argparse
import json
import time
import polars
import re

import backblaze_drive_stats_data
import etl_pipeline


def _normalize_drive_model_name(raw_drive_model: str) -> str:
    # Tokenize to see if we have manufacturer -- split with no params uses multi-whitespace as separator,
    #   so we get some nice trim and whitespace collapse
    model_tokens: list[str] = raw_drive_model.split()

    if not 1 <= len(model_tokens) <= 2:
        raise ValueError(f"Drive model name '{raw_drive_model}' did not result in 1 or 2 tokens")

    models_to_mfrs: dict[str, str] = {
        r'ST\d+'    : 'Seagate',
        r'WU[HS]72' : 'WDC/HGST',
    }

    expected_mfr_strings: set[str] = {
        'WDC/HGST',
        'Seagate',
        'Toshiba',
        'WDC',
    }

    # Figure out the manufacturer if there wasn't on00e
    if len(model_tokens) == 1:
        for curr_regex in models_to_mfrs:
            if re.match(curr_regex, model_tokens[0]):
                return f"{models_to_mfrs[curr_regex]} {model_tokens[0]}"

        # If we get here, we didn't get a match and puke out
        raise ValueError(f"Cannot determine mfr from model string: {raw_drive_model}")

    # Two token cases

    # Do some mfr name mappings
    name_mappings: dict[str, str] = {
        "TOSHIBA"   : "Toshiba",
        "HGST"      : "WDC/HGST",
        "WDC"       : "WDC/HGST",
    }
    if model_tokens[0] in name_mappings:
        model_tokens[0] = name_mappings[model_tokens[0]]

    if model_tokens[0] not in expected_mfr_strings:
        raise ValueError(f"Drive mfr {model_tokens[0]} not recognized")

    normalized_drive_model_name: str = " ".join(model_tokens)

    return normalized_drive_model_name


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


def _get_smart_drive_model_mappings(smart_drive_model_names_series: polars.Series) -> polars.DataFrame:
    print( etl_pipeline.next_stage_banner() )

    smart_drive_model_mappings_df: polars.DataFrame = smart_drive_model_names_series.to_frame()

    # Add column with normalized drive model name
    smart_drive_model_mappings_df: polars.DataFrame = smart_drive_model_mappings_df.with_columns(
        polars.col("drive_model_name_smart").map_batches(_create_normalized_model_name_series,
                                                         return_dtype=polars.String).alias(
            "drive_model_name_normalized")
    ).sort(by="drive_model_name_normalized")

    normalized_drive_model_name_count: int = smart_drive_model_mappings_df.get_column(
        "drive_model_name_normalized" ).unique().len()

    print(f"\t{smart_drive_model_names_series.len()} SMART drive model names -> {normalized_drive_model_name_count} "
        "normalized drive model names" )

    return smart_drive_model_mappings_df


def _get_smart_drive_model_names(args: argparse.Namespace,
                                 original_source_lazyframe: polars.LazyFrame) -> polars.Series:

    print(etl_pipeline.next_stage_banner())

    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\tRetrieved {len(drive_model_patterns):,} regexes for SMART drive model names from "
        f"\"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    # We want all unique drive model names found in the source file which match one of the drive model regexes
    operation_start: float = time.perf_counter()
    print("\tRetrieving unique SMART drive model names from Polars...")
    drive_models_smart_series: polars.Series = original_source_lazyframe.filter(
        polars.col("model_name").str.contains(multi_regex_pattern)
    ).select(
        "model_name"
    ).collect().get_column("model_name").unique().sort().rename("drive_model_name_smart")
    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tRetrieved {len(drive_models_smart_series):,} SMART drive model names in "
        f"{operation_duration:.01f} seconds")

    return drive_models_smart_series


def _create_normalized_model_name_series( drive_models_name_smart_series: polars.Series ) -> polars.Series:
    normalized_names: list[str] = []
    for smart_model_name in drive_models_name_smart_series:
        normalized_names.append(_normalize_drive_model_name(smart_model_name))
    model_names_normalized_series: polars.Series = polars.Series("model_name_normalized", normalized_names)

    return model_names_normalized_series


def _get_source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    lf: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args).select(
        polars.col("model").alias("model_name"),
        polars.col("date")
    )

    return lf

def _get_date_ranges_per_drive_model(
        original_source_lazyframe: polars.LazyFrame,
        smart_model_name_mappings_dataframe: polars.DataFrame) -> polars.DataFrame:

    print(etl_pipeline.next_stage_banner())

    print("\tMaterializing dataframe for date ranges per drive model...")

    operation_start: float = time.perf_counter()

    # Get drives with min/max date
    drive_dates: polars.DataFrame = original_source_lazyframe.select(
        polars.col("model_name"),
        polars.col("date")
    ).join(
        smart_model_name_mappings_dataframe.lazy(),
        left_on="model_name",
        right_on="drive_model_name_smart"
    ).group_by(
        "drive_model_name_normalized"
    ).agg(
        polars.col("date").min().alias("first_seen_date"),
        polars.col("date").max().alias("last_seen_date"),
    ).with_columns(
        polars.concat_str(
            [
                polars.col("first_seen_date").dt.year(),
                polars.col("first_seen_date").dt.quarter(),
            ],
            separator=" Q"
        ).alias("first_seen"),

        polars.concat_str(
            [
                polars.col("last_seen_date").dt.year(),
                polars.col("last_seen_date").dt.quarter(),
            ],
            separator=" Q"
        ).alias("last_seen"),
    ).select(
        "drive_model_name_normalized",
        "first_seen",
        "last_seen",
    ).sort(
        "first_seen",
        "last_seen",
        descending=True,
    ).collect()

    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tMaterialized data in {operation_duration:.01f} seconds")

    print(drive_dates)

    raise NotImplementedError("not done yet")


def _main() -> None:
    processing_start: float = time.perf_counter()

    etl_pipeline.create_pipeline(
        (
            "Retrieve SMART drive model names",
            "Create mapping table for SMART model name -> normalized model name...",
            "Create report of drives by date ranges seen",
        )
    )

    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _get_source_lazyframe(args)

    smart_drive_model_names: polars.Series = _get_smart_drive_model_names(args, original_source_lazyframe)

    smart_model_name_mappings_dataframe: polars.DataFrame = _get_smart_drive_model_mappings(smart_drive_model_names)

    # Can delete SMART drive model name series as its no longer used
    del smart_drive_model_names

    date_ranges_per_drive_model: polars.DataFrame = _get_date_ranges_per_drive_model(
        original_source_lazyframe, smart_model_name_mappings_dataframe)

    processing_duration: float = time.perf_counter() - processing_start
    print(f"\nETL pipeline total processing time: {processing_duration:.01f} seconds\n")


if __name__ == "__main__":
    _main()

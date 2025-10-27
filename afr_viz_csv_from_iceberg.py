import argparse
import csv
import datetime
import json
import multiprocessing
import pathlib
import polars
import re
import time

import iceberg_table


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Create quarterly AFR visualization CSV")

    default_min_drives: int = 2_000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
                        type=int, default=default_min_drives)

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

    parser.add_argument("output_csv", help="Path to output visualization CSV file")
    return parser.parse_args()


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


def _source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    print("\nOpening Polars datasource...")

    # Let's try some Iceberg magic
    current_iceberg_schema_uri: str = iceberg_table.current_metadata_file_s3_uri(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        args.bucket_name,
        args.table_path
    )

    base_filename: str = pathlib.Path(current_iceberg_schema_uri).name

    print(f"\tCurrent Backblaze Drive Stats Iceberg schema file: {base_filename}")
    print(f"\t\tSchema URI: {current_iceberg_schema_uri}")

    storage_options = {
        "s3.endpoint"           : args.s3_endpoint,
        "s3.region"             : args.b2_region,
        "s3.access-key-id"      : args.b2_access_key,
        "s3.secret-access-key"  : args.b2_secret_access_key,
    }

    source_lazyframe: polars.LazyFrame = polars.scan_iceberg(current_iceberg_schema_uri, 
                                                             storage_options=storage_options)

    return source_lazyframe


def _increment_datetime_one_quarter(dt: datetime.date) -> datetime.date:
    incremented_year: int
    incremented_month: int = (dt.month + 3) % 12
    if incremented_month == 1:
        incremented_year = dt.year + 1
    else:
        incremented_year = dt.year

    return datetime.date(incremented_year, incremented_month, 1)


def _compute_drive_quarterly_afrs( args: argparse.Namespace,
                    source_lazyframe: polars.LazyFrame,
                    smart_model_name_mappings_dataframe: polars.DataFrame ) -> dict[str, list[dict[str, float|int]]]:

    print("\nETL pipeline stage 3 of 4: Calculate AFR...")

    # Iterate over all normalized names
    normalized_drive_models: list[str] = smart_model_name_mappings_dataframe.get_column(
        "drive_model_name_normalized").unique().sort().to_list()

    norm_drive_model_count: int = len(normalized_drive_models)
    print(f"\tAttempting to compute quarterly AFR for {norm_drive_model_count:,} candidate drive models...")

    operation_start: float = time.perf_counter()

    quarterly_afr_per_drive: dict[str, list[ dict[str, float | int]] ] = {}

    for model_idx, curr_norm_drive_model in enumerate(normalized_drive_models):
        print(f"\tCandidate drive model {model_idx + 1:2d} of {norm_drive_model_count:2d}: \"{curr_norm_drive_model}\"")

        model_processing_start: float = time.perf_counter()

        drive_models_smart_names: list[str] = smart_model_name_mappings_dataframe.filter(
                polars.col("drive_model_name_normalized").eq(curr_norm_drive_model)
            ).get_column(
                "drive_model_name_smart"
            ).sort().to_list()

        # print(f"\t\t\tSMART drive model names: {json.dumps(drive_models_smart_names)}")


        # Pull all data rows for this drive
        drive_model_health_rows: polars.DataFrame = source_lazyframe.filter(
            polars.col("model").is_in(drive_models_smart_names)
        ).select(
            "date", "serial_number", "failure"
        ).collect()

        drive_data_dates: dict[str, datetime.date] = {
            'min': drive_model_health_rows.get_column("date").min(),
            'max': drive_model_health_rows.get_column("date").max(),
        }

        # print(f"\t\tMin date: {drive_data_dates['min'].isoformat()}")

        # Walk data by quarter
        quarter_start_year_month: dict[str, int] = {
            "year": drive_data_dates['min'].year,
        }
        if drive_data_dates['min'].month >= 10:
            quarter_start_year_month['month'] = 10
        elif drive_data_dates['min'].month >= 7:
            quarter_start_year_month['month'] = 7
        elif drive_data_dates['min'].month >= 4:
            quarter_start_year_month['month'] = 4
        else:
            quarter_start_year_month['month'] = 1

        quarter_start_date: datetime.date = datetime.date(quarter_start_year_month['year'],
                                                          quarter_start_year_month['month'],
                                                          1)
        # print(f"\t\tInitial date start of walk: {quarter_start_date.isoformat()}")

        cumulative_drive_days: int = 0
        cumulative_drive_failures: int = 0

        while quarter_start_date <= drive_data_dates['max']:
            # print(f"\t\tProcessing quarter of data starting on {quarter_start_date.isoformat()}")

            quarter_drive_data: polars.DataFrame = drive_model_health_rows.filter(
                polars.col("date").dt.year().eq(quarter_start_date.year),
                polars.col("date").dt.month().is_between(
                    quarter_start_date.month,
                    quarter_start_date.month + 3,
                    # Can leave out closed, it defaults to inclusive on both ends
                )
            ).select(
                polars.col('serial_number').unique().count().alias("unique_serial_numbers_seen"),
                polars.col('failure').count().alias("quarter_drive_days"),
                polars.col('failure').sum().alias("quarter_drive_failures"),
            )

            quarter_drive_deploy_count: int = quarter_drive_data.get_column("unique_serial_numbers_seen").item()

            if quarter_drive_deploy_count < args.min_drives:
                quarter_start_date = _increment_datetime_one_quarter(quarter_start_date)
                del quarter_drive_data
                continue
            # print(f"\t\tFound quarter with enough drives starting {quarter_start_date.isoformat()}")

            quarter_drive_days: int = quarter_drive_data.get_column("quarter_drive_days").item()
            quarter_drive_failures: int = quarter_drive_data.get_column("quarter_drive_failures").item()

            # Do AFR calc
            cumulative_drive_days += quarter_drive_days
            cumulative_drive_failures += quarter_drive_failures

            afr_at_end_of_quarter: float = _afr_calc(cumulative_drive_days, cumulative_drive_failures)

            if curr_norm_drive_model not in quarterly_afr_per_drive:
                quarterly_afr_per_drive[curr_norm_drive_model]: list[dict[str, int | float]] = []

            quarterly_entry: dict[ str, int | float ] = {
                'quarter_start'     : quarter_start_date.isoformat(),
                'drives_deployed'   : quarter_drive_deploy_count,
                'afr'               : afr_at_end_of_quarter,
            }

            quarterly_afr_per_drive[curr_norm_drive_model].append(quarterly_entry)

            # Explicitly mark memory eligible for collection
            del quarter_drive_data

            quarter_start_date = _increment_datetime_one_quarter(quarter_start_date)

        model_processing_duration: float = time.perf_counter() - model_processing_start

        del drive_model_health_rows

        if curr_norm_drive_model in quarterly_afr_per_drive:
            print(f"\t\tCreated {len(quarterly_afr_per_drive[curr_norm_drive_model]):2d} quarters of AFR data "
                  f"in {model_processing_duration:.01f} seconds")
        else:
            print("\t\tINFO: candidate drive model filtered out due to no quarters with >= "
                  f"{args.min_drives:,} drives deployed (modify with --min_drives)")

    operation_duration: float = time.perf_counter() - operation_start
    drive_models_with_afr: int = len(quarterly_afr_per_drive)
    print(f"\n\tComputed quarterly AFR for {drive_models_with_afr:,} of {norm_drive_model_count:,} "
          f"candidate drive models in {operation_duration:.01f} seconds")

    if drive_models_with_afr < norm_drive_model_count:
        print(f"\t\t{norm_drive_model_count - drive_models_with_afr:,} candidate drive models filtered out due to "
            "insufficient drive deploy count")

    return quarterly_afr_per_drive


def _afr_calc(cumulative_drive_days: int, cumulative_drive_failures: int) -> float:
    # Scaling factor is 365 unit-days / year
    afr_scaling_factor: float = 365.0

    annualized_failure_rate_percent: float = ( float(cumulative_drive_failures) / float(cumulative_drive_days) ) * \
                                             afr_scaling_factor * 100.0

    return annualized_failure_rate_percent


def _create_normalized_model_name_series( drive_models_name_smart_series: polars.Series ) -> polars.Series:
    normalized_names: list[str] = []
    for smart_model_name in drive_models_name_smart_series:
        normalized_names.append(_normalize_drive_model_name(smart_model_name))
    model_names_normalized_series: polars.Series = polars.Series("model_name_normalized", normalized_names)

    return model_names_normalized_series


def _get_smart_drive_model_mappings(smart_drive_model_names_series: polars.Series) -> polars.DataFrame:
    print("\nETL pipeline stage 2 of 4: Create mapping table for SMART model name -> normalized model name...")

    smart_drive_model_mappings_df: polars.DataFrame = smart_drive_model_names_series.to_frame()

    # Add column with normalized drive model name
    smart_drive_model_mappings_df: polars.DataFrame = smart_drive_model_mappings_df.with_columns(
        polars.col("drive_model_name_smart").map_batches(_create_normalized_model_name_series,
                                                         return_dtype=polars.String).alias(
            "drive_model_name_normalized")
    )

    normalized_drive_model_name_count: int = smart_drive_model_mappings_df.get_column(
        "drive_model_name_normalized" ).unique().len()

    print(f"\t{smart_drive_model_names_series.len()} SMART drive model names -> {normalized_drive_model_name_count} "
        "normalized drive model names" )

    return smart_drive_model_mappings_df


def _get_smart_drive_model_names(args: argparse.Namespace,
                                original_source_lazyframe: polars.LazyFrame) -> polars.Series:

    print("\nETL pipeline stage 1 of 4: Retrieve candidate SMART drive model names...")

    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\tRetrieved {len(drive_model_patterns):,} regexes for SMART drive model names from "
        f"\"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    # We want all unique drive model names found in the source file which match one of the drive model regexes
    operation_start: float = time.perf_counter()
    print("\tRetrieving unique candidate SMART drive model names from Polars...")
    drive_models_smart_series: polars.Series = original_source_lazyframe.filter(
        polars.col("model").str.contains(multi_regex_pattern)
    ).select(
        "model"
    ).collect().get_column("model").unique().sort().rename("drive_model_name_smart")
    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tRetrieved {len(drive_models_smart_series):,} candidate SMART drive model names in "
        f"{operation_duration:.01f} seconds")

    return drive_models_smart_series


def _generate_output_csv(args: argparse.Namespace,
                          computed_afr_data: dict[str, dict[str, float]],
                         drive_deploy_count_dataframe: polars.DataFrame ) -> None:

    print("\nETL pipeline stage 4 of 4: Writing AFR data to visualization CSV...")
    print(f"\tCreating visualization CSV file \"{args.output_csv}\"")

    column_names: list[str] = [
        "Year",
        "Quarter",
    ]

    max_quarters: int = 0
    csv_columns_per_drive_model: dict[str, str] = {}
    for curr_drive_model in sorted(computed_afr_data):

        # Get drives deployed for this drive model
        drives_deployed: int = drive_deploy_count_dataframe.filter(
            polars.col("drive_model_name_normalized").eq(curr_drive_model)
        ).select( "drives_deployed" ).item()

        model_and_drive_count: str = f"{curr_drive_model} ({drives_deployed:,})"

        column_names.append(model_and_drive_count)
        csv_columns_per_drive_model[curr_drive_model] = model_and_drive_count
        max_quarters = max(max_quarters, len(computed_afr_data[curr_drive_model]))

    # Create dict of lists of AFR values per drive model
    human_readable_data: dict[str, list[float]] = {}
    for curr_drive_model in sorted(computed_afr_data):
        human_readable_data[curr_drive_model]: list[float] = []
        for curr_qtr in sorted(computed_afr_data[curr_drive_model]):
            human_readable_data[curr_drive_model].append(computed_afr_data[curr_drive_model][curr_qtr])

    # Do not need computed AFR data anymore, make the memory eligible for garbage collection
    del computed_afr_data

    with open(args.output_csv, "w", newline='') as output_csv:
        csv_writer = csv.DictWriter(output_csv, fieldnames=column_names)
        csv_writer.writeheader()

        # print(json.dumps(column_names, indent=2))
        print(f"\tMax quarters of AFR data for any drive model: {max_quarters:,}")

        agg_increments_per_year: int = 4
        display_year: int = 0
        for curr_quarter in range(max_quarters):
            display_quarter: int = (curr_quarter + 1) % agg_increments_per_year

            if display_quarter == 0:
                display_quarter = 4
            elif display_quarter == 1:
                display_year += 1

            # print(f"\t\tCreating CSV row for year {display_year}, quarter {display_quarter}")
            data_row: dict[str, int | float | str] = {
                'Year':  display_year,
                'Quarter': display_quarter,
            }

            # Iterate across all drives to see if they have AFR data for the current quarter
            for curr_drive_model in human_readable_data:
                # Is there still AFR data for this drive or have we consumed it all?
                if human_readable_data[curr_drive_model]:
                    curr_quarter_afr: float = human_readable_data[curr_drive_model].pop(0)
                    data_row[csv_columns_per_drive_model[curr_drive_model]] = f"{curr_quarter_afr:.03f}"

            # print(json.dumps(data_row, indent=4, sort_keys=True))
            csv_writer.writerow(data_row)


def _main() -> None:

    processing_start: float = time.perf_counter()

    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _source_lazyframe(args)

    smart_drive_model_names: polars.Series = _get_smart_drive_model_names(args, original_source_lazyframe)

    smart_model_name_mappings_dataframe: polars.DataFrame = _get_smart_drive_model_mappings(smart_drive_model_names)

    # Can delete SMART drive model name series as its no longer used
    del smart_drive_model_names

    drive_model_quarterly_afr_stats: dict[str, list[dict[str, str | int]]] = _compute_drive_quarterly_afrs(
        args, original_source_lazyframe, smart_model_name_mappings_dataframe )
    print( "\nDrive quarterly AFR data:\n" + json.dumps(drive_model_quarterly_afr_stats, indent=4, sort_keys=True) )

    # _generate_output_csv(args, drive_model_quarterly_afr_stats, drive_deploy_count_dataframe)
    #
    processing_duration: float = time.perf_counter() - processing_start
    print(f"\nETL pipeline total processing time: {processing_duration:.01f} seconds\n")


if __name__ == "__main__":

    # Not sure why this needs to be done in main context but that's what they say
    #   Using spawn even though it's more expensive as to not inherit the memory space
    #       of the parent that is taking up 24 GB oe memory
    multiprocessing.set_start_method('spawn')

    _main()

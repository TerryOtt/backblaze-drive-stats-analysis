import argparse
import csv
import json
import polars
import re
import time


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Create quarterly AFR visualization CSV")

    # Back off max CPU count as we have stats worker process pegging a CPU and then leave one for
    #   background/OS
    parquet_max_batch_size_default: int = 16 * 1024
    parser.add_argument('--max-batch', help="Max size of Polars record batch, " +
                                                         f"default: {parquet_max_batch_size_default:,}",
                        type=int, default=parquet_max_batch_size_default)

    default_min_drives: int = 2_000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
                        type=int, default=default_min_drives)

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument("input_parquet_file", help="Path to parquet file we read from")
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
    print(f"\nPolars datasource: local Parquet file, \"{args.input_parquet_file}\"")
    source_lazyframe: polars.LazyFrame = polars.scan_parquet(args.input_parquet_file)
    return source_lazyframe


def _get_deploy_counts_with_min_drive_count_filter(args: argparse.Namespace,
                                                   original_source_lazyframe: polars.LazyFrame,
                                                   smart_model_name_mappings_dataframe: polars.DataFrame
    ) -> polars.DataFrame:

    print("\nETL pipeline stage 3 / 6: Retrieve deploy counts for drive models...")

    print("\tRetrieving drive deployment counts from Polars...")
    operation_start: float = time.perf_counter()

    # Oh my SQL of death
    drive_model_deploy_count_dataframe: polars.DataFrame = original_source_lazyframe.select(
        "model", "serial_number"
        ).unique().filter(
            polars.col("model").is_in(
                smart_model_name_mappings_dataframe.get_column("drive_model_name_smart").to_list()
            )
        ).join(smart_model_name_mappings_dataframe.lazy(),
           left_on="model",
           right_on="drive_model_name_smart"
        ).select(
            "drive_model_name_normalized", "serial_number"
        ).group_by(
            "drive_model_name_normalized"
        ).agg(
            [
                polars.col("serial_number").count().alias("drives_deployed"),
            ]
        ).select(
            "drive_model_name_normalized", "drives_deployed"
        ).filter(
            polars.col("drives_deployed").ge(args.min_drives)
        ).sort(
            "drive_model_name_normalized"
        ).collect()

    operation_duration: float = time.perf_counter() - operation_start
    print( f"\t\tRetrieved drive deploy counts for {len(drive_model_deploy_count_dataframe):,} candidate drive models "
           f"in {operation_duration:.01f} seconds\n")

    #print(drive_model_deploy_count_dataframe)

    # How many candidate normalized drive model names did we start with?
    candidate_drive_models_count: int = smart_model_name_mappings_dataframe.get_column(
        "drive_model_name_normalized").unique().len()

    if len(drive_model_deploy_count_dataframe) < candidate_drive_models_count:
        print(f"\tINFO: {candidate_drive_models_count - len(drive_model_deploy_count_dataframe):,} candidate "
              f"drive models were filtered out due to drive counts < {args.min_drives:,} "
              "\n\t\t(modify with --min-drives)" )

    return drive_model_deploy_count_dataframe


def _get_afr_input_data(source_lazyframe: polars.LazyFrame,
                        smart_model_name_mappings_dataframe: polars.DataFrame) -> dict[str, dict[str, dict[str, int]]]:

    afr_input_data: dict[str, dict[str, dict[str, int]]] = {}
    for curr_batch_df in source_lazyframe.select(
            "date", "model", "failure"
        ).filter(
            polars.col("model").is_in(smart_model_name_mappings_dataframe.get_column(
                "drive_model_name_smart").to_list()
            )
        ).join( smart_model_name_mappings_dataframe.lazy(),
                left_on="model",
                right_on="drive_model_name_smart"
        ).select("drive_model_name_normalized", "date", "failure").group_by("drive_model_name_normalized", "date").agg(
            [
                polars.col("failure").count().alias("drives_seen"),
                polars.col("failure").sum().alias("failure_count"),
            ]
        ).sort("drive_model_name_normalized", "date").collect_batches(chunk_size=16384):

        month_quarter_lookup_table: dict[int, int] = _get_month_quarter_lookup_table()
        for curr_row in curr_batch_df.iter_rows(named=True):
            if curr_row["drive_model_name_normalized"] not in afr_input_data:
                afr_input_data[curr_row["drive_model_name_normalized"]]: dict[str, dict[str, int]] = {}

            year_quarter: str = f"{curr_row["date"].year} Q{month_quarter_lookup_table[curr_row["date"].month]}"
            if year_quarter not in afr_input_data[curr_row["drive_model_name_normalized"]]:
                afr_input_data[curr_row["drive_model_name_normalized"]][year_quarter]: dict[str, int] = {
                    "drive_days": 0,
                    "failure_count": 0,
                }

            this_entry: dict[str, int] = afr_input_data[curr_row["drive_model_name_normalized"]][year_quarter]
            this_entry["drive_days"] += curr_row["drives_seen"]
            this_entry["failure_count"] += curr_row["failure_count"]

    return afr_input_data


def _get_afr_stats( source_lazyframe: polars.LazyFrame,
                    smart_model_name_mappings_dataframe: polars.DataFrame ) -> dict[str, dict[str, float]]:

    print("\nETL pipeline stage 4 / 6: Retrieve AFR calculation input data...")
    print(f"\tRetrieving daily drive health data from Polars for "
          f"{smart_model_name_mappings_dataframe.get_column("drive_model_name_normalized").unique().len():,} "
          "drive models...")

    operation_start: float = time.perf_counter()
    quarterly_afr_input_data: dict[str, dict[str, dict[str, int]]] = _get_afr_input_data(
        source_lazyframe, smart_model_name_mappings_dataframe)
    operation_duration: float = time.perf_counter() - operation_start
    print(f"\tRetrieved drive health data in {operation_duration:.01f} seconds")

    # print(json.dumps(quarterly_afr_input_data, indent=4, sort_keys=True))

    print("\nETL pipeline stage 5 / 6: Perform AFR calculations...")
    afr_stats: dict[str, dict[str, float]] = _do_quarterly_afr_calcs_for_all_drives(quarterly_afr_input_data)
    print("\tQuarterly AFR calculations completed")

    return afr_stats


def _get_month_quarter_lookup_table() -> dict[int, int]:
    month_quarter_lookup_table: dict[int, int] = {
         1: 1,
         2: 1,
         3: 1,

         4: 2,
         5: 2,
         6: 2,

         7: 3,
         8: 3,
         9: 3,

        10: 4,
        11: 4,
        12: 4,
    }

    return month_quarter_lookup_table


def _afr_calc(cumulative_drive_days: int, cumulative_drive_failures: int) -> float:
    # Scaling factor is 365 unit-days / year
    afr_scaling_factor: float = 365.0

    annualized_failure_rate_percent: float = ( float(cumulative_drive_failures) / float(cumulative_drive_days) ) * \
                                             afr_scaling_factor * 100.0

    return annualized_failure_rate_percent


def _do_quarterly_afr_calcs_for_all_drives(drive_quarterly_afr_data: dict[str, dict[str, dict[str, int]]]
                                          ) -> dict[str, dict[str, float]]:
    quarterly_afr_all_drives: dict[str, dict[str, float]] = {}

    for curr_drive_model in drive_quarterly_afr_data:
        drive_quarterly_afr: dict[str, float] = {}

        cumulative_drive_days: int = 0
        cumulative_drive_failures: int = 0
        for curr_quarter in sorted(drive_quarterly_afr_data[curr_drive_model]):
            cumulative_drive_days += drive_quarterly_afr_data[curr_drive_model][curr_quarter]['drive_days']
            cumulative_drive_failures += drive_quarterly_afr_data[curr_drive_model][curr_quarter]['failure_count']
            drive_quarterly_afr[curr_quarter] = _afr_calc(cumulative_drive_days, cumulative_drive_failures)

        quarterly_afr_all_drives[curr_drive_model] = drive_quarterly_afr

    return quarterly_afr_all_drives


def _create_normalized_model_name_series( drive_models_name_smart_series: polars.Series ) -> polars.Series:
    normalized_names: list[str] = []
    for smart_model_name in drive_models_name_smart_series:
        normalized_names.append(_normalize_drive_model_name(smart_model_name))
    model_names_normalized_series: polars.Series = polars.Series("model_name_normalized", normalized_names)

    return model_names_normalized_series


def _get_smart_drive_model_mappings(smart_drive_model_names_series: polars.Series) -> polars.DataFrame:
    print("\nETL pipeline stage 2 / 6: Create mapping table for SMART model name -> normalized model name...")

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

    print("\nETL pipeline stage 1 / 6: Retrieve candidate SMART drive model names...")

    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\tRetrieved {len(drive_model_patterns):,} regexes for SMART drive model names from "
        f"\"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    # We want all unique drive model names found in the source file which match one of the drive model regexes
    operation_start: float = time.perf_counter()
    print("\tRetrieving unique candidate SMART drive model names from Polars...")
    drive_models_smart_series: polars.Series = original_source_lazyframe.select("model").unique().filter(
        polars.col("model").str.contains(multi_regex_pattern)
    ).sort("model").collect().to_series().rename("drive_model_name_smart")
    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tRetrieved {len(drive_models_smart_series):,} candidate SMART drive model names in "
        f"{operation_duration:.01f} seconds")

    return drive_models_smart_series


def _truncate_drive_model_name_mapping_dataframe(smart_model_name_mappings_dataframe: polars.DataFrame,
                                                 drive_deploy_count_df: polars.DataFrame ) -> polars.DataFrame:

    truncated_model_name_mappings_dataframe: polars.DataFrame = smart_model_name_mappings_dataframe.join(
        drive_deploy_count_df, on="drive_model_name_normalized" ).select( "drive_model_name_normalized",
        "drive_model_name_smart").sort( "drive_model_name_normalized", "drive_model_name_smart")

    return truncated_model_name_mappings_dataframe


def _generate_output_csv(args: argparse.Namespace,
                          computed_afr_data: dict[str, dict[str, float]],
                         drive_deploy_count_dataframe: polars.DataFrame ) -> None:

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
    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _source_lazyframe(args)
    smart_drive_model_names: polars.Series = _get_smart_drive_model_names(args, original_source_lazyframe)
    smart_model_name_mappings_dataframe: polars.DataFrame = _get_smart_drive_model_mappings(smart_drive_model_names)

    # Can delete SMART drive model name series as its no longer used
    del smart_drive_model_names

    #print(json.dumps(drive_model_mapping, indent=4, sort_keys=True))
    drive_deploy_count_dataframe: polars.DataFrame = _get_deploy_counts_with_min_drive_count_filter(
        args, original_source_lazyframe, smart_model_name_mappings_dataframe )

    # Remove all rows from name mappings dataframe that got filtered out due to being below min drive counts
    smart_model_name_mappings_dataframe: polars.DataFrame = _truncate_drive_model_name_mapping_dataframe(
        smart_model_name_mappings_dataframe, drive_deploy_count_dataframe )

    # print(smart_model_name_mappings_dataframe)

    drive_model_quarterly_afr_stats: dict[str, dict[str, float]] = _get_afr_stats(
        original_source_lazyframe, smart_model_name_mappings_dataframe )
    # print( "\nDrive quarterly AFR data:\n" + json.dumps(drive_model_quarterly_afr_stats, indent=4, sort_keys=True) )

    print("\nETL pipeline stage 6 / 6: Writing AFR data to visualization CSV...")
    print(f"\tVisualization CSV file \"{args.output_csv}\"")
    _generate_output_csv(args, drive_model_quarterly_afr_stats, drive_deploy_count_dataframe)


if __name__ == "__main__":
    _main()

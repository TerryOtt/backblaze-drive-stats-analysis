import argparse
import datetime
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
    return parser.parse_args()


def _normalize_drive_model_name(raw_drive_model: str) -> str:
    # Tokenize to see if we have manufacturer -- split with no params uses multi-whitespace as separator,
    #   so we get some nice trim and whitespace collapse
    model_tokens: list[str] = raw_drive_model.split()

    if not 1 <= len(model_tokens) <= 2:
        raise ValueError(f"Drive model name '{raw_drive_model}' did not result in 1 or 2 tokens")

    models_to_mfrs: dict[str, str] = {
        r'ST\d+'    : 'Seagate',
        r'WU[HS]72' : 'WDC',
    }

    expected_mfr_strings: set[str] = {
        'HGST',
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

    # Do sane casing on Toshiba
    if model_tokens[0].upper() == "TOSHIBA":
        normalized_drive_model_name: str = f"Toshiba {model_tokens[1]}"
    else:
        if model_tokens[0] not in expected_mfr_strings:
            raise ValueError(f"Drive mfr {model_tokens[0]} not recognized")

        normalized_drive_model_name: str = " ".join(model_tokens)

    return normalized_drive_model_name


def _get_drive_model_mapping(args: argparse.Namespace,
                             original_source_lazyframe: polars.LazyFrame) -> dict[str, list[str]]:

    print("\nETL pipeline stage 1 / 5: Retrieve candidate drive model names...")

    drive_model_mapping: dict[str, list[str]] = {}

    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\tRetrieved {len(drive_model_patterns):,} drive model regexes from \"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    # We want all unique drive model names found in the source file which match one of the drive model regexes
    operation_start: float = time.perf_counter()
    print("\tRetrieving unique candidate drive model names from Polars...")
    drive_models: polars.Series = original_source_lazyframe.select("model").unique().filter(
        polars.col("model").str.contains(multi_regex_pattern)
    ).sort("model").collect().to_series()
    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\t\tRetrieved {len(drive_models):,} candidate drive model names in {operation_duration:.01f} seconds")

    # Iterate over drive models to create normalized name -> [raw name from source, ...] mappings
    for current_drive_model in drive_models:
        #print(f"Found drive model \"{current_drive_model}\"")
        normalized_drive_model_name: str = _normalize_drive_model_name(current_drive_model)
        if normalized_drive_model_name not in drive_model_mapping:
            drive_model_mapping[normalized_drive_model_name]: list[str] = []
        drive_model_mapping[normalized_drive_model_name].append(current_drive_model)

    return drive_model_mapping


def _source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    print(f"\nPolars datasource: local Parquet file, \"{args.input_parquet_file}\"")
    source_lazyframe: polars.LazyFrame = polars.scan_parquet(args.input_parquet_file)
    return source_lazyframe


def _get_afr_calc_data(source_lazyframe: polars.LazyFrame, drive_model_normalized: str,
                       drive_model_raw_names: list[str]) -> polars.DataFrame:

    query_select_columns: list[str] = [
        "date",
        "model",
        "failure",
    ]

    post_filter_select_columns: list[str] = [
        "date",
        "failure",
    ]

    # Don't need to collect batches here, at most a few thousand rows, just get it in one swing
    afr_calc_data: polars.DataFrame = source_lazyframe.select(query_select_columns).filter(
        polars.col("model").str.contains_any(drive_model_raw_names)
    ).select(post_filter_select_columns).group_by("date").agg(
        [
            polars.col("failure").count().alias("drives_seen"),
            polars.col("failure").sum().alias("failure_count"),
        ]
    ).sort("date").collect()
    # print(f"\t\t\tRetrieved {len(afr_calc_data):6,} days of aggregated AFR calc data from Polars")
    return afr_calc_data


def _get_deploy_counts_per_raw_model_string(original_source_lazyframe: polars.LazyFrame,
                                            full_raw_model_list: list[str] ) -> polars.DataFrame:

    print("\tRetrieving deploy counts from Polars for candidate drive models...")
    operation_start: float = time.perf_counter()
    raw_model_deploy_count_dataframe: polars.DataFrame = original_source_lazyframe.select(
        "model", "serial_number" ).filter(
        polars.col("model").str.contains_any(full_raw_model_list)
    ).unique().group_by("model").agg(
        [
            polars.col("serial_number").count().alias("drives_deployed"),
        ]
    ).sort("model").collect()
    operation_duration: float = time.perf_counter() - operation_start
    print( f"\t\tRetrieved drive deploy counts for {len(raw_model_deploy_count_dataframe):,} candidate drive models "
           f"in {operation_duration:.01f} seconds\n")

    return raw_model_deploy_count_dataframe


def _exclude_insufficient_deploy_counts(args: argparse.Namespace,
                                        original_source_lazyframe: polars.LazyFrame,
                                        drive_model_mapping: dict[str, list[str]] ) -> None:

    print("\nETL pipeline stage 2 / 5: Exclude candidate drive models with insufficient deploy counts...")
    print(f"\tMinimum model deploy count: {args.min_drives:,} (modify with --min-drives)")
    # Pull deploy counts for all raw models of interest
    full_raw_model_list: list[str] = []
    for drive_model in sorted(drive_model_mapping):
        full_raw_model_list += drive_model_mapping[drive_model]

    # print(json.dumps(full_raw_model_list, indent=4))

    raw_model_deploy_count_dataframe: polars.DataFrame = _get_deploy_counts_per_raw_model_string(
        original_source_lazyframe, sorted(full_raw_model_list) )

    aggregated_deploy_counts: dict[str, int] = {}
    for curr_row in raw_model_deploy_count_dataframe.iter_rows(named=True):
        # print(f"\t{curr_row["model"]}: {curr_row["drives_deployed"]}")
        normalized_name: str = _normalize_drive_model_name(curr_row["model"])
        if normalized_name not in aggregated_deploy_counts:
            aggregated_deploy_counts[normalized_name] = 0
        aggregated_deploy_counts[normalized_name] += curr_row["drives_deployed"]

    original_drive_models_of_interest: int = len(drive_model_mapping)
    drive_models_were_excluded: bool = False
    for curr_drive_model in sorted(aggregated_deploy_counts):
        if aggregated_deploy_counts[curr_drive_model] < args.min_drives:
            print(f"\tINFO: excluded candidate drive model \"{curr_drive_model}\" "
                  f"\n\t\tDeployed drives: {aggregated_deploy_counts[curr_drive_model]:5,}")

            del drive_model_mapping[curr_drive_model]

            if not drive_models_were_excluded:
                drive_models_were_excluded = True

    if drive_models_were_excluded:
        print()

    # print(json.dumps(aggregated_deploy_counts, indent=4, sort_keys=True))
    print(f"\tDrive models with sufficient deploy counts: {len(drive_model_mapping):,} "
          f"(down from initial list of {original_drive_models_of_interest:,} candidate drive models)")


def _get_afr_stats( args: argparse.Namespace,
                    original_source_lazyframe: polars.LazyFrame,
                    drive_model_mapping: dict[str, list[str]] ) -> dict[str, dict[str, float]]:
    afr_stats: dict[str, dict[str, float]] = {}
    print("\nETL pipeline stage 3 / 5: Retrieve AFR calculation input data from Polars...")
    print(f"\tRetrieving daily drive health data for {len(drive_model_mapping):,} drive models...")

    operation_start: float = time.perf_counter()
    # for curr_drive_model in sorted(drive_model_mapping):
    #     afr_result_for_drive: dict[str, int | dict[str, float]] = _get_afr_for_single_model( curr_drive_model,
    #         drive_model_mapping[curr_drive_model], original_source_lazyframe, args )
    #
    #     if afr_result_for_drive is not None:
    #         stats_key_with_deploy_count: str = f"{curr_drive_model} ({afr_result_for_drive['deployed_drives']:,})"
    #         afr_stats[stats_key_with_deploy_count] = afr_result_for_drive['quarterly_afr']
    #
    operation_duration: float = time.perf_counter() - operation_start
    print(f"\tRetrieved drive health data in {operation_duration:.01f} seconds")

    print("\nETL pipeline stage 4 / 5: Perform AFR calculations...")
    operation_start: float = time.perf_counter()
    operation_duration: float = time.perf_counter() - operation_start
    print(f"\tQuarterly AFR calculations completed in {operation_duration:.01f} seconds")


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


def _do_quarterly_afr_calcs(drive_quarterly_afr_data: dict[str, dict[str, int]]) -> dict[str, float]:
    drive_quarterly_afr: dict[str, float] = {}

    cumulative_drive_days: int = 0
    cumulative_drive_failures: int = 0
    for curr_quarter in sorted(drive_quarterly_afr_data):
        cumulative_drive_days += drive_quarterly_afr_data[curr_quarter]['drive_days']
        cumulative_drive_failures += drive_quarterly_afr_data[curr_quarter]['failure_count']
        drive_quarterly_afr[curr_quarter] = _afr_calc(cumulative_drive_days, cumulative_drive_failures)

    return drive_quarterly_afr


def _get_afr_for_single_model(drive_model_normalized: str,
                              drive_model_raw_names: list[str],
                              source_lazyframe: polars.LazyFrame,
                              args: argparse.Namespace ) -> dict[str, int | dict[str, float]]:

    # print(f"\t\tAFR calc starting on drive model {drive_model_normalized}...")
    # print(f"\t\t\tDrive model names to query Polars datasource for: {json.dumps(drive_model_raw_names)}")

    quarterly_afr: dict[str, int | dict[str, float]] = {
        'deployed_drives': 1024,
    }

    data_for_afr_calc: polars.DataFrame = _get_afr_calc_data(source_lazyframe, drive_model_normalized,
                                                             drive_model_raw_names)

    drive_quarterly_afr_data: dict[str, dict[str, int]] = {}
    month_quarter_lookup_table: dict[int, int] = _get_month_quarter_lookup_table()

    for curr_results_row in data_for_afr_calc.iter_rows(named=True):
        row_date: datetime.date = curr_results_row['date']
        drive_days: int = curr_results_row['drives_seen']
        failure_count: int = curr_results_row['failure_count']

        year_quarter: str = f"{row_date.year} Q{str(month_quarter_lookup_table[row_date.month])}"

        if year_quarter not in drive_quarterly_afr_data:
            drive_quarterly_afr_data[year_quarter] = {
                'drive_days': 0,
                'failure_count': 0,
            }

        drive_quarterly_afr_data[year_quarter]['drive_days'] += drive_days
        drive_quarterly_afr_data[year_quarter]['failure_count'] += failure_count

    drive_quarterly_afr: dict[str, float] = _do_quarterly_afr_calcs(drive_quarterly_afr_data)

    quarterly_afr['quarterly_afr']: dict[str, float] = drive_quarterly_afr

    return quarterly_afr


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _source_lazyframe(args)
    drive_model_mapping: dict[str, list[str]] = _get_drive_model_mapping(args, original_source_lazyframe)
    #print(json.dumps(drive_model_mapping, indent=4, sort_keys=True))
    _exclude_insufficient_deploy_counts(args, original_source_lazyframe, drive_model_mapping )
    drive_model_quarterly_afr_stats: dict[str, dict[str, float]] = _get_afr_stats( args,
        original_source_lazyframe, drive_model_mapping )
    # print( "\nDrive quarterly AFR data:\n" + json.dumps(drive_model_quarterly_afr_stats, indent=4, sort_keys=True) )

    print("\nETL pipeline stage 5 / 5: Writing AFR data to visualization CSV...")
    operation_start: float = time.perf_counter()
    operation_duration: float = time.perf_counter() - operation_start
    print(f"\tVisualization CSV created in {operation_duration:.01f} seconds")


if __name__ == "__main__":
    _main()

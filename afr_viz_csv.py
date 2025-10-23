import argparse
import json
import multiprocessing
import polars
import re
import time


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Create quarterly AFR visualization CSV")

    # Back off max CPU count as we have stats worker process pegging a CPU and then leave one for
    #   background/OS
    worker_pool_size_default: int = multiprocessing.cpu_count() - 2
    parser.add_argument('--workers', help=f"Size of worker pool, default: {worker_pool_size_default}",
                        type=int, default=worker_pool_size_default)

    default_min_drives: int = 2_000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
                        type=int, default=default_min_drives)

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument("parquet_file", help="Path to parquet file")
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

    # Figure out the manufacturer if there wasn't one
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

    print("\nGetting drive model mappings...")

    drive_model_mapping: dict[str, list[str]] = {}

    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_patterns: list[str] = json.load(json_handle)
    print(f"\tRead drive model regexes from \"{args.drive_patterns_json}\"")

    multi_regex_pattern: str = "|".join(drive_model_patterns)
    # print(f"multi regex pattern: {multi_regex_pattern}")

    # We want all unique drive model names found in the source file which match one of the drive model regexes
    operation_start: float = time.perf_counter()
    drive_models: polars.Series = original_source_lazyframe.select("model").filter(
        polars.col("model").str.contains(multi_regex_pattern)
    ).unique().sort("model").collect().to_series()
    operation_end: float = time.perf_counter()
    operation_duration: float = operation_end - operation_start

    # How many unique drive models and how much time?
    print( f"\tRetrieved {len(drive_models):,} drive models names which matched patterns of interest "
           f"from Polars datasource in {operation_duration:.03f} seconds")

    # Iterate over drive models to create normalized name -> [raw name from source, ...] mappings
    for current_drive_model in drive_models:
        #print(f"Found drive model \"{current_drive_model}\"")
        normalized_drive_model_name: str = _normalize_drive_model_name(current_drive_model)
        if normalized_drive_model_name not in drive_model_mapping:
            drive_model_mapping[normalized_drive_model_name]: list[str] = []
        drive_model_mapping[normalized_drive_model_name].append(current_drive_model)

    return drive_model_mapping


def _source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    print(f"\nPolars datasource: local Parquet file, \"{args.parquet_file}\"")
    source_lazyframe: polars.LazyFrame = polars.scan_parquet(args.parquet_file)
    return source_lazyframe


def _drive_model_afr_worker(drive_model_normalized: str,
                            drive_model_raw_names: list[str],
                            original_source_lazyframe: polars.LazyFrame) -> dict[str, float]:
    quarterly_afr: dict[str, float] = {}
    print(f"\t\tWorker pool process starting on drive model {drive_model_normalized}")

    return quarterly_afr


def _get_afr_stats( args: argparse.Namespace,
                    original_source_lazyframe: polars.LazyFrame,
                    drive_model_mapping: dict[str, list[str]] ) -> dict[str, dict[str, float]]:
    afr_stats: dict[str, dict[str, float]] = {}
    print("\nGetting AFR stats...")

    # Fire up a worker pool that each gets a drive model to pull data for and do its own independent AFR calc on
    print(f"\tCreating worker pool of size {args.workers} (modify with --workers)")
    worker_args: list[tuple[str, list[str], polars.LazyFrame]] = []
    for curr_drive_model in sorted(drive_model_mapping):
        worker_args.append((curr_drive_model, drive_model_mapping[curr_drive_model], original_source_lazyframe))

    with multiprocessing.Pool(processes=args.workers) as worker_pool:
        afr_results: list[dict[str, float]] = worker_pool.starmap(_drive_model_afr_worker, worker_args)

        for curr_drive_model in sorted(drive_model_mapping):
            # Results from starmap are in the same order of the parameters passed in, so pull head of list
            afr_stats[curr_drive_model] = afr_results.pop(0)

        # Make sure we consumed all results from starmap and the list is empty
        assert not afr_results
        del afr_results

    return afr_stats


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = _source_lazyframe(args)
    drive_model_mapping: dict[str, list[str]] = _get_drive_model_mapping(args, original_source_lazyframe)
    #print(json.dumps(drive_model_mapping, indent=4, sort_keys=True))
    drive_model_quarterly_afr_stats: dict[str, dict[str, float]] = _get_afr_stats( args,
        original_source_lazyframe, drive_model_mapping )


if __name__ == "__main__":
    _main()

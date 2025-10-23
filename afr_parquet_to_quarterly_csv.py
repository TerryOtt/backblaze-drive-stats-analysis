import argparse
import datetime
import json
import multiprocessing
import os

# Doing this as we are already multi-CPU, limit each worker process to one Polars thread
#   NOTE: env var *has* to be set before Polars is imported
os.environ['POLARS_MAX_THREADS'] = "1"
import polars

import re
import time


# Right mix for runtime
#  - r8g.8xlarge (32 CPU, 256 GB RAM)
#  - run with --workers 30 and --max-batch 5000
#  - consumes 150 GB of RAM
#  - long pole is the 5 min 30 seconds single-threaded stats processor
#       - Shard by drive model?
#       - Each process to manage stats for a given model writes its dict to a queue when done
#       - Parent stats collector then takes the list of dicts, unifies them in one dict, returns to parent process

#  - oooor we use the Go polars bindings and write it in Go which makes more sense to me

# Define this once, not every time we need a date lookup
_month_quarter_map: dict[str, str] = {
    '01': '1',
    '02': '1',
    '03': '1',

    '04': '2',
    '05': '2',
    '06': '2',

    '07': '3',
    '08': '3',
    '09': '3',

    '10': '4',
    '11': '4',
    '12': '4',
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Convert Parquet to CSV')

    # Back off max CPU count as we have stats worker process pegging a CPU and then leave one for
    #   background/OS
    worker_pool_size_default: int = multiprocessing.cpu_count() - 2
    parser.add_argument('--workers', help=f"Size of worker pool, default: {worker_pool_size_default}",
                        type=int, default=worker_pool_size_default)
    parquet_max_batch_size_default: int = 1024 * 128
    parser.add_argument('--max-batch', help="Max size of Parquet record batch, " +
                                                         f"default: {parquet_max_batch_size_default:,}",
                        type=int, default=parquet_max_batch_size_default)
    default_min_drives: int = 2_000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
                        type=int, default=default_min_drives)

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument('input_parquet', help='Path to Parquet created from Iceberg table')
    return parser.parse_args()


def _convert_date_to_year_quarter(drive_entry_date: str) -> str:
    month_quarter: str = f"{drive_entry_date[:4]} Q{_month_quarter_map[drive_entry_date[5:7]]}"
    return month_quarter


def _process_materialized_dataframe( daily_drive_health_report_queue: multiprocessing.Queue,
                                     models_with_stats: set[str],
                                     models_seen: set[str],
                                     drive_model_regexes: list[str],
                                     polars_dataframe_materialized: polars.DataFrame ) -> None:

    multi_stats_msg: list[dict[str, str | bool]] = []

    for data_record in polars_dataframe_materialized.iter_rows(named=True):
        # print("Got valid row data record from deserialized dataframe")

        # Is this the first time this worker has seen this drive model string?
        if data_record['model'] not in models_seen:

            # Mark that we've seen it
            models_seen.add(data_record['model'])

            # Scan regexes to see if we should track stats on it
            for curr_regex in drive_model_regexes:

                # If we find a match, add it to set of models we're tracking and stop looking further
                if re.search(curr_regex, data_record['model']):
                    models_with_stats.add(data_record['model'])
                    break

        # Check if we are tracking stats on it
        if data_record['model'] not in models_with_stats:
            # Ignore it
            continue

        # Turn the date into a year/quarter combo
        year_quarter: str = _convert_date_to_year_quarter(data_record['date'].isoformat())

        afr_stats_msg: dict[str, str | bool] = {
            'model': data_record['model'],
            'serial_number': data_record['serial_number'],
            'year_quarter': year_quarter,
        }
        if data_record['failure'] == 1:
            afr_stats_msg['failure'] = True
        else:
            # Don't even include the key if there was no failure
            # afr_stats_msg['failure'] = False
            pass

        # Append this stats message into the queue stats container
        multi_stats_msg.append(afr_stats_msg)


    del polars_dataframe_materialized

    # Ship a whole record batch worth of data in one queue msg to stats processor
    daily_drive_health_report_queue.put(multi_stats_msg)

    del multi_stats_msg


def _parquet_batch_worker(processing_batch_queue: multiprocessing.Queue,
                          daily_drive_health_report_queue: multiprocessing.Queue,
                          args: argparse.Namespace) -> None:

    # Read the regexes for drive models  we care about
    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_regexes: list[str] = json.load(json_handle)

    models_with_stats: set[str] = set()
    models_seen: set[str] = set()

    parquet_columns: list[str] = [
        'model',
        'date',
        'serial_number',
        'failure',
    ]

    parquet_lazyframe: polars.LazyFrame = polars.scan_parquet(args.input_parquet).select(
        parquet_columns )

    while True:
        queue_msg: dict[str, int] | None = processing_batch_queue.get()

        if queue_msg is None:
            #print("Parquet batch worker got a poison pill, no more work")
            break

        # We got a date range to ourselves to work
        print( f"\t\tWorker starting on records for {queue_msg['year']}-{queue_msg['month']:02d}")

        # Now apply the year and month filter, then collect those batches
        filtered_lazyframe: polars.LazyFrame = parquet_lazyframe.filter(
            (polars.col("date").dt.year() == queue_msg['year']) &
            (polars.col("date").dt.month() == queue_msg['month']) )

        del queue_msg

        for polars_materialized_dataframe in filtered_lazyframe.collect_batches(chunk_size=args.max_batch):
            # print(f"\tWorker got materialized dataframe with {len(polars_materialized_dataframe)} rows")
            _process_materialized_dataframe(daily_drive_health_report_queue,
                                            models_with_stats,
                                            models_seen,
                                            drive_model_regexes,
                                            polars_materialized_dataframe)

            del polars_materialized_dataframe

    # Drop memory asap
    del drive_model_regexes
    del models_with_stats
    del models_seen  

    # Need to send poison pill to stats worker to help it know when all work is done
    daily_drive_health_report_queue.put(None)

    # Mark that this process will not be writing to the queue anymore
    daily_drive_health_report_queue.close()

    del daily_drive_health_report_queue

    print("\tParquet reader worker terminating cleanly")


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


def _add_drive_deploy_counts_to_model_names(drive_quarterly_afr_calc_data: dict[str, dict[str, dict[str, int]]],
                                            drive_serial_nums_per_model: dict[str, set[str]]) -> None:

    old_names: list[str] = []
    for curr_drive_model in drive_quarterly_afr_calc_data:
        if curr_drive_model not in drive_serial_nums_per_model:
            raise ValueError(f"Did not find drive {curr_drive_model} in serial numbers per model")
        old_names.append(curr_drive_model)

    for curr_old_name in old_names:
        drive_count: int = len(drive_serial_nums_per_model[curr_old_name])
        new_name: str = f"{curr_old_name} ({drive_count:,})"

        # Remove data under old key, add under new key
        drive_quarterly_afr_calc_data[new_name] = drive_quarterly_afr_calc_data.pop(curr_old_name)

    print("\tAdded drive deployment counts to model names")


def _cull_drive_models(args: argparse.Namespace,
                       drive_quarterly_afr_calc_data: dict[str, dict[str, dict[str, int]]],
                       drive_serial_nums_per_model: dict[str, set[str]]) -> None:

    drive_models_to_cull: list[str] = []
    for drive_model_name in drive_quarterly_afr_calc_data:
        if len(drive_serial_nums_per_model[drive_model_name]) < args.min_drives:
            print(f"\tINFO: drive model '{drive_model_name}' being culled due to insufficient deploy count " +
                  f"({len(drive_serial_nums_per_model[drive_model_name]):,} < min of {args.min_drives:,}) " +
                  "\n\t\t(modify minimum drives per model config setting with --min-drives)")
            drive_models_to_cull.append(drive_model_name)

    for drive_model_to_cull in drive_models_to_cull:
        del drive_quarterly_afr_calc_data[drive_model_to_cull]


def _afr_stats_worker(daily_drive_health_report_queue: multiprocessing.Queue,
                      drive_afr_data_queue: multiprocessing.Queue,
                      args: argparse.Namespace) -> None:

    poison_pills_received: int = 0
    data_records_received: int = 0
    print( "\tAFR stats worker: successfully started" )
    processing_start_time: float = time.perf_counter()
    drive_quarterly_afr_calc_data: dict[str, dict[str, dict[str, int]]] = {}

    known_drive_model_norm_mappings: dict[str, str] = {}
    drive_models_seen: set[str] = set()
    drive_serial_nums_per_model: dict[str, set[str]] = {}

    while True:
        queue_contents: list[dict[str, str | int]] | None = daily_drive_health_report_queue.get()

        if queue_contents is None:
            poison_pills_received += 1
            #print(f"Stats worker has received {poison_pills_received} / {args.workers} poison pills")
            if poison_pills_received >= args.workers:
                # print("Stats worker got all poison pills, no more stats work to do, bailing")
                break

            # Otherwise read next queue message
            continue


        num_stats_entry_in_queue_msg: int = len(queue_contents)


        # If we get here, we have a valid stats message, so process all the records in it
        while queue_contents:
            curr_health_record: dict[str, str | int] = queue_contents.pop() 
            if curr_health_record['model'] in known_drive_model_norm_mappings:
                normalized_drive_model_name: str = known_drive_model_norm_mappings[curr_health_record['model']]
            elif curr_health_record['model'] not in drive_models_seen:
                normalized_drive_model_name: str = _normalize_drive_model_name(curr_health_record['model'])

                # Did name norm change it?
                if normalized_drive_model_name != curr_health_record['model']:
                    known_drive_model_norm_mappings[curr_health_record['model']] = normalized_drive_model_name

                # Regardless, mark it as ome we have seen before
                drive_models_seen.add(curr_health_record['model'])
            else:
                # We know it and it needs no cleanup
                normalized_drive_model_name: str = curr_health_record['model']

            if normalized_drive_model_name not in drive_quarterly_afr_calc_data:
                drive_quarterly_afr_calc_data[normalized_drive_model_name] = {}
            if curr_health_record['year_quarter'] not in drive_quarterly_afr_calc_data[normalized_drive_model_name]:
                drive_quarterly_afr_calc_data[normalized_drive_model_name][curr_health_record['year_quarter']] = {
                    'drive_operating_days': 0,
                    'drive_failures': 0,
                }

            # Update stats with this record
            curr_drive_quarter: dict[str, int] = \
                drive_quarterly_afr_calc_data[normalized_drive_model_name][curr_health_record['year_quarter']]

            # Increment drive_days for this model regardless
            curr_drive_quarter['drive_operating_days'] += 1

            # If there was a failure for the drive, increment drive model failure count as well
            if 'failure' in curr_health_record:
                curr_drive_quarter['drive_failures'] += 1

            # If this is the first time we've seen this serial number for this model, add it
            #       so we can see how many drives deployed per drive model
            if normalized_drive_model_name not in drive_serial_nums_per_model:
                drive_serial_nums_per_model[normalized_drive_model_name] = set()
            if curr_health_record['serial_number'] not in drive_serial_nums_per_model[normalized_drive_model_name]:
                drive_serial_nums_per_model[normalized_drive_model_name].add(curr_health_record['serial_number'])


            del curr_health_record

        del queue_contents

        data_records_received += num_stats_entry_in_queue_msg


    del drive_models_seen
    del known_drive_model_norm_mappings

    processing_end_time: float = time.perf_counter()
    running_time: int = int(processing_end_time - processing_start_time)
    if running_time > 0:
        records_per_second: int = data_records_received // running_time
    else:
        records_per_second: int = 0

    # Cull any drive models without enough deployed drives
    _cull_drive_models(args, drive_quarterly_afr_calc_data, drive_serial_nums_per_model)

    # Turn "WDC WUH721816ALE6L4" into "WDC WUH721816ALE6L4 (27,689)"
    _add_drive_deploy_counts_to_model_names(drive_quarterly_afr_calc_data, drive_serial_nums_per_model)

    # Send our data with inputs for AFR calcs back to parent
    drive_afr_data_queue.put(drive_quarterly_afr_calc_data)
    del drive_quarterly_afr_calc_data
    drive_afr_data_queue.close()
    del drive_afr_data_queue

    del drive_serial_nums_per_model


    print("\tAFR stats worker: processing complete")
    print(f"\tAFR stats worker: processed {data_records_received:11,} drive health records " +
          f"(filtered by drive models of interest) in {running_time:,} seconds" )
    print(f"\tAFR stats worker: processed {records_per_second:,} records/second")


def _afr_calc(cumulative_drive_days: int, cumulative_drive_failures: int) -> float:
    # Scaling factor is 365 unit-days / year
    afr_scaling_factor: float = 365.0

    annualized_failure_rate_percent: float = ( float(cumulative_drive_failures) / float(cumulative_drive_days) ) * \
                                             afr_scaling_factor * 100.0

    return annualized_failure_rate_percent


def _get_afr_calc_input_data(args: argparse.Namespace) -> dict[str, dict[str, dict[str, int]]]:
    print("\nStarting Parquet data reads...")

    # Set up multiprocessing
    processing_batch_queue: multiprocessing.Queue = multiprocessing.Queue()
    daily_drive_health_report_queue: multiprocessing.Queue = multiprocessing.Queue(maxsize=1024*128)
    drive_afr_data_queue: multiprocessing.SimpleQueue = multiprocessing.SimpleQueue()
    child_processes: list[multiprocessing.Process] = []

    for i in range(args.workers):
        worker_handle = multiprocessing.Process(target=_parquet_batch_worker,
                                                args=(processing_batch_queue,
                                                      daily_drive_health_report_queue,
                                                      args) )
        worker_handle.start()
        child_processes.append(worker_handle)

    print(f"\tParent: launched {args.workers} Parquet reader worker processes (modify with --workers)")

    # Start AFR stats worker
    worker_handle = multiprocessing.Process(target=_afr_stats_worker,
                                            args=(daily_drive_health_report_queue, drive_afr_data_queue, args) )
    worker_handle.start()
    child_processes.append(worker_handle)

    # print("Parquet schema:\n")
    # print(polars.read_parquet_schema(args.input_parquet))

    print(f"\tParquet reader workers using max chunk setting of {args.max_batch} (modify with --max-batch)")
    print("\tPolars thread pool size (should be set to 1, that gives one CPU per worker process): " +
          f"{polars.thread_pool_size()}")

    # Create year/month combos and send those to workers
    start_year: int = 2013
    current_year: int = datetime.datetime.now().year
    current_month: int = datetime.datetime.now().month

    # Iterate backwards over years to get the biggest chunks of work up front
    for scan_year in range(current_year, start_year - 1, -1):
        for scan_month in range(12, 0, -1):

            # Ignore any future months
            if scan_year == current_year and scan_month > current_month:
                continue

            queue_msg: dict[str, int] = {
                'year': scan_year,
                'month': scan_month
            }
            processing_batch_queue.put(queue_msg)

    print("\tParent: all work has been divvied up across Parquet reader workers")

    # Poison pill the batch worker pool, one per worker
    for _ in range(args.workers):
        processing_batch_queue.put(None)
    #print("All poison pills sent to batch workers")

    processing_batch_queue.close()
    del processing_batch_queue

    # Note: when batch workers are done, they poison pill stats worker
    # so that stats worker knows when it's time to come home

    # Wait for ALL workers to rejoin (batch workers + AFR stats worker
    print("\tParent: waiting for all workers to finish and rejoin")
    while child_processes:
        handle_to_join: multiprocessing.Process = child_processes.pop()
        handle_to_join.join()
    print("\tParent: all workers have rejoined, all data has been read successfully!")

    # Read AFR calc data created by stats worker and do the maths
    afr_calc_input_data: dict[str, dict[str, dict[str, int]]] = drive_afr_data_queue.get()

    return afr_calc_input_data


def _compute_drive_quarterly_afr(afr_calc_input_data: dict[str, dict[str, dict[str, int]]]
                                 ) -> dict[str, dict[str, float]]:

    print("\nComputing quarterly AFR stats for drives models of interest...")

    computed_drive_quarterly_afr: dict[str, dict[str, float]] = {}

    for curr_drive_model in sorted(afr_calc_input_data):
        #print( f"\tDrive model: {curr_drive_model}")
        curr_drive_quarterly_data: dict[str, dict[str, int]] = afr_calc_input_data[curr_drive_model]
        cumulative_drive_days: int = 0
        cumulative_drive_failures: int = 0

        computed_drive_quarterly_afr[curr_drive_model] = {}

        for curr_drive_quarter in sorted(curr_drive_quarterly_data):
            #print(f"\t\t{curr_drive_quarter}")
            cumulative_drive_days += curr_drive_quarterly_data[curr_drive_quarter]['drive_operating_days']
            cumulative_drive_failures += curr_drive_quarterly_data[curr_drive_quarter]['drive_failures']

            computed_drive_quarterly_afr[curr_drive_model][curr_drive_quarter] = _afr_calc(
                cumulative_drive_days, cumulative_drive_failures )

    return computed_drive_quarterly_afr


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    afr_calc_input_data: dict[str, dict[str, dict[str, int]]] = _get_afr_calc_input_data(args)
    computed_drive_quarterly_afr: dict[str, dict[str, float]] = _compute_drive_quarterly_afr(afr_calc_input_data)

    # Drop our reference to AFR calc input data as it's no longer needed
    del afr_calc_input_data

    #print(json.dumps(computed_drive_quarterly_afr, indent=4, sort_keys=True))


if __name__ == "__main__":
    _main()

import argparse
import json
import multiprocessing
import pandas
import pyarrow.parquet
import re
import time


# Right mix for runtime
#  - c8g.2xlarge (8 CPU, 16 GB RAM)
#  - run with default workers and parquet max batch size
#  - load 8.5 on 8 CPU machine
#  - consumes 5.0 GB of the 16 GB RAM (~31%)
#  - Finishes in 130 seconds (2 mins, 10 seconds)
#  - *Massive* RAM savings now that I switched from
#       multiprocessing.Queue (no max size) to
#       multiprocessing.SimpleQueue.
#
#       Literally saving 150 GB of RAM by find/replace drop in replace from Queue to SimpleQueue

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
    default_min_drives: int = 1000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
                        type=int, default=default_min_drives)

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument('input_parquet', help='Path to Parquet created from Iceberg table')
    return parser.parse_args()


def _convert_date_to_year_quarter(drive_entry_date: str) -> str:
    month_quarter: str = f"{drive_entry_date[:4]} Q{_month_quarter_map[drive_entry_date[5:7]]}"
    return month_quarter


def _parquet_batch_worker(processing_batch_queue: multiprocessing.SimpleQueue,
                          daily_drive_health_report_queue: multiprocessing.SimpleQueue,
                          args: argparse.Namespace) -> None:

    # Read the regexes for drive models  we care about
    with open(args.drive_patterns_json, "r") as json_handle:
        drive_model_regexes: list[str] = json.load(json_handle)

    models_with_stats: set[str] = set()
    models_seen: set[str] = set()

    while True:
        queue_contents: pyarrow.RecordBatch | None = processing_batch_queue.get()

        if queue_contents is None:
            #print("Parquet batch worker got a poison pill, no more work")
            break

        #print( f"Worker got record batch with {queue_contents.num_rows} rows")
        pandas_df: pandas.DataFrame = queue_contents.to_pandas()

        # How many records in this dataframe?
        num_records_in_batch: int = len(pandas_df)

        multi_stats_msg: list[dict[str, str | bool]] = []

        for record_index, data_record in enumerate(pandas_df.itertuples(index=False)):
            # print("Got valid row data record from deserialized dataframe")

            # Is this the first time this worker has seen this drive model string?
            if data_record.model not in models_seen:

                # Mark that we've seen it
                models_seen.add(data_record.model)

                # Scan regexes to see if we should track stats on it
                for curr_regex in drive_model_regexes:

                    # If we find a match, add it to set of models we're tracking and stop looking further
                    if re.search(curr_regex, data_record.model):
                        models_with_stats.add(data_record.model)
                        break

            # Check if we are tracking stats on it
            if data_record.model not in models_with_stats:
                # Ignore it
                continue

            # Turn the date into a year/quarter combo
            year_quarter: str = _convert_date_to_year_quarter(data_record.date.isoformat())

            afr_stats_msg: dict[str, str | bool] = {
                'model'         : data_record.model,
                'serial_number' : data_record.serial_number,
                'year_quarter'  : year_quarter,
            }
            if data_record.failure == 1:
                afr_stats_msg['failure'] = True
            else:
                # Don't even include the key if there was no failure
                #afr_stats_msg['failure'] = False
                pass

            # Append this stats message into the queue stats container
            multi_stats_msg.append(afr_stats_msg)

        # Ship a whole record batch worth of data in one queue msg to stats processor
        daily_drive_health_report_queue.put(multi_stats_msg)

    # Need to send poison pill to stats worker to help it know when all work is done
    daily_drive_health_report_queue.put(None)

    # print("\tParquet reader worker terminating cleanly")


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


def _afr_stats_worker(daily_drive_health_report_queue: multiprocessing.SimpleQueue,
                      drive_afr_data_queue: multiprocessing.SimpleQueue,
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

        # If we get here, we have a valid stats message, so process all the records in it
        for curr_health_record in queue_contents:
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

        num_stats_entry_in_queue_msg: int = len(queue_contents)
        data_records_received += num_stats_entry_in_queue_msg

    processing_end_time: float = time.perf_counter()
    running_time: int = int(processing_end_time - processing_start_time)
    records_per_second: int = data_records_received // running_time

    # Cull any drive models without enough deployed drives
    _cull_drive_models(args, drive_quarterly_afr_calc_data, drive_serial_nums_per_model)

    # Send our data with inputs for AFR calcs back to parent
    drive_afr_data_queue.put(drive_quarterly_afr_calc_data)

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
    processing_batch_queue: multiprocessing.SimpleQueue = multiprocessing.SimpleQueue()
    daily_drive_health_report_queue: multiprocessing.SimpleQueue = multiprocessing.SimpleQueue()
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

    with pyarrow.parquet.ParquetFile(args.input_parquet) as parquet_handle:
        #print(parquet_handle.schema)
        columns: list[str] = [
            'model',
            'date',
            'serial_number',
            'failure',
        ]

        print(f"\tParent: using Parquet max batch size of {args.max_batch:,} records (modify with --max-batch)")
        for curr_batch in parquet_handle.iter_batches(batch_size=args.max_batch, columns=columns):
            processing_batch_queue.put(curr_batch)

        print("\tParent: all work has been sent to Parquet reader workers")

    # Poison pill the batch worker pool, one per worker
    for _ in range(args.workers):
        processing_batch_queue.put(None)
    #print("All poison pills sent to batch workers")

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

    # print(json.dumps(computed_drive_quarterly_afr, indent=4, sort_keys=True))


if __name__ == "__main__":
    _main()

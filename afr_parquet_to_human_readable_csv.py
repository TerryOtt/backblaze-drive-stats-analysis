import argparse
import csv
import json
import math
import multiprocessing
import re
import time

import pandas
import pyarrow.parquet

# Right mix for runtime
#  - c8g.2xlarge (8 CPU, 16 GB RAM)
#  - run with default workers and parquet max batch size
#  - load 8.5 on 8 CPU machine
#  - consumes 5.0 GB of the 16 GB RAM (~31%)
#  - Finishes in 160 seconds (2 mins, 40 seconds)
#  - *Massive* RAM savings now that I switched from
#       multiprocessing.Queue (no max size) to
#       multiprocessing.SimpleQueue.
#  - Literally saving 150 GB of RAM by find/replace drop in
#       for SimpleQueue

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

    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument('input_parquet', help='Path to Parquet created from Iceberg table')
    return parser.parse_args()


def _convert_date_to_year_quarter(drive_entry_date: str) -> str:
    month_quarter: str = f"{drive_entry_date[:4]} Q{_month_quarter_map[drive_entry_date[5:7]]}"
    return month_quarter


def _parquet_batch_worker(processing_batch_queue: multiprocessing.SimpleQueue,
                          daily_drive_health_report_queue: multiprocessing.SimpleQueue) -> None:

    while True:
        queue_contents: pyarrow.RecordBatch | None = processing_batch_queue.get()

        if queue_contents is None:
            #print("Parquet batch worker got a poison pill, no more work")
            break

        #print( f"Worker got record batch with {queue_contents.num_rows} rows")
        pandas_df: pandas.DataFrame = queue_contents.to_pandas()

        # How many records in this dataframe?
        num_records_in_batch: int = len(pandas_df)

        # Since we know how many records in this dataframe, pre-allocate a batch results message
        #   of the proper size
        multi_stats_msg: list[dict[str, str | bool] | None] = [None] * num_records_in_batch

        for record_index, data_record in enumerate(pandas_df.itertuples(index=False)):
            # print("Got valid row data record from deserialized dataframe")

            # Turn the date into a year/quarter combo
            year_quarter: str = _convert_date_to_year_quarter(data_record.date.isoformat())

            afr_stats_msg: dict[str, str | bool] = {
                'model'         : data_record.model,
                'year_quarter'  : year_quarter,
            }
            if data_record.failure == 1:
                afr_stats_msg['failure'] = True
            else:
                afr_stats_msg['failure'] = False

            # Pack afr stats message into the proper allocated spot in the queue stats container
            multi_stats_msg[record_index] = afr_stats_msg

        daily_drive_health_report_queue.put(multi_stats_msg)

    #print("\tParquet reader worker terminating cleanly")
    # Need to send poison pill to stats worker to help it know when all work is done
    daily_drive_health_report_queue.put(None)


def _afr_stats_worker(daily_drive_health_report_queue: multiprocessing.SimpleQueue, args: argparse.Namespace) -> None:
    poison_pills_received: int = 0
    data_records_received: int = 0
    print( "\tAFR stats worker: successfully started" )
    processing_start_time: float = time.perf_counter()
    while True:
        queue_contents: list[dict[str, str | int]] | None = daily_drive_health_report_queue.get()

        if queue_contents is None:
            poison_pills_received += 1
            #print(f"Stats worker has received {poison_pills_received} / {args.workers} poison pills")
            if poison_pills_received < args.workers:
                continue

            #print("Stats worker got all poison pills, no more stats work to do, bailing")
            break

        # TO DO: process stats message
        num_stats_entry_in_queue_msg: int = len(queue_contents)
        data_records_received += num_stats_entry_in_queue_msg

    processing_end_time: float = time.perf_counter()
    running_time: int = int(processing_end_time - processing_start_time)
    records_per_second: int = data_records_received // running_time

    print("\tAFR stats worker: processing complete, " +
          f"processed {data_records_received:11,} drive health records in {running_time:,} seconds" )
    print(f"\tAFR stats worker: processed {records_per_second:,} records/second")


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    with open(args.drive_patterns_json, 'r') as pattern_handle:
        drive_patterns: list[str] = json.load(pattern_handle)

    # Set up multiprocessing
    processing_batch_queue: multiprocessing.SimpleQueue = multiprocessing.SimpleQueue()
    daily_drive_health_report_queue: multiprocessing.SimpleQueue = multiprocessing.SimpleQueue()
    child_processes: list[multiprocessing.Process] = []

    print("\nStarting Parquet processing...")

    for i in range(args.workers):
        worker_handle = multiprocessing.Process(target=_parquet_batch_worker,
                                                args=(processing_batch_queue,daily_drive_health_report_queue))
        worker_handle.start()
        child_processes.append(worker_handle)

    print(f"\tParent: launched {args.workers} Parquet reader worker processes (modify with --workers)")

    # Start AFR stats worker
    worker_handle = multiprocessing.Process(target=_afr_stats_worker,
                                            args=(daily_drive_health_report_queue, args))
    worker_handle.start()
    child_processes.append(worker_handle)

    with pyarrow.parquet.ParquetFile(args.input_parquet) as parquet_handle:
        #print(parquet_handle.schema)
        columns: list[str] = ['model', 'date', 'failure']
        print(f"\tParent: using max Parquet batch size of {args.max_batch:,} records (modify with --max-batch)")
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
    print("\tParent: all workers have rejoined, all processing completed successfully!")


if __name__ == "__main__":
    _main()

import argparse
import csv
import json
import math
import multiprocessing
import re
import time

import pandas
import pyarrow.parquet


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Convert Parquet to CSV')
    worker_pool_size_default: int = multiprocessing.cpu_count()
    parser.add_argument('--workers', help=f"Size of worker pool, default: {worker_pool_size_default}",
                        type=int, default=worker_pool_size_default)
    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument('input_parquet', help='Path to Parquet created from Iceberg table')
    return parser.parse_args()


def _parquet_batch_worker(processing_batch_queue: multiprocessing.Queue):

    while True:
        queue_contents: pyarrow.RecordBatch | None = processing_batch_queue.get()

        if queue_contents is None:
            print("Worker got its poison pill, no more work")
            break

        print( f"Worker got record batch with {queue_contents.num_rows} rows")
        pandas_df: pandas.DataFrame = queue_contents.to_pandas()
        for _ in pandas_df.itertuples(index=False):
            #print("Got valid row tuple from deserialized dataframe")
            pass


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    afr_stats = {}
    with open(args.drive_patterns_json, 'r') as pattern_handle:
        drive_patterns: list[str] = json.load(pattern_handle)

    # Set up multiprocessing
    processing_batch_queue: multiprocessing.Queue = multiprocessing.Queue(maxsize=args.workers)

    child_processes: list[multiprocessing.Process] = []

    for i in range(args.workers):
        worker_handle = multiprocessing.Process(target=_parquet_batch_worker, args=(processing_batch_queue,))
        worker_handle.start()
        child_processes.append(worker_handle)

    print(f"Started {args.workers} workers for pyarrow batches")

    with pyarrow.parquet.ParquetFile(args.input_parquet) as parquet_handle:
        #print(parquet_handle.schema)
        columns: list[str] = ['model', 'date', 'failure']
        batch_size: int = 1024 * 1024

        for curr_batch in parquet_handle.iter_batches(batch_size=batch_size, columns=columns):
            processing_batch_queue.put(curr_batch)

        print("All batches sent to workers")

    # Poison pill the pool, one per worker
    for _ in range(args.workers):
        processing_batch_queue.put(None)
    print("All poison pills sent to workers")

    # Wait for workers to rejoin
    print("Waiting for all workers to finish")
    while child_processes:
        handle_to_join: multiprocessing.Process = child_processes.pop()
        handle_to_join.join()

    print("All workers rejoined cleanly")


if __name__ == "__main__":
    _main()

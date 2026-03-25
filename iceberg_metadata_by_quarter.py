import argparse
import datetime
import json
import pathlib
import time
import typing

import s3fs

import iceberg_table


def _bin_metadata_by_qtr(b2_access_key: str,
                         b2_secret_access_key: str,
                         s3_endpoint: str,
                         metadata_files: list[str],
                         program_state: dict[str, typing.Any]) -> dict[str, set[datetime.date]]:


    s3_handle: s3fs.S3FileSystem = s3fs.S3FileSystem(
        key=b2_access_key,
        secret=b2_secret_access_key,
        client_kwargs={
            'endpoint_url': s3_endpoint,
        },
    )

    quarter_lookup_table: dict[int, int] = {
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

    # Initialize binned metadata from program state
    if program_state is not None and 'binned_metadata' in program_state:
        binned_metadata: dict[str, set[datetime.date]] = program_state['binned_metadata']

        # Remove any metadata files that are OBE due to program state
        trimmed_metadata: list[str] = []
        for metadata_file in reversed(metadata_files):
            base_filename: str = pathlib.Path(metadata_file).name
            # If we've hit versions we know about, bail
            if base_filename[:5] <= program_state["latest_metadata_version"]:
                break

            trimmed_metadata.append(metadata_file)

        metadata_files = reversed(trimmed_metadata)

    else:
        binned_metadata: dict[str, set[datetime.date]] = {}

    # Crack each JSON metadata open and read its contents
    printed_search_string: bool = False

    for metadata_file in metadata_files:
        if not printed_search_string:
            print("Pulling update date for all metadata files that exist:")
            printed_search_string = True

        base_filename: str = pathlib.Path(metadata_file).name

        # Read from S3
        with s3_handle.open(f"s3://{metadata_file}") as s3_file:
            metadata_content = s3_file.read()
        parsed_metadata = json.loads(metadata_content)

        metadata_file_date: datetime.date = datetime.datetime.fromtimestamp(
            parsed_metadata["last-updated-ms"] / 1000.0 ).date()

        print(f"\t{base_filename}: {metadata_file_date.isoformat()}")

        quarter_number: int = quarter_lookup_table[metadata_file_date.month]

        qtr_str: str = f"{metadata_file_date.year} Q{quarter_number}"
        # print(f"Date {metadata_file_date.isoformat()} has quarter {qtr_str}")

        if qtr_str not in binned_metadata:
            binned_metadata[qtr_str] = set()

        binned_metadata[qtr_str].add( metadata_file_date )

        if program_state is not None:
            if 'latest_metadata_version' not in program_state or \
                    base_filename[:5] > program_state['latest_metadata_version']:
                program_state['latest_metadata_version'] = base_filename[:5]

    if program_state is not None:
        program_state['binned_metadata'] = binned_metadata

    return binned_metadata


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Metadata versions/dates per qtr")

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

    parser.add_argument("b2_access_key",
                        help="Backblaze B2 Access Key")
    parser.add_argument("b2_secret_access_key",
                        help="Backblaze B2 Secret Access Key")

    parser.add_argument("state_file", nargs='?', help="Path to state file")

    return parser.parse_args()


def _main() -> None:
    args: argparse.Namespace = _parse_args()

    program_state: dict[str, typing.Any] | None = None

    if args.state_file:
        if pathlib.Path(args.state_file).exists():
            with open(args.state_file) as f:
                program_state = json.load(f)
                for curr_qtr_str in program_state['binned_metadata']:
                    # Convert date strings to datetime objects
                    for date_index, _  in enumerate(program_state['binned_metadata'][curr_qtr_str]):
                        program_state['binned_metadata'][curr_qtr_str][date_index] = \
                            datetime.date.fromisoformat(program_state['binned_metadata'][curr_qtr_str][date_index])
                print(f"\nRead state for metadata versions up to {program_state['latest_metadata_version']}")

        else:
            program_state = {}

    start_time: float = time.perf_counter()
    metadata_files: list[str] = iceberg_table.metadata_files(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        args.bucket_name,
        args.table_path,
    )
    end_time: float = time.perf_counter()

    print(f"\nFound {len(metadata_files):,} Iceberg metadata files in {end_time - start_time:.01f} seconds")

    metadata_files_by_qtr: dict[str, set[datetime.date]] = _bin_metadata_by_qtr(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        metadata_files,
        program_state,
    )

    # print(json.dumps(metadata_files_by_qtr["2026 Q1"], indent=4, sort_keys=True, default=str))

    # Print date range per quarter
    prev_year: str | None = None

    print()
    print("Quarterly date ranges for metadata:\n")

    for curr_qtr_str in reversed(sorted(metadata_files_by_qtr)):
        sorted_dates = sorted(metadata_files_by_qtr[curr_qtr_str])
        date_min: datetime.date = sorted_dates[0]
        date_max: datetime.date = sorted_dates[-1]

        if prev_year and curr_qtr_str[:4] != prev_year:
            print()
        print(f"\t{curr_qtr_str}: {date_min.isoformat()} - {date_max.isoformat()}")

        prev_year = curr_qtr_str[:4]

    if args.state_file:
        state_for_disk: dict[str, str | dict[str, list[str]]] = {
            'latest_metadata_version': program_state['latest_metadata_version'],
            'binned_metadata': {},
        }

        for curr_qtr_str in reversed(sorted(metadata_files_by_qtr)):
            sorted_dates: list[datetime.date] = sorted(metadata_files_by_qtr[curr_qtr_str])
            file_strings: list[str] = [
                sorted_dates[0].isoformat(),
                sorted_dates[-1].isoformat(),
            ]

            state_for_disk['binned_metadata'][curr_qtr_str] = file_strings

        with open(args.state_file, "w") as f:
            json.dump(state_for_disk, f, indent=4, sort_keys=True)


if __name__ == "__main__":
    _main()

import argparse
import time
import polars

import backblaze_drive_stats_data
import etl_pipeline


type TotalDrivesPerQuarter = dict[int, dict[int, int]]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get drive model distributions over time")

    default_min_drives: int = 0
    parser.add_argument('--min-drives',
                        help=f"Minimum number of deployed drives to be included, default: {default_min_drives:,}",
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

    parser.add_argument("output_xlsx", help="Path to output visualization XLSX file (s3:// supported)")
    return parser.parse_args()


def _get_source_lazyframe_with_name_mappings(args: argparse.Namespace) -> polars.LazyFrame:
    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    # Get
    print( etl_pipeline.next_stage_banner() )
    smart_model_name_mappings_dataframe: polars.DataFrame = backblaze_drive_stats_data.get_smart_drive_model_mappings(
        args, source_lazyframe)

    print(etl_pipeline.next_stage_banner())
    # update lazyframe with name mappings
    source_lazyframe = source_lazyframe.join(
        smart_model_name_mappings_dataframe.lazy(),
        left_on="model",
        right_on="drive_model_name_smart",
    ).select(
        "date",
        "drive_model_name_normalized",
        "serial_number",
    )
    # print(source_lazyframe.collect_schema())
    print("\tCompleted")

    return source_lazyframe


def _get_materialized_drive_distribution_data(args: argparse.Namespace,
                                              source_lazyframe: polars.LazyFrame) -> polars.DataFrame:
    print(etl_pipeline.next_stage_banner())
    stage_begin: float = time.perf_counter()
    print("\tMaterializing drive model distribution data from Apache Iceberg on Backblaze B2 using Polars...")
    quarterly_drive_distribution_data: polars.DataFrame = source_lazyframe.group_by(
        polars.col("drive_model_name_normalized").alias("model_name"),
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
    ).agg(
        polars.col("serial_number").unique().count().alias("unique_serial_numbers"),
    ).filter(
        polars.col("unique_serial_numbers").ge(args.min_drives)
    ).select(
        "year",
        "quarter",
        "model_name",
        "unique_serial_numbers"
    ).sort(
        [
            "year",
            "quarter",
            "unique_serial_numbers",
        ],
        descending=[
            True,
            True,
            True,
        ]
    ).collect()

    # print(quarterly_drive_distribution_data)

    print("\t\tCompleted")

    stage_duration: float = time.perf_counter() - stage_begin
    print(f"\tStage duration: {stage_duration:.01f} seconds")

    return quarterly_drive_distribution_data


def _compute_total_drives_per_quarter(quarterly_drive_distribution_data: polars.DataFrame) -> TotalDrivesPerQuarter:
    print(etl_pipeline.next_stage_banner())
    total_drives_per_quarter_dataframe: polars.DataFrame = quarterly_drive_distribution_data.group_by(
        "year", "quarter"
    ).agg(
        polars.col("unique_serial_numbers").sum().alias("total_drives")
    )

    # print(total_drives_per_quarter)

    drives_per_quarter: TotalDrivesPerQuarter = {}

    for curr_row in total_drives_per_quarter_dataframe.iter_rows():
        year, quarter, total_drives = curr_row
        if year not in drives_per_quarter:
            drives_per_quarter[year]: dict[int, int] = {}
        drives_per_quarter[year][quarter]: int = total_drives

    # for curr_year in sorted(drives_per_quarter):
    #     print(f"\t{curr_year}")
    #     for curr_quarter in sorted(drives_per_quarter[curr_year]):
    #         print(f"\t\tQ{curr_quarter}: {drives_per_quarter[curr_year][curr_quarter]:9,}")
    #     print()

    print("\tCompleted")

    return drives_per_quarter


def _main() -> None:
    args: argparse.Namespace = _parse_args()

    etl_pipeline.create_pipeline(
        (
            "Create normalized drive model name mappings",
            "Update source lazyframe with normalized drive model names and filtered columns",
            "Create quarterly drive model distribution data",
            "Calculate total drives deployed per quarter",
        )
    )

    source_lazyframe: polars.LazyFrame = _get_source_lazyframe_with_name_mappings(args)

    quarterly_drive_distribution_dataframe: polars.DataFrame = _get_materialized_drive_distribution_data(
        args, source_lazyframe)

    total_drives_per_quarter: TotalDrivesPerQuarter = _compute_total_drives_per_quarter(
        quarterly_drive_distribution_dataframe)

    prev_year_quarter: tuple[int, int] = (0, 0)
    for curr_dataframe_row in quarterly_drive_distribution_dataframe.iter_rows():
        year, quarter, model_name, drives_deployed = curr_dataframe_row

        if year != prev_year_quarter[0]:
            print(f"\n{year} ")

        if quarter != prev_year_quarter[1]:
            print(f"\n\tQ{quarter} ({total_drives_per_quarter[year][quarter]:9,} total drives)")
            prev_year_quarter = (year, quarter)

        print(f"\t\t\t{model_name:30}: {drives_deployed:7,}")




if __name__ == "__main__":
    _main()

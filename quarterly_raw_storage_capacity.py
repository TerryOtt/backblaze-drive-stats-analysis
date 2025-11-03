import argparse
import time
import polars

import backblaze_drive_stats_data
import etl_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get drive model distributions over time")

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

                             # KB     MB     GB     TB
    bytes_in_terabyte: int = 1024 * 1024 * 1024 * 1024

    # update lazyframe with name mappings
    source_lazyframe = source_lazyframe.join(
        smart_model_name_mappings_dataframe.lazy(),
        left_on="model",
        right_on="drive_model_name_smart"

    # Pull the columns we care about
    ).select(
        "date",
        "serial_number",
        (polars.col("capacity_bytes") // bytes_in_terabyte).alias("capacity_tb")
    )
    # print(source_lazyframe.collect_schema())
    print("\tCompleted")

    return source_lazyframe


def _get_materialized_quarterly_storage_capacity(source_lazyframe: polars.LazyFrame) -> polars.DataFrame:
    print(etl_pipeline.next_stage_banner())
    stage_begin: float = time.perf_counter()
    print("\tMaterializing quarterly raw storage capacity from Apache Iceberg on Backblaze B2 using Polars...")
    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = source_lazyframe.group_by(
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
    ).agg(
        polars.struct(["serial_number", "capacity_tb"]).unique().alias("drives_seen")
    ).with_columns(
        polars.col("drives_seen").list.eval(
            polars.element().struct.field("capacity_tb").sum()
        ).alias(
            "summed_tb"
        )
    ).select(
        "year", "quarter", polars.col("summed_tb").list.first().alias("qtr_capacity_tb")
    ).sort(
        [
            "year",
            "quarter"
        ],
        descending=[
            True,
            True,
        ]
    ).collect()

    print(quarterly_raw_storage_capacity_dataframe)

    print("\t\tCompleted")

    stage_duration: float = time.perf_counter() - stage_begin
    print(f"\tStage duration: {stage_duration:.01f} seconds")

    return quarterly_raw_storage_capacity_dataframe


def _main() -> None:
    args: argparse.Namespace = _parse_args()

    etl_pipeline.create_pipeline(
        (
            "Create normalized drive model name mappings",
            "Update source lazyframe with normalized drive model names and filtered columns",
            "Create quarterly raw storage capacity data",
        )
    )

    source_lazyframe: polars.LazyFrame = _get_source_lazyframe_with_name_mappings(args)

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = _get_materialized_quarterly_storage_capacity(
        source_lazyframe)

    prev_year: int = 0

    exabytes_in_terabyte: float = 1024 * 1024
    for curr_row in quarterly_raw_storage_capacity_dataframe.iter_rows():
        year, quarter, capacity_tb = curr_row

        if prev_year != year:
            print(f"\n{year}")
            prev_year = year

        qtr_exabytes: float = capacity_tb / exabytes_in_terabyte

        if qtr_exabytes < 0.1:
            continue

        print(f"\tQ{quarter}: {qtr_exabytes:5,.01f} exabytes (EB)")


if __name__ == "__main__":
    _main()

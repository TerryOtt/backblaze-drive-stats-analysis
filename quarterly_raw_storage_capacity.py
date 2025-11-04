import argparse
import time
import polars
from dateutil.rrule import YEARLY

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
    bytes_per_terabyte: float = 1000 * 1000 * 1000 * 1000

    # update lazyframe with name mappings
    source_lazyframe = source_lazyframe.join(
        smart_model_name_mappings_dataframe.lazy(),
        left_on="model",
        right_on="drive_model_name_smart"

    # Pull the columns we care about
    ).select(
        "date",
        polars.col("drive_model_name_normalized").alias("model_name"),
        (polars.col("capacity_bytes") / bytes_per_terabyte).alias("capacity_tb"),
        "serial_number",
    )
    # print(source_lazyframe.collect_schema())
    print("\tCompleted")

    return source_lazyframe


def _get_materialized_quarterly_storage_capacity(source_lazyframe: polars.LazyFrame) -> polars.DataFrame:
    print(etl_pipeline.next_stage_banner())
    stage_begin: float = time.perf_counter()
    print("\tMaterializing quarterly raw storage capacity from Apache Iceberg on Backblaze B2 using Polars...")

    # # Start with creating a dataframe mapping drive model -> capacity in TB
    # unique_models_with_capacity_tb: polars.DataFrame = source_lazyframe.unique(
    #     "model_name"
    # ).select(
    #     "model_name", "capacity_tb"
    # ).collect()

    #print(unique_models_with_capacity_tb)

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = source_lazyframe.group_by(
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
        "model_name",
        "capacity_tb",
    ).agg(
        polars.col("serial_number").unique().count().alias("unique_sn_per_quarter_and_model")
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

    terabytes_per_petabyte: int = 1000
    petabytes_per_exabyte: int = 1000

    # for curr_row in quarterly_raw_storage_capacity_dataframe.iter_rows():
    #     year, quarter, capacity_tb = curr_row
    #
    #     qtr_petabytes: float = capacity_tb / terabytes_per_petabyte
    #     if qtr_petabytes > 0.0:
    #         qtr_exabytes: float = qtr_petabytes / petabytes_per_exabyte
    #         print(f"{year} Q{quarter}: {qtr_petabytes:7,.01f} petabytes (PB) / {qtr_exabytes:4.01f} exabytes (EB)")
    #
    #     if prev_year != year:
    #         prev_year = year


if __name__ == "__main__":
    _main()

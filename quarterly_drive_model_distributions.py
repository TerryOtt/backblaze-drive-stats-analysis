import argparse
import typing

import polars

import backblaze_drive_stats_data
import etl_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get drive model distributions over time")

    default_min_drives: int = 2_000
    parser.add_argument('--min-drives', help="Minimum number of deployed drives for model, default: " +
                        f"{default_min_drives:,}",
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




def _main() -> None:
    args: argparse.Namespace = _parse_args()

    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    pipeline_stage_descriptions: tuple[str, ...] = (
        "Create normalized drive model name mappings",
        "Update source lazyframe with normalized drive model names and filtered columns",
        "Create quarterly drive model distribution data",
    )

    etl_pipeline.create_pipeline(pipeline_stage_descriptions)

    print( etl_pipeline.next_stage_banner() )
    smart_model_name_mappings_dataframe: polars.DataFrame = backblaze_drive_stats_data.get_smart_drive_model_mappings(
        args, source_lazyframe)

    print(etl_pipeline.next_stage_banner())
    # update lazyframe with name mappings
    source_lazyframe = source_lazyframe.join(
        smart_model_name_mappings_dataframe.lazy(),
        left_on="model",
        right_on="drive_model_name_smart",
    # ).select(
    #     "date",
    #     "drive_model_name_normalized",
    #     "serial_number",
    )
    # print(source_lazyframe.collect_schema())

    print(etl_pipeline.next_stage_banner())
    quarterly_drive_distribution_data: polars.DataFrame = source_lazyframe.group_by(
        "drive_model_name_normalized",
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.year().alias("quarter"),
    ).agg(
        polars.col("serial_number").unique().count().alias("qtr_unique_serial_numbers"),
    ).collect().sort(
        "year",
        "quarter"
        "drive_model_name_normalized",
    )

    print(quarterly_drive_distribution_data)

if __name__ == "__main__":
    _main()

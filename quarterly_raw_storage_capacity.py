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


def _get_source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    print( etl_pipeline.next_stage_banner() )

                          # KB     MB     GB     TB
    bytes_per_tb: float = 1000 * 1000 * 1000 * 1000

    # Reduce to columns we care about
    source_lazyframe = source_lazyframe.select(
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
        (polars.col("capacity_bytes") / bytes_per_tb).alias("capacity_tb"),
        polars.col("model").alias("model_name"),
        "serial_number",
    )

    # print(source_lazyframe.collect_schema())

    print("\tCompleted")

    return source_lazyframe


def _get_materialized_quarterly_storage_capacity(source_lazyframe: polars.LazyFrame) -> polars.DataFrame:
    print(etl_pipeline.next_stage_banner())
    stage_begin: float = time.perf_counter()
    print("\tMaterializing quarterly raw storage capacity from Apache Iceberg on Backblaze B2 using Polars...")

    tb_per_pb: float = 1000.0
    pb_per_eb: float = 1000.0

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = source_lazyframe.group_by(
        "year",
        "quarter",
        "model_name",
    ).agg(
        polars.col("serial_number").unique().count().alias("unique_sn_per_quarter_and_model"),

        # Mode returns a list of values as there can be a tie of most frequent.
        # There won't be in our case
        polars.col("capacity_tb").mode().first().alias("median_capacity_tb"),
    ).with_columns(
        (polars.col("median_capacity_tb") * polars.col("unique_sn_per_quarter_and_model") / tb_per_pb).alias(
            "total_pb_for_model" )
    ).group_by(
        "year", "quarter"
    ).agg(
        polars.col("total_pb_for_model").sum().alias("raw_storage_pb"),
    ).with_columns(
        (polars.col("raw_storage_pb") / pb_per_eb).alias("raw_storage_eb"),
    ).select(
        "year",
        "quarter",
        "raw_storage_eb",
    ).sort(
        (
            "year",
            "quarter",
        ),
    ).collect()

    # print(quarterly_raw_storage_capacity_dataframe)

    print("\t\tCompleted")

    stage_duration: float = time.perf_counter() - stage_begin
    print(f"\tStage duration: {stage_duration:.01f} seconds")

    return quarterly_raw_storage_capacity_dataframe


def _main() -> None:
    args: argparse.Namespace = _parse_args()

    etl_pipeline.create_pipeline(
        (
            "Get source lazyframe (note: includes to fix rows with invalid capacities from SMART",
            "Create quarterly raw storage capacity data",
        )
    )

    source_lazyframe: polars.LazyFrame = _get_source_lazyframe(args)

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = _get_materialized_quarterly_storage_capacity(
        source_lazyframe)

    print("\nBackblaze Raw Storage Capacity By Quarter:\n")

    prev_eb: float = 0

    output_rows: list[str] = []

    prev_year: int = 2013
    for curr_row in quarterly_raw_storage_capacity_dataframe.iter_rows():
        year, quarter, raw_capacity_eb = curr_row

        if year != prev_year:
            output_rows.append("")
            prev_year = year

        if prev_eb != 0:
            output_rows.append(f"\t{year} Q{quarter}: {raw_capacity_eb:5.02f} exabytes (EB) "
                  f"(delta: {raw_capacity_eb - prev_eb:5.02f} EB)")
        else:
            output_rows.append(f"\t{year} Q{quarter}: {raw_capacity_eb:5.02f} exabytes (EB)")

        prev_eb = raw_capacity_eb

    # Show from newest to oldest
    for curr_output_row in reversed(output_rows):
        print(curr_output_row)


if __name__ == "__main__":
    _main()

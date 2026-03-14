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
    print( etl_pipeline.next_stage_banner() )
    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)
    # print(source_lazyframe.collect_schema())

                          # KB     MB     GB     TB
    bytes_per_tb: float = 1000 * 1000 * 1000 * 1000

    # Reduce to columns we care about
    source_lazyframe = source_lazyframe.select(
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
        polars.col("datacenter").str.to_uppercase(),
        polars.col("model").alias("model_name"),
        (polars.col("capacity_bytes") / bytes_per_tb).alias("capacity_tb"),
        "serial_number",
    ).filter(
        polars.col("datacenter").is_not_null(),
        polars.col("datacenter").str.len_chars().ge(4)
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
        "datacenter",
        "model_name",
    ).agg(
        polars.col("serial_number").unique().count().alias("unique_sn_per_quarter_and_model"),
        polars.col("capacity_tb").median().alias("median_capacity_tb"),
    ).with_columns(
        (polars.col("median_capacity_tb") * polars.col("unique_sn_per_quarter_and_model") / tb_per_pb).alias(
            "total_pb_for_model" )
    ).group_by(
        "year", "quarter", "datacenter",
    ).agg(
        polars.col("total_pb_for_model").sum().alias("raw_storage_pb")
    ).with_columns(
        (polars.col("raw_storage_pb") / pb_per_eb).alias("raw_storage_eb"),
    ).select(
        "year",
        "quarter",
        "datacenter",
        "raw_storage_eb",
    ).sort(
        (
            "year",
            "quarter",
            "datacenter",
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
            "Get source lazyframe (note: fixes rows from source with invalid capacities from SMART)",
            "Create quarterly raw storage capacity data by datacenter",
        )
    )

    source_lazyframe: polars.LazyFrame = _get_source_lazyframe(args)

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = _get_materialized_quarterly_storage_capacity(
        source_lazyframe)

    print("\nBackblaze Raw Storage Capacity By Quarter & Datacenter:\n")

    output_rows: list[str] = []

    prev_year_qtr: tuple[int, int] = (2023, 3)
    year_qtr_datacenters: list[str] = []
    prev_qtr_cumulative_eb: float = 0
    for year, quarter, datacenter_iata_code, datacenter_eb in quarterly_raw_storage_capacity_dataframe.iter_rows():
        # print(f"{year} Q{quarter}")
        if (year, quarter) != prev_year_qtr:
            prev_year, prev_quarter = prev_year_qtr
            output_rows.append(f"\t{prev_year} Q{prev_quarter} ({prev_qtr_cumulative_eb:5.02f} EB): "
                  f"{", ".join(year_qtr_datacenters)}")
            year_qtr_datacenters.clear()
            prev_year_qtr = (year, quarter)
            prev_qtr_cumulative_eb = 0

        year_qtr_datacenters.append(f"{datacenter_iata_code} = {datacenter_eb:4.02f} EB")
        prev_qtr_cumulative_eb += datacenter_eb

    # Print out final year and quarter
    if year_qtr_datacenters:
        output_rows.append(f"\t{prev_year_qtr[0]} Q{prev_year_qtr[1]} ({prev_qtr_cumulative_eb:5.02f} EB): "
              f"{", ".join(year_qtr_datacenters)}")

    for curr_year_qtr in reversed(output_rows):
        print(curr_year_qtr)


if __name__ == "__main__":
    _main()

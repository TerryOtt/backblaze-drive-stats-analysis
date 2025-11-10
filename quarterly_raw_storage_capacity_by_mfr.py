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


                          # KB     MB     GB     TB
    bytes_per_tb: float = 1000 * 1000 * 1000 * 1000

    tb_per_pb: float = 1000.0

    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    source_lazyframe = source_lazyframe.join(
        # Join with normalized name mapping dataframe to reduce to drives of interest
        backblaze_drive_stats_data.get_smart_drive_model_mappings(args, source_lazyframe).lazy(),
        left_on="model",
        right_on="drive_model_name_smart",

    # Reduce to columns we care about
    ).select(
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
        polars.col("drive_model_name_normalized").alias("model_name"),
        "serial_number",
        (polars.col("capacity_bytes") / bytes_per_tb).alias("capacity_tb"),
    )

    # Add a column for "correct" capacity per model (i.e., mode of TB capacities reported for that model)
    #   Don't remember the mode?
    #   I didn't remember it either when scyost mentioned it.
    #   It's the _most common_/_most frequently seen_ value in the list
    drive_model_capacity_by_model: polars.LazyFrame = source_lazyframe.group_by(
        "model_name",
    ).agg(
        # Have to chain first() because mode returns a list, as there can be a tie of most frequent
        polars.col("capacity_tb").mode().first().alias("capacity_tb_normalized"),
    )

    source_lazyframe = source_lazyframe.join(
        drive_model_capacity_by_model,
        on="model_name",

    # Reduce columns back down after getting normalized drive capacity per model
    ).select(
        "year",
        "quarter",
        "model_name",
        polars.col("capacity_tb_normalized").alias("capacity_tb"),
        "serial_number"

    # Give one row per SN per quarter
    ).unique(

    # Aggregate number of drives per quarter per model
    ).group_by(
        "year",
        "quarter",
        "model_name",
        "capacity_tb",
    ).agg(
        polars.col("serial_number").count().alias("drives_deployed"),

    # Do the math to compute PB per quarter per model
    ).select(
        "year",
        "quarter",
        "model_name",
        (polars.col("capacity_tb") * polars.col("drives_deployed") / tb_per_pb).alias("model_cumulative_raw_pb"),
    ).sort(
        "year",
        "quarter",
        "model_name",
    )

    # print(source_lazyframe.collect_schema())

    print("\tCompleted")

    return source_lazyframe


def _get_materialized_quarterly_storage_capacity(source_lazyframe: polars.LazyFrame) -> polars.DataFrame:
    print(etl_pipeline.next_stage_banner())
    stage_begin: float = time.perf_counter()
    print("\tMaterializing quarterly raw storage capacity from Apache Iceberg on Backblaze B2 using Polars...")

    tb_per_pb: float = 1000.0
    # pb_per_eb: float = 1000.0

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = source_lazyframe.select(
        "year",
        "quarter",
        polars.col("model_name").str.split(" ").list.first().alias("drive_mfr"),
        "model_cumulative_raw_pb",
    ).group_by(
        "year",
        "quarter",
        "drive_mfr",
    ).agg(
        polars.col("model_cumulative_raw_pb").sum().alias("raw_capacity_pb")
    ).sort(
        (
            "year",
            "quarter",
            "drive_mfr",
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

    print("\nBackblaze Raw Storage Capacity By Quarter By Drive Manufacturer (PB):\n")

    output_rows: list[str] = []

    prev_year_qtr: tuple[int, int] = (0, 0)
    prev_year_data: list[tuple[str, float]] = []
    prev_year_pb: float = 0

    for curr_row in quarterly_raw_storage_capacity_dataframe.iter_rows():
        year, quarter, drive_mfr, raw_capacity_pb = curr_row

        if (year, quarter) != prev_year_qtr:
            # Do we have data to output?
            if prev_year_data:
                detailed_data: list[str] = []
                for curr_mfr_qtr in prev_year_data:
                    detailed_data.append(f"{curr_mfr_qtr[0]} = {curr_mfr_qtr[1]:8,.01f} "
                                         f"({curr_mfr_qtr[1] / prev_year_pb * 100.0:3.0f}%)")
                output_rows.append(f"\t{prev_year_qtr[0]} Q{prev_year_qtr[1]} (Total = {prev_year_pb:8,.01f}):\t" +
                                    "    ".join(detailed_data) )
                prev_year_data.clear()
                prev_year_pb = 0

            prev_year_qtr = (year, quarter)

            # If we've rolled into a new year, add a blank line
            if quarter == 1:
                output_rows.append("")

        prev_year_data.append( (drive_mfr, raw_capacity_pb) )
        prev_year_pb += raw_capacity_pb

    if prev_year_data:
        detailed_data: list[str] = []
        for curr_mfr_qtr in prev_year_data:
            detailed_data.append(f"{curr_mfr_qtr[0]} = {curr_mfr_qtr[1]:8,.01f} "
                                 f"({curr_mfr_qtr[1] / prev_year_pb * 100.0:3.0f}%)")
        output_rows.append(f"\t{prev_year_qtr[0]} Q{prev_year_qtr[1]} (Total = {prev_year_pb:8,.01f}):\t" +
                                    "    ".join(detailed_data) )

    # Show from newest to oldest
    for curr_output_row in reversed(output_rows):
        print(curr_output_row)


if __name__ == "__main__":
    _main()

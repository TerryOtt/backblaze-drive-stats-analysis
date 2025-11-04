import argparse
import re
import sys
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


# def _correct_source_lazyframe_drive_model_capacity(source_lazyframe: polars.LazyFrame) -> polars.LazyFrame:
#
#     drive_model_tb_corrections: dict[str, float] = {
#
#         r'^MTFDDAV240TCB$'                      :  0.240,
#
#         r'^CT250MX500SSD1$'                     :  0.250,
#         r'\b250\s?GB$'                          :  0.250,
#         r'^Seagate BarraCuda 120 SSD ZA250'     :  0.250,
#         r'^Seagate IronWolf ZA250'              :  0.250,
#         r'^WDC WDS250G'                         :  0.250,
#         r'^Seagate BarraCuda SSD ZA250'         :  0.250,
#
#         r'^DELLBOSS VD$'                        :  0.480,
#         r'^MTFDDAV480TCB$'                      :  0.480,
#
#         r'^Seagate BarraCuda 120 SSD ZA500'     :  0.500,
#         r'^Seagate FireCuda 120 SSD ZA500'      :  0.500,
#
#         r'^WDC WD10E'                           :  1.000,
#
#         r'\bH[DM]S5C4040'                       :  4.001,
#         r'\b[HW]U[HS]726040'                    :  4.001,
#         r'^ST4000[A-Z]{2}'                      :  4.001,
#         r'^TOSHIBA MD04ABA400V$'                :  4.001,
#
#         r'^ST6000[A-Z]{2}'                      :  6.001,
#         r'^WDC WD60E'                           :  6.001,
#
#         r'\b[HW]U[HS]728080'                    :  8.002,
#         r'\b[HW]U[HS]728T8T'                    :  8.002,
#         r'^ST8000[A-Z]{2}'                      :  8.002,
#         r'^TOSHIBA HDWF180$'                    :  8.002,
#
#         r'\b[HW]U[HS]721010'                    : 10.001,
#         r'^ST10000[A-Z]{2}'                     : 10.001,
#
#         r'\b[HW]U[HS]721212'                    : 12.000,
#         r'^ST12000[A-Z]{2}'                     : 12.000,
#
#         r'^ST14000[A-Z]{2}'                     : 14.001,
#         r'^TOSHIBA MG\d{2}ACA14T'               : 14.001,
#         r'\b[HW]U[HS]72\d{2}14'                 : 14.001,
#
#         r'^ST16000[A-Z]{2}'                     : 16.001,
#         r'^TOSHIBA MG\d{2}ACA16T'               : 16.001,
#         r'\b[HW]U[HS]72\d{2}16'                 : 16.001,
#
#         r'^ST18000[A-Z]{2}'                     : 18.000,
#
#         r'^TOSHIBA MG\d{2}ACA20T'               : 20.001,
#
#         r'\b[HW]U[HS]72\d{2}2'                  : 22.001,
#
#         r'^ST24000[A-Z]{2}'                     : 24.000,
#     }
#
#     # Get all the models with sizes that aren't real
#     bad_boy_models: list[str] = source_lazyframe.filter(
#         # LT 1 GB                                             GT 40 TB
#         polars.col("capacity_bytes").lt(1000 * 1000 * 1000) | polars.col("capacity_tb").gt(40.0)
#     ).select(
#         "model_name",
#     ).unique().collect().get_column("model_name").to_list()
#
#     # Create size correction lists
#     update_model_name: list[str] = []
#     update_capacity_tb: list[float] = []
#     for curr_bad_boy in bad_boy_models:
#         found_capacity: bool = False
#         for match_regex, match_capacity_tb in drive_model_tb_corrections.items():
#             if re.search(match_regex, curr_bad_boy):
#                 update_model_name.append(curr_bad_boy)
#                 update_capacity_tb.append(match_capacity_tb)
#                 found_capacity = True
#                 break
#
#         # If we didn't find a mapping, blow up the world
#         if not found_capacity:
#             raise ValueError(f"Could not find correct capacity for model {curr_bad_boy}")
#
#     corrected_data_lazyframe: polars.LazyFrame = polars.LazyFrame(
#         {
#             "model_name"    : update_model_name,
#             "capacity_tb"   : update_capacity_tb,
#         }
#     )
#
#     updated_lazyframe: polars.LazyFrame = source_lazyframe.update(
#         corrected_data_lazyframe,
#         on="model_name",
#         how="full",
#     )
#
#     return updated_lazyframe


def _get_source_lazyframe(args: argparse.Namespace) -> polars.LazyFrame:
    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    print( etl_pipeline.next_stage_banner() )

                          # KB     MB     GB     TB
    bytes_per_tb: float = 1000 * 1000 * 1000 * 1000

    # Reduce to columns we care about
    source_lazyframe = source_lazyframe.select(
        "date",
        polars.col("model").alias("model_name"),
        "capacity_bytes",
        (polars.col("capacity_bytes") / bytes_per_tb).alias("capacity_tb"),
        "serial_number",

    # Remove rows with bad drive capacity data reported by SMART
    ).filter(
        polars.col("capacity_tb").is_between(0.001, 25.000)
    )
    # print(source_lazyframe.collect_schema())

    # Add logic so data streamed through the dataframe has any bunk byte capacities fixed
    # source_lazyframe = _correct_source_lazyframe_drive_model_capacity(source_lazyframe)

    print("\tCompleted")

    return source_lazyframe


def _get_materialized_quarterly_storage_capacity(source_lazyframe: polars.LazyFrame) -> polars.DataFrame:
    print(etl_pipeline.next_stage_banner())
    stage_begin: float = time.perf_counter()
    print("\tMaterializing quarterly raw storage capacity from Apache Iceberg on Backblaze B2 using Polars...")

    tb_per_pb: float = 1000.0
    pb_per_eb: float = 1000.0

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = source_lazyframe.group_by(
        polars.col("date").dt.year().alias("year"),
        polars.col("date").dt.quarter().alias("quarter"),
        "model_name",
        "capacity_tb",
    ).agg(
        polars.col("serial_number").unique().count().alias("unique_sn_per_quarter_and_model")
    ).with_columns(
        (polars.col("capacity_tb") * polars.col("unique_sn_per_quarter_and_model") / tb_per_pb).alias(
            "total_pb_for_model" )
    ).group_by(
        "year", "quarter"
    ).agg(
        polars.col("total_pb_for_model").sum().alias("raw_storage_pb"),
    ).with_columns(
        (polars.col("raw_storage_pb") / pb_per_eb).alias("raw_storage_eb"),
    ).sort(
    "year",
    "quarter"
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

    # Note: any models read with incorrect capacities will be corrected by the logic in this source
    #       lazyframe
    source_lazyframe: polars.LazyFrame = _get_source_lazyframe(args)

    quarterly_raw_storage_capacity_dataframe: polars.DataFrame = _get_materialized_quarterly_storage_capacity(
        source_lazyframe)

    print("\nBackblaze Raw Storage Capacity By Quarter:\n")

    for curr_row in quarterly_raw_storage_capacity_dataframe.iter_rows():
        year, quarter, raw_capacity_pb, raw_capacity_eb = curr_row

        print(f"\t{year} Q{quarter}: {raw_capacity_pb:6,.0f} petabytes (PB) / "
              f"{raw_capacity_eb:4.01f} exabytes (EB)")

    raise NotImplementedError("TODO: need to investigate 2018 Q1 with twice the storage it should have")


if __name__ == "__main__":
    _main()

import argparse
import polars

import backblaze_drive_stats_data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create and populate Postgres data from Drive Stats Iceberg")

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

    #parser.add_argument("input_parquet_file", help="Path to parquet file we read from")

    parser.add_argument("b2_access_key",
                        help="Backblaze B2 Access Key")
    parser.add_argument("b2_secret_access_key",
                        help="Backblaze B2 Secret Access Key")

    # TODO: need a CSV or PG creds to create the DB with

    return parser.parse_args()


def _find_and_add_drive_models(args: argparse.Namespace, source_lazyframe: polars.LazyFrame) -> None:
    drive_model_dataframe: polars.DataFrame = source_lazyframe.select(
        "model",
        "capacity_bytes"
    ).group_by(
        "model"
    ).agg(
        polars.col("capacity_bytes").mode().first().alias("capacity_bytes_mode"),
    ).select(
        polars.col("model").alias("drive_model_smart"),
        ( polars.col("capacity_bytes_mode") / 1000 / 1000 / 1000 / 1000 ).alias("capacity_tb"),
    ).sort(
        "drive_model_smart"
    ).collect()

    print( drive_model_dataframe )


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    # Get and add all models
    _find_and_add_drive_models(args, source_lazyframe)

    # Get and add all drives

    # Get daily info and add it



if __name__ == "__main__":
    _main()

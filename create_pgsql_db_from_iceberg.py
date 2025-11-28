import argparse
import os
import polars
import psycopg2
import psycopg2.extensions

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

    parser.add_argument("b2_access_key",
                        help="Backblaze B2 Access Key")
    parser.add_argument("b2_secret_access_key",
                        help="Backblaze B2 Secret Access Key")

    return parser.parse_args()


def _db_connect():
    db_details = {
        'db_name'       : os.environ['PGDATABASE'],
        'db_user'       : os.environ['PGUSER'],
        'db_host'       : os.environ['PGHOST'],
        'db_password'   : os.environ['PGPASSWORD'],
    }

    return psycopg2.connect(f"dbname='{db_details['db_name']}' user='{db_details['db_user']}' "
                            f"host='{db_details['db_host']}' password='{db_details['db_password']}'")


def _find_and_add_drives(source_lazyframe: polars.LazyFrame,
                         db_cursor: psycopg2.extensions.cursor) -> None:

    db_cursor.execute(
        "SELECT drive_model_name_smart, drive_model_id_pk FROM drive_models;"
    )
    drive_model_smart_to_model_ids: list[dict[str, str]] = []
    for curr_row in db_cursor:
        drive_model_smart_to_model_ids.append(
            {
                "drive_model_smart": curr_row[0],
                "drive_model_id": curr_row[1]
            }
        )
    drive_model_name_ids: polars.DataFrame = polars.from_dicts(drive_model_smart_to_model_ids)

    drives_dataframe: polars.DataFrame = source_lazyframe.group_by(
        "serial_number"
    ).agg(
        polars.col("model").mode().first().alias("drive_model_smart"),
    ).collect().join(
        drive_model_name_ids,
        on="drive_model_smart",
    )

    for serial_number, _, drive_model_id in drives_dataframe.iter_rows():
        db_cursor.execute(
            "INSERT INTO drives (drive_model_fk, drive_serial_number) VALUES (%s, %s);",
            (drive_model_id, serial_number)
        )


def _find_and_add_drive_models(source_lazyframe: polars.LazyFrame,
                               db_cursor: psycopg2.extensions.cursor) -> None:

    drive_model_dataframe: polars.DataFrame = source_lazyframe.group_by(
        "model"
    ).agg(
        polars.col("capacity_bytes").mode().first().alias("capacity_bytes_mode"),
    ).select(
        polars.col("model").alias("drive_model_smart"),
        ( polars.col("capacity_bytes_mode") / 1000 / 1000 / 1000 / 1000 ).alias("capacity_tb"),
    ).collect()

    for drive_model_name_smart, drive_model_capacity_tb in drive_model_dataframe.iter_rows():
        db_cursor.execute(
            "INSERT INTO drive_models (drive_model_name_smart, drive_model_size_tb) VALUES (%s, %s);",
            (drive_model_name_smart, drive_model_capacity_tb)
        )


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    source_lazyframe: polars.LazyFrame = backblaze_drive_stats_data.source_lazyframe(args)

    # Connect to PGSQL
    with _db_connect() as db_handle:
        with db_handle.cursor() as db_cursor:

            # Get and add all models
            _find_and_add_drive_models(source_lazyframe, db_cursor)

            # Get and add all drives
            _find_and_add_drives(source_lazyframe, db_cursor)

            # Get daily info and add it



if __name__ == "__main__":
    _main()

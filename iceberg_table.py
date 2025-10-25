import s3fs


def current_metadata_file_s3_uri( b2_access_key: str,
                                  b2_secret_access_key: str,
                                  s3_endpoint: str,
                                  s3_bucket_name: str,
                                  table_path: str) -> str:

    s3_handle: s3fs.S3FileSystem = s3fs.S3FileSystem(
        key=b2_access_key,
        secret=b2_secret_access_key,
        endpoint_url=s3_endpoint
    )

    bucket_path: str = f"{s3_bucket_name}/{table_path}/metadata"

    files = s3_handle.ls(bucket_path)

    metadata_json_files: list[str] = []

    for curr_file in files:
        if curr_file.endswith(".metadata.json"):
            metadata_json_files.append(curr_file)

    # Latest metadata is latest rev, so end of sorted list
    latest_metadata_json_file: str = f"s3://{sorted(metadata_json_files)[-1]}"

    return latest_metadata_json_file
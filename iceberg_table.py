import s3fs


def metadata_files(b2_access_key: str,
                   b2_secret_access_key: str,
                   s3_endpoint: str,
                   s3_bucket_name: str,
                   table_path: str) -> list[str]:

    s3_handle: s3fs.S3FileSystem = s3fs.S3FileSystem(
        key=b2_access_key,
        secret=b2_secret_access_key,
        client_kwargs={
            'endpoint_url': s3_endpoint,
        },
    )

    bucket_path: str = f"{s3_bucket_name}/{table_path}/metadata"

    files = s3_handle.ls(bucket_path)

    metadata_json_files: list[str] = []

    for curr_file in files:
        if curr_file.endswith(".metadata.json"):
            metadata_json_files.append(curr_file)

    return sorted(metadata_json_files)


def current_metadata_file_s3_uri( b2_access_key: str,
                                  b2_secret_access_key: str,
                                  s3_endpoint: str,
                                  s3_bucket_name: str,
                                  table_path: str) -> str:

    # Latest metadata is latest rev, so end of sorted list
    latest_metadata_json_file: str = f"s3://{metadata_files(b2_access_key,
                                                            b2_secret_access_key,
                                                            s3_endpoint,
                                                            s3_bucket_name,
                                                            table_path)[-1]}"

    return latest_metadata_json_file

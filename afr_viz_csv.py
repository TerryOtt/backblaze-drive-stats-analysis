import argparse
import polars


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Create quarterly AFR visualization CSV")
    parser.add_argument('drive_patterns_json', help='Path to JSON with drive regexes')
    parser.add_argument("parquet_file", help="Path to parquet file")
    return parser.parse_args()

def _get_drive_model_mapping(original_source_lazyframe: polars.LazyFrame) -> dict[str, list[str]]:
    drive_model_mapping: dict[str, list[str]] = {}

    # We want all unique drive model names
    drive_models_series: polars.Series = \
        original_source_lazyframe.select("model").collect().to_series().unique().sort()

    # Iterate over drive models in Series to create normalized name -> [raw names] mappings
    for current_drive_model in drive_models_series:
        print(f"Found drive model \"{current_drive_model}\"")

    return drive_model_mapping


def _main() -> None:
    args: argparse.Namespace = _parse_args()
    original_source_lazyframe: polars.LazyFrame = polars.scan_parquet(args.parquet_file)
    _get_drive_model_mapping(original_source_lazyframe)


if __name__ == "__main__":
    _main()

import argparse
import csv
import datetime
import json
import re


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert AFR CSV to human-readable CSV")
    parser.add_argument("trino_csv", help="Path to CSV with raw data from Trino")
    parser.add_argument("human_readable_csv", help="Path to human-readable CSV")

    return parser.parse_args()


def _read_trino_csv(args: argparse.Namespace) -> dict[str, dict[str, dict[str, int]]]:
    known_models: set[str] = set()
    trino_data_by_drive_model: dict[str, dict[str, dict[str, int]]] = {}
    with open(args.trino_csv, "r") as afr_csv:
        csv_reader:csv.DictReader[str] = csv.DictReader(afr_csv)

        for curr_csv_row in csv_reader:
            cleaned_model: str = _clean_drive_model_str(curr_csv_row['model'])
            # Is this a new drive model?
            if cleaned_model not in known_models:
                #print(f"Found new model {cleaned_model}")
                known_models.add(cleaned_model)
                model_day_index = 1
                trino_data_by_drive_model[cleaned_model] = {}

            row_date: str = datetime.date.fromisoformat(curr_csv_row['date']).isoformat()

            # Is this a new date for this drive?
            if row_date not in trino_data_by_drive_model[cleaned_model]:
                trino_data_by_drive_model[cleaned_model][row_date] = {
                    'drive_count'   : 0,
                    'failure_count' : 0,
                }

            trino_data_by_drive_model[cleaned_model][row_date]['drive_count'] += \
                int(curr_csv_row['drive_count'])
            trino_data_by_drive_model[cleaned_model][row_date]['failure_count'] += \
                int(curr_csv_row['failure_count'])

    return trino_data_by_drive_model


def _clean_drive_model_str(drive_model_str: str) -> str:
    """Clean up whitespace and deal with inconsistent model names.
    :param drive_model_str: Drive model string from CSV
    :return: model string with all extra whitespace removed
    """
    clean_str: str = re.sub(r'\s+', ' ', drive_model_str.strip())
    model_string_tokens: list[str] = clean_str.split(' ')

    # If there are multiple tokens and the final token matches a drive of interest,
    #    use the final token as the drive model as the data isn't consistent.
    #
    # Example: raw data includes both "WUH721816ALE6L4" and "WDC WUH721816ALE6L4" models,
    #          which are obviously the same thing. WOMP WOMP.
    if len(model_string_tokens) > 1:
        # Compare last token to regular expressions that are drives of interest
        # 	If the regular expression matches, use final token as drive model
        drive_models_of_interest_patterns: tuple[str, ...] = (
            r'[HW]UH72\d+',
            r'ST\d+NM',
            r'MG\d{2}',
        )

        # re.match only checks for matches at the start of the string, hence no need for ^ or \b
        for curr_pattern in drive_models_of_interest_patterns:
            if re.match(curr_pattern, model_string_tokens[-1]):
                return model_string_tokens[-1]

    return clean_str


def _main():
    args: argparse.Namespace = _parse_args()

    trino_data: dict[str, dict[datetime.date, dict[str, int]]] = _read_trino_csv(args)

    print(json.dumps(trino_data, indent=4, sort_keys=True))


if __name__ == "__main__":
    _main()

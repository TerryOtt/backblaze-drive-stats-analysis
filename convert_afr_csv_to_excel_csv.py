import argparse
import csv
import json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert raw Trino CSV to Excel CSV")
    parser.add_argument("trino_csv", help="Path to Trino CSV")
    parser.add_argument("excel_csv", help="Path to Excel CSV")

    return parser.parse_args()

def _convert_afr_csv_to_excel(args: argparse.Namespace) -> dict[int, dict[str, float]]:
    excel_data: dict[int, dict[str, float]] = {}
    with open(args.trino_csv, "r") as trino_csv:
        csv_reader:csv.DictReader[str] = csv.DictReader(trino_csv)

        for curr_csv_line in csv_reader:
            day_index: int = int(curr_csv_line['day_index'])
            drive_model: str = curr_csv_line["drive_model"]
            # Does day index exist in excel data?
            if day_index not in excel_data:
                excel_data[day_index]: dict[str, float] = {}

            # Append AFR for this day
            excel_data[day_index][drive_model] = float(curr_csv_line['annualized_failure_rate_percent'])

    return excel_data


def _write_excel_data_to_csv(excel_data: dict[int, dict[str, float]], args: argparse.Namespace) -> None:
    for day_index_str in sorted(excel_data):
        day_index_int: int = int(day_index_str)
        print(f"Day index: {day_index_int}")

def _main():
    args: argparse.Namespace = _parse_args()
    excel_data: dict[int, dict[str, float]] = _convert_afr_csv_to_excel(args)
    #print(json.dumps(excel_data, indent=4, sort_keys=True))
    _write_excel_data_to_csv(excel_data, args)

if __name__ == "__main__":
    _main()
import argparse
import csv


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert raw Trino CSV to Excel CSV")
    parser.add_argument("trino_csv", help="Path to Trino CSV")
    parser.add_argument("excel_csv", help="Path to Excel CSV")

    return parser.parse_args()


def _convert_afr_csv_to_excel(args: argparse.Namespace) -> tuple[list[str], dict[int, dict[str, float]]]:
    drive_model_column_names: set[str] = set()
    excel_data: dict[int, dict[str, float]] = {}

    with open(args.trino_csv, "r") as trino_csv:
        csv_reader:csv.DictReader[str] = csv.DictReader(trino_csv)

        for curr_csv_line in csv_reader:
            day_index: int = int(curr_csv_line['day_index'])
            drive_model: str = curr_csv_line['drive_model']

            if drive_model not in drive_model_column_names:
                drive_model_column_names.add(drive_model)

            # Does day index exist in excel data?
            if day_index not in excel_data:
                excel_data[day_index]: dict[str, float] = {}

            # Append AFR for this day
            excel_data[day_index][drive_model] = float(curr_csv_line['annualized_failure_rate_percent'])

    sorted_column_names: list[str] = list(sorted(drive_model_column_names))

    return sorted_column_names, excel_data


def _create_excel_csv(
        args: argparse.Namespace,
        drive_model_column_names: list[str],
        excel_data: dict[int, dict[str, float]] ) -> None:

    csv_columns: list[str] = [
        'day_index',
    ]

    # Populate rest of column names
    csv_columns.extend(drive_model_column_names)

    with open(args.excel_csv, "w") as excel_csv_handle:
        csv_writer: csv.DictWriter = csv.DictWriter(excel_csv_handle, fieldnames=csv_columns)

        # Write header row
        csv_writer.writeheader()

        for day_index in sorted(excel_data):

            data_row: dict[str, int | float] = {
                'day_index': day_index,
            }

            for curr_drive_model in drive_model_column_names:
                # Does Excel data have AFR data for this drive on the given day?
                if curr_drive_model in excel_data[day_index]:
                    data_row[curr_drive_model] = excel_data[day_index][curr_drive_model]

            csv_writer.writerow(data_row)


def _main():
    args: argparse.Namespace = _parse_args()
    returned_tuple: tuple[list[str], dict[int, dict[str, float]]] = _convert_afr_csv_to_excel(args)
    drive_model_column_names: list[str] = returned_tuple[0]
    excel_data: dict[int, dict[str, float]] = returned_tuple[1]
    _create_excel_csv(args, drive_model_column_names, excel_data)


if __name__ == "__main__":
    _main()

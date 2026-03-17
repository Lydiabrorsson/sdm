import json
import csv
from pathlib import Path

# Folder with your processed JSON files
INPUT_DIR = Path("data/processed")

# Folder where the CSVs will be written
OUTPUT_DIR = Path("data/import_ready")
OUTPUT_DIR.mkdir(exist_ok=True)


def load_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_csv(file_path, records):

    columns = list(records[0].keys())

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for record in records:
            writer.writerow(record)

def convert_all_json_to_csv():
    json_files = sorted(INPUT_DIR.glob("*.json"))

    for json_file in json_files:
        try:
            data = load_json_file(json_file)

            csv_name = json_file.stem + ".csv"
            csv_path = OUTPUT_DIR / csv_name

            write_csv(csv_path, data)
            print(f"Converted: {json_file.name} -> {csv_name}")

        except Exception as e:
            print(f"Error converting {json_file.name}: {e}")


if __name__ == "__main__":
    convert_all_json_to_csv()
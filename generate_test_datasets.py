import json
import os
import random

INPUT_FILE = "dataset_5000MB.json"

SIZES_MB = [512, 1024, 2048, 3072, 5120]  # Sizes in MB to generate

# Directory to save outputs
OUTPUT_DIR = "generated_datasets"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Loading original records...")
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    original_records = [line.rstrip() for line in f]

print(f"Loaded {len(original_records)} records.")

for size_mb in SIZES_MB:
    target_bytes = size_mb * 1024 * 1024

    output_path = os.path.join(OUTPUT_DIR, f"dataset_{size_mb}MB.json")
    total_bytes = 0
    num_records = 0

    with open(output_path, "w", encoding="utf-8") as out_file:
        while total_bytes < target_bytes:
            # Pick a random record
            record = random.choice(original_records)
            encoded_line = record + "\n"
            line_size = len(encoded_line.encode("utf-8"))

            out_file.write(encoded_line)
            total_bytes += line_size
            num_records += 1

    print(
        f"Wrote {output_path} - "
        f"{num_records} records, {total_bytes / (1024 * 1024):.2f} MB"
    )

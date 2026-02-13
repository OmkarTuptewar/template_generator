import ijson
import csv

INPUT_FILE = "/Users/int1964/TEMPLATE_GENRATOR/gt_querybank_data_dump.json"
OUTPUT_FILE = "data.csv"

with open(INPUT_FILE, "r", encoding="utf-8") as infile, \
     open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:

    writer = csv.writer(outfile)

    # CSV Header
    writer.writerow(["query", "templatized_query", "lob"])

    # ijson reads array elements one-by-one
    objects = ijson.items(infile, "item")

    count = 0

    for obj in objects:
        writer.writerow([
            obj.get("query", ""),
            obj.get("templatized_query", ""),
            obj.get("lob", "")
        ])

        count += 1
        if count % 10000 == 0:
            print(f"{count} rows processed...")

print("✅ Conversion completed!")

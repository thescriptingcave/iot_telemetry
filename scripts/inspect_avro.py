import sys
import fastavro

if len(sys.argv) < 2:
    print("Usage: python inspect_avro.py <file.avro> [limit]")
    sys.exit(1)

filename = sys.argv[1]
limit = int(sys.argv[2]) if len(sys.argv) > 2 else 3

with open(filename, "rb") as f:
    reader = fastavro.reader(f)
    for i, rec in enumerate(reader):
        print(rec)
        if i >= limit - 1:
            print(f"... (stopped at {limit} records)")
            break

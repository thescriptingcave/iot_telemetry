# generate_humidity_data.py
import fastavro
from fastavro import writer, parse_schema
import json
import random
from datetime import datetime, timezone

# Load schema from .avsc file
with open("humidity.avsc") as f:
    schema = parse_schema(json.load(f))

# Generate 5 sample records
records = [
    {
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),  # ms since epoch
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "sensor_id": f"sensor_{random.randint(1, 10)}"
    }
    for _ in range(5)
]

# Write to .avro file
with open("humidity_test.avro", "wb") as out:
    writer(out, schema, records)

print("✅ Created humidity_test.avro")

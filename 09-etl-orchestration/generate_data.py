"""Generate a deliberately messy IoT sensor dataset for the ETL practice.

Produces ~100k rows covering ~5 days of readings from ~200 sensors across a
handful of locations. The dataset is seeded with realistic data-quality issues:
nulls, duplicates, out-of-range values, inconsistent timestamp formats, and
typos. The kind of mess a pipeline has to clean up.

Run: python generate_data.py
Output: /app/data/sensors_raw.csv
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUT = Path("/app/data/sensors_raw.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

NUM_SENSORS = 200
NUM_DAYS = 5
READINGS_PER_DAY = 100  # per sensor

LOCATIONS_CLEAN = ["warehouse-a", "warehouse-b", "office-1", "office-2", "coldroom"]
# Typos and casing variants we'll sprinkle in
LOCATION_VARIANTS = {
    "warehouse-a": ["warehouse-a", "Warehouse-A", "warehouse_a", "warehousea"],
    "warehouse-b": ["warehouse-b", "WAREHOUSE-B", "warehouse-b "],
    "office-1": ["office-1", "Office-1", "office 1"],
    "office-2": ["office-2", "office-2", "Office2"],
    "coldroom": ["coldroom", "cold-room", "COLDROOM"],
}
STATUSES = ["ok", "warning", "error"]
STATUS_VARIANTS = ["ok", "OK", "warning", "error", "ERR", "unknown", ""]

sensor_ids = [str(uuid.uuid4()) for _ in range(NUM_SENSORS)]
sensor_location = {s: random.choice(LOCATIONS_CLEAN) for s in sensor_ids}

start = datetime(2026, 4, 14, 0, 0, 0)
rows = []

for sensor_id in sensor_ids:
    loc_canonical = sensor_location[sensor_id]
    for day in range(NUM_DAYS):
        for r in range(READINGS_PER_DAY):
            ts = start + timedelta(
                days=day,
                minutes=r * 14 + random.randint(0, 3),
            )
            # Timestamp format: mostly ISO, sometimes epoch seconds
            if random.random() < 0.15:
                ts_str = str(int(ts.timestamp()))
            else:
                ts_str = ts.isoformat()

            # Location: mostly canonical, sometimes a typo/casing variant
            if random.random() < 0.2:
                location = random.choice(LOCATION_VARIANTS[loc_canonical])
            else:
                location = loc_canonical

            # Temperature: usually reasonable, sometimes null, sometimes wild
            temp = random.gauss(22, 4)
            if loc_canonical == "coldroom":
                temp = random.gauss(4, 1.5)
            if random.random() < 0.05:
                temp = None
            elif random.random() < 0.02:
                temp = random.choice([-999.0, 9999.0, 500.0, -200.0])

            # Humidity: usually 30-70, sometimes null or impossible
            hum = random.gauss(50, 10)
            if random.random() < 0.04:
                hum = None
            elif random.random() < 0.015:
                hum = random.choice([-10.0, 150.0, 200.0])

            # Status: mostly canonical, sometimes variant/invalid
            if random.random() < 0.15:
                status = random.choice(STATUS_VARIANTS)
            else:
                status = random.choices(STATUSES, weights=[0.85, 0.1, 0.05])[0]

            # Occasional null sensor_id ;  should be quarantined
            sid = sensor_id
            if random.random() < 0.01:
                sid = None

            rows.append({
                "sensor_id": sid,
                "timestamp": ts_str,
                "temperature": f"{temp:.2f}" if temp is not None else "",
                "humidity": f"{hum:.2f}" if hum is not None else "",
                "location": location,
                "status": status,
            })

# Inject exact duplicates (~1.5% of rows)
num_exact_dupes = int(len(rows) * 0.015)
for _ in range(num_exact_dupes):
    rows.append(dict(random.choice(rows)))

# Inject (sensor_id, timestamp) collisions with different readings (~0.8%)
num_key_dupes = int(len(rows) * 0.008)
for _ in range(num_key_dupes):
    base = random.choice(rows)
    if base["sensor_id"] is None:
        continue
    dup = dict(base)
    dup["temperature"] = f"{random.gauss(22, 4):.2f}"
    dup["humidity"] = f"{random.gauss(50, 10):.2f}"
    rows.append(dup)

random.shuffle(rows)

with OUT.open("w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["sensor_id", "timestamp", "temperature", "humidity", "location", "status"],
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows):,} rows to {OUT}")

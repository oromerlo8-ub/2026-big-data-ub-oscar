# ETL Pipelines & Orchestration Practice

## Setup

From your local git repository:

```bash
cd 09-etl-orchestration
docker compose up -d --build
```

Two services start:

- **Jupyter** → http://localhost:8888 (for Part 1)
- **Airflow** → http://localhost:8080 (for Part 2)

Spark UI appears at http://localhost:4040 once you start a SparkSession.

---

## Data

A synthetic, deliberately messy IoT sensor dataset. Generate it with:

```bash
docker compose exec jupyter python generate_data.py
```

This writes `./data/sensors_raw.csv` (~100k rows, ~6 MB). The data is seeded with:

- Nulls in `sensor_id`, `temperature`, `humidity`
- Timestamp formats mixed between ISO-8601 and Unix epoch seconds
- Location typos and casing variants (`warehouse-a` vs `Warehouse-A` vs `warehouse_a`)
- Out-of-range readings (`temperature=9999`, `humidity=-10`)
- Exact duplicate rows
- `(sensor_id, timestamp)` collisions with different readings
- Invalid `status` values

This is the kind of mess your pipeline has to handle.

---

## Notebook Setup

From http://localhost:8888, create a new notebook under `/app/notebooks/`. Use this as the first cell:

```python
import os, shutil, time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, coalesce, lower, trim,
    to_timestamp, to_date, from_unixtime, regexp_replace,
    count, sum as _sum, countDistinct, row_number,
)
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("ETLPractice")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
```

Load the raw data but *not* with Spark's CSV reader directly. Spark's CSV reader converts empty cells to null at read time and there's no clean way to turn that off.
To see the distinction between "cell was empty" and "value was null," read with PyArrow first (which lets you control null conversion explicitly), then hand the Arrow table off to Spark.

```python
import pyarrow.csv as pcsv

arrow_table = pcsv.read_csv(
    "/app/data/sensors_raw.csv",
    convert_options=pcsv.ConvertOptions(
        null_values=[],              # no string matches null
        strings_can_be_null=False,   # empty string stays empty, not null
    ),
)

raw = spark.createDataFrame(arrow_table.to_pandas())

raw.printSchema()
print(f"Rows: {raw.count():,}")
raw.show(5, truncate=False)
```

This is the Arrow interop pattern from Session 03: Arrow as a neutral columnar format between tools, with fine-grained control over type and null handling that Spark's CSV reader alone doesn't give us.

---

## Part 1: Building the ETL Pipeline (55 min)

### Exercise 1: Explore the Mess (10 min)

Before cleaning anything, we need to know what's broken.

**Q1.** Count rows, distinct `sensor_id`s, and how many rows have `sensor_id` missing (null or empty string).

<details>
<summary>Answer</summary>

```python
total = raw.count()
missing_sid = raw.filter(col("sensor_id").isNull() | (col("sensor_id") == "")).count()
distinct_sid = raw.select("sensor_id").distinct().count()

print(f"Total rows:          {total:,}")
print(f"Distinct sensors:    {distinct_sid:,}")
print(f"Missing sensor_id:   {missing_sid:,}")
```

Sensor_id missings are the easy-to-kill kind of bad data: no ID → no point keeping the row.
</details>

---

**Q2.** For each column, count the nulls **and** the empty strings separately. Which columns are affected? Where does the distinction matter?

<details>
<summary>Hint</summary>

```python
for c in raw.columns:
    nulls = raw.filter(col(c).isNull()).count()
    empties = raw.filter(col(c) == "").count()
    print(f"{c:14s}  null={nulls:6d}  empty={empties:6d}")
```
</details>

<details>
<summary>Answer & discussion</summary>

You should see nulls at zero across the board and empties concentrated in `sensor_id`, `temperature`, `humidity`, and `status`. That's because we read with **PyArrow** and told it explicitly `strings_can_be_null=False` ;  Arrow kept empty cells as empty strings.

**The gotcha**: if we had read with `spark.read.csv(...)` directly, every empty cell would have been silently converted to null. The distinction between *"the source system wrote an empty string"* and *"the source system wrote a null"* would have been erased at read time. In production ;  especially when different upstream systems write `""`, `NULL`, `N/A`, `\N`, or just leave the cell blank ;  that distinction can matter: "never populated" vs "explicitly cleared" vs "field not present."

**Rule of thumb**: when reading dirty CSVs you don't control, pick a reader that lets you be explicit about null handling (PyArrow, Polars) rather than one that silently decides for you (Spark's default, Pandas' default).
</details>

---

**Q3.** The `timestamp` column is mixed-format ;  some rows are ISO-8601 (`2026-04-14T00:07:31`) and some are Unix epoch seconds (`1776374851`). Estimate how many of each. (Hint: ISO timestamps contain a `-` or `T`, epoch seconds don't.)

<details>
<summary>Answer</summary>

```python
iso_count   = raw.filter(col("timestamp").contains("T")).count()
epoch_count = raw.filter(~col("timestamp").contains("-")).count()

print(f"ISO-like:    {iso_count:,}")
print(f"Epoch-like:  {epoch_count:,}")
```

The counts won't sum exactly to total ;  the heuristics overlap. We'll write a proper parser in Exercise 2.
</details>

---

**Q4.** Count exact duplicate rows. Then count `(sensor_id, timestamp)` duplicates with **different** readings (harder ;  these can't be dropped blindly).

<details>
<summary>Answer</summary>

```python
exact_dupes = raw.count() - raw.dropDuplicates().count()

key_dupe_groups = (raw
    .filter(col("sensor_id").isNotNull())
    .groupBy("sensor_id", "timestamp")
    .count()
    .filter(col("count") > 1))

print(f"Exact duplicate rows:             {exact_dupes:,}")
print(f"(sensor_id, timestamp) collisions: {key_dupe_groups.count():,}")
```

Exact duplicates → drop. Key collisions with different readings → need a policy (keep first / keep last / average). We'll decide in Exercise 2.
</details>

---

### Exercise 2: Clean the Data (15 min)

**Q5.** Build a cleaned DataFrame `clean` that:

1. Drops rows where `sensor_id` is null or empty
2. Parses the mixed `timestamp` column into a proper `TimestampType` column called `ts`
3. Casts `temperature` and `humidity` to `DoubleType` (empty strings become null, which is correct)
4. Normalises `location` to lowercase, stripped, with `_` and spaces replaced by `-`
5. Normalises `status` to lowercase, stripped

<details>
<summary>Hint</summary>

For the mixed timestamp, use `when()`:

```python
.withColumn("ts",
    when(col("timestamp").contains("-"),
         to_timestamp(col("timestamp")))
    .otherwise(from_unixtime(col("timestamp").cast("long")).cast("timestamp")))
```
</details>

<details>
<summary>Answer</summary>

```python
clean = (raw
    # 1. Drop null / empty sensor_id
    .filter(col("sensor_id").isNotNull() & (col("sensor_id") != ""))

    # 2. Parse mixed timestamps
    .withColumn("ts",
        when(col("timestamp").contains("-"),
             to_timestamp(col("timestamp")))
        .otherwise(from_unixtime(col("timestamp").cast("long")).cast("timestamp")))

    # 3. Cast numeric columns
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("humidity",    col("humidity").cast("double"))

    # 4. Normalise location
    .withColumn("location",
        regexp_replace(lower(trim(col("location"))), "[_ ]", "-"))

    # 5. Normalise status
    .withColumn("status", lower(trim(col("status"))))

    .drop("timestamp"))  # replaced by `ts`

clean.printSchema()
clean.show(5)
```

Verify the timestamp parse didn't silently fail:

```python
print(f"Null ts after parse: {clean.filter(col('ts').isNull()).count()}")
```
</details>

---

**Q6.** Drop exact duplicates from `clean`. Then, for `(sensor_id, ts)` collisions, **keep only the row with the highest temperature** (policy: in a collision we trust the warmer reading, assuming a cold reading is more likely to be a sensor glitch ;  a deliberate choice, not a universal rule).

<details>
<summary>Hint</summary>

Use a window function:

```python
w = Window.partitionBy("sensor_id", "ts").orderBy(col("temperature").desc_nulls_last())
clean = clean.dropDuplicates().withColumn("rn", row_number().over(w)) \
             .filter(col("rn") == 1).drop("rn")
```
</details>

<details>
<summary>Answer</summary>

```python
w = Window.partitionBy("sensor_id", "ts").orderBy(col("temperature").desc_nulls_last())

clean = (clean
    .dropDuplicates()
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn"))

print(f"Rows after dedup: {clean.count():,}")
```
</details>

---

**Q7.** Some `humidity` values are null. Fill them with the **mean humidity for that sensor's location**. Sensors with no humidity readings at all stay null.

<details>
<summary>Hint</summary>

Compute per-location means, join back.

```python
loc_mean = clean.groupBy("location").agg(F.avg("humidity").alias("loc_mean_hum"))
```
</details>

<details>
<summary>Answer</summary>

```python
loc_mean = clean.groupBy("location").agg(F.avg("humidity").alias("loc_mean_hum"))

clean = (clean
    .join(loc_mean, on="location", how="left")
    .withColumn("humidity", coalesce(col("humidity"), col("loc_mean_hum")))
    .drop("loc_mean_hum"))

clean.filter(col("humidity").isNull()).count()  # should be small / zero
```

This is a deliberate design choice: filling nulls with a location-level mean assumes the sensor agrees with its neighbours. Flag this in your commit message ;  it's a transformation that changes meaning.
</details>

---

**Q8.** Map every variant of `status` to one of `ok`, `warning`, `error`. Anything else (`unknown`, `""`, typos) becomes `unknown`.

<details>
<summary>Answer</summary>

```python
valid_statuses = {"ok", "warning", "error"}

clean = clean.withColumn("status",
    when(col("status").isin(list(valid_statuses)), col("status"))
    .otherwise(lit("unknown")))

clean.groupBy("status").count().show()
```
</details>

---

### Exercise 3: Quality Checks & Quarantine (15 min)

**Q9.** Define "valid" for this dataset:

- `sensor_id` is not null
- `ts` is not null
- `temperature` is between -50 and 60 (or null ;  we allow missing, not absurd)
- `humidity` is between 0 and 100 (or null)
- `location` is in the canonical set `{warehouse-a, warehouse-b, office-1, office-2, coldroom}`

Write a boolean column `is_valid` that captures all of this.

<details>
<summary>Answer</summary>

```python
CANONICAL_LOCATIONS = ["warehouse-a", "warehouse-b", "office-1", "office-2", "coldroom"]

clean = clean.withColumn("is_valid",
    col("sensor_id").isNotNull()
    & col("ts").isNotNull()
    & ((col("temperature").isNull()) | col("temperature").between(-50, 60))
    & ((col("humidity").isNull())    | col("humidity").between(0, 100))
    & col("location").isin(CANONICAL_LOCATIONS))

clean.groupBy("is_valid").count().show()
```
</details>

---

**Q10.** Split `clean` into two DataFrames: `valid` (passing all checks) and `quarantine` (failing at least one). Report the quarantine rate as a percentage.

<details>
<summary>Answer</summary>

```python
valid      = clean.filter(col("is_valid")).drop("is_valid")
quarantine = clean.filter(~col("is_valid")).drop("is_valid")

total_q = quarantine.count()
total   = clean.count()
print(f"Quarantine rate: {total_q / total * 100:.2f}%  ({total_q:,} / {total:,} rows)")
```

In production, you'd alert if this rate crosses a threshold (e.g. >5%).
</details>

---

**Q11.** Write both datasets to Parquet under `/app/data/output/`:

- `valid/` ;  partitioned by `date=YYYY-MM-DD` (extracted from `ts`)
- `quarantine/` ;  single directory, not partitioned (low volume, human review)

<details>
<summary>Answer</summary>

```python
output_root = "/app/data/output"
shutil.rmtree(output_root, ignore_errors=True)

valid_with_date = valid.withColumn("date", to_date(col("ts")))

(valid_with_date.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(f"{output_root}/valid"))

(quarantine.write
    .mode("overwrite")
    .parquet(f"{output_root}/quarantine"))

# Verify
import subprocess
subprocess.run(["ls", f"{output_root}/valid"])
```
</details>

---

### Exercise 4: Make It Idempotent ;  The Static-vs-Dynamic Trap (15 min)

This is the most important exercise in the session. You're going to **lose data**, then fix it.

**Q12.** Simulate a partial rerun. Take only the data for `2026-04-14` from `valid`, and write it to `/app/data/output/valid/` with the same `mode("overwrite").partitionBy("date")` as Q11. Then `ls` the output directory. What happened?

<details>
<summary>Hint</summary>

```python
today_only = valid_with_date.filter(col("date") == "2026-04-14")

(today_only.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(f"{output_root}/valid"))

subprocess.run(["ls", f"{output_root}/valid"])
```
</details>

<details>
<summary>Answer</summary>

You should see that **only `date=2026-04-14` exists** in the output directory. The other four days of data are **gone**, even though you "only wrote today's partition."

This is the static overwrite mode ;  Spark deletes the whole target directory before writing. This is the single most common way production pipelines silently destroy data.
</details>

---

**Q13.** Regenerate the full `valid/` output (rewrite everything from Q11). Then enable **dynamic partition overwrite mode** and repeat Q12. Confirm all five days are intact after the partial rewrite.

<details>
<summary>Answer</summary>

```python
# First, regenerate everything
(valid_with_date.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(f"{output_root}/valid"))
subprocess.run(["ls", f"{output_root}/valid"])  # 5 date= directories

# Flip the mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Now rerun today's partition only
today_only = valid_with_date.filter(col("date") == "2026-04-14")
(today_only.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(f"{output_root}/valid"))

subprocess.run(["ls", f"{output_root}/valid"])  # all 5 date= directories still there
```

With `dynamic` mode, Spark only overwrites the partitions present in the DataFrame ;  which is what "idempotent rerun of today" actually means.
</details>

---

**Q14.** Refactor the entire ETL into a single function `run_etl(input_path, output_root)` that:

1. Reads the raw CSV
2. Runs all the cleaning + quality splits from Exercises 2 and 3
3. Writes `valid/` (partitioned, dynamic mode) and `quarantine/`
4. Returns a dict with counts (`rows_in`, `rows_valid`, `rows_quarantine`, `quarantine_rate`)

We'll call this from Airflow in Part 2, so create a file called `etl.py` as an importable module on your notebooks folder.

<details>
<summary>Answer</summary>

```python
# %%writefile /app/notebooks/etl.py
"""IoT sensor ETL ;  importable from Airflow."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, coalesce, lower, trim,
    to_timestamp, to_date, from_unixtime, regexp_replace, row_number,
)
from pyspark.sql.window import Window

CANONICAL_LOCATIONS = ["warehouse-a", "warehouse-b", "office-1", "office-2", "coldroom"]


def run_etl(input_path: str, output_root: str) -> dict:
    spark = (SparkSession.builder
        .appName("IoT_ETL")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate())

    raw = spark.read.csv(input_path, header=True, inferSchema=False)
    rows_in = raw.count()

    clean = (raw
        .filter(col("sensor_id").isNotNull() & (col("sensor_id") != ""))
        .withColumn("ts",
            when(col("timestamp").contains("-"), to_timestamp(col("timestamp")))
            .otherwise(from_unixtime(col("timestamp").cast("long")).cast("timestamp")))
        .withColumn("temperature", col("temperature").cast("double"))
        .withColumn("humidity",    col("humidity").cast("double"))
        .withColumn("location",
            regexp_replace(lower(trim(col("location"))), "[_ ]", "-"))
        .withColumn("status", lower(trim(col("status"))))
        .drop("timestamp")
        .dropDuplicates())

    w = Window.partitionBy("sensor_id", "ts").orderBy(col("temperature").desc_nulls_last())
    clean = (clean.withColumn("rn", row_number().over(w))
                  .filter(col("rn") == 1).drop("rn"))

    loc_mean = clean.groupBy("location").agg(F.avg("humidity").alias("m"))
    clean = (clean.join(loc_mean, on="location", how="left")
                  .withColumn("humidity", coalesce(col("humidity"), col("m")))
                  .drop("m"))

    clean = clean.withColumn("status",
        when(col("status").isin(["ok", "warning", "error"]), col("status"))
        .otherwise(lit("unknown")))

    clean = clean.withColumn("is_valid",
        col("sensor_id").isNotNull()
        & col("ts").isNotNull()
        & ((col("temperature").isNull()) | col("temperature").between(-50, 60))
        & ((col("humidity").isNull())    | col("humidity").between(0, 100))
        & col("location").isin(CANONICAL_LOCATIONS))

    valid      = clean.filter(col("is_valid")).drop("is_valid")
    quarantine = clean.filter(~col("is_valid")).drop("is_valid")

    valid_with_date = valid.withColumn("date", to_date(col("ts")))

    (valid_with_date.write
        .mode("overwrite").partitionBy("date")
        .parquet(f"{output_root}/valid"))

    (quarantine.write
        .mode("overwrite")
        .parquet(f"{output_root}/quarantine"))

    rows_valid = valid_with_date.count()
    rows_quar  = quarantine.count()
    return {
        "rows_in": rows_in,
        "rows_valid": rows_valid,
        "rows_quarantine": rows_quar,
        "quarantine_rate": rows_quar / rows_in if rows_in else 0.0,
    }
```

Test it from the notebook:

```python
from etl import run_etl
stats = run_etl("/app/data/sensors_raw.csv", "/app/data/output")
print(stats)
```
</details>

---

## Part 2: Orchestrating with Airflow (30 min)

### Exercise 5: Start Airflow and Write a Minimal DAG (10 min)

**Q15.** The Airflow container is already running. Find the auto-generated admin password.

<details>
<summary>Hint</summary>

```bash
docker compose logs airflow | grep "Password for user"
```

Or read `standalone_admin_password.txt` inside the container:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```
</details>

Open **http://localhost:8080** and log in as `admin` with the printed password.

---

**Q16.** Create `./dags/hello_world.py` with the simplest possible two-task DAG that runs on-demand (no schedule). Trigger it from the UI.

<details>
<summary>Answer</summary>

```python
# dags/hello_world.py
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="hello_world",
    start_date=datetime(2026, 4, 1),
    schedule=None,          # manual trigger only
    catchup=False,
    tags=["practice"],
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello, Airflow!'",
    )

    say_goodbye = BashOperator(
        task_id="say_goodbye",
        bash_command="echo 'Goodbye, world!'",
    )

    say_hello >> say_goodbye
```

Save the file. Within ~10 seconds, Airflow's DAG processor picks it up and you'll see `hello_world` in the UI. Click it → toggle the switch to un-pause → trigger a run via the ▶ button → click the green squares to read the logs.
</details>

---

### Exercise 6: The Real ETL DAG (15 min)

**Q17.** Create `./dags/iot_etl.py` with three tasks using classic operators:

1. `validate_input` (PythonOperator) ;  checks that `/app/data/sensors_raw.csv` exists and has rows; raises if not.
2. `run_etl` (PythonOperator) ;  calls `run_etl()` from the module you wrote in Q14.
3. `quality_check` (PythonOperator) ;  verifies that `/app/data/output/valid/` has at least one Parquet file and that the quarantine rate is below 30%.

Run `validate_input >> run_etl >> quality_check`. Schedule daily with `schedule="@daily"`. Include `retries=2` in `default_args`.

<details>
<summary>Hint</summary>

The ETL module lives in `/app/notebooks/etl.py` inside the jupyter container but you need it importable from the airflow container. Two options:

- Copy `etl.py` into `./dags/` so it's inside the mounted volume
- Or mount `./notebooks/` into the airflow container too

Simpler: put `etl.py` in `./dags/` (shared volume, already mounted at `/opt/airflow/dags`).
</details>

<details>
<summary>Answer</summary>

First, copy the ETL module next to the DAG:

```bash
cp ./notebooks/etl.py ./dags/etl.py
```

Then create `./dags/iot_etl.py`:

```python
# dags/iot_etl.py
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

INPUT_PATH  = "/app/data/sensors_raw.csv"
OUTPUT_ROOT = "/app/data/output"


def validate_input(**context):
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file missing: {INPUT_PATH}")
    size = os.path.getsize(INPUT_PATH)
    if size == 0:
        raise ValueError(f"Input file is empty: {INPUT_PATH}")
    print(f"Input OK ;  {size/1e6:.1f} MB at {INPUT_PATH}")


def run_etl_task(**context):
    from etl import run_etl
    stats = run_etl(INPUT_PATH, OUTPUT_ROOT)
    print(f"ETL stats: {stats}")
    # Push stats to XCom so quality_check can use them
    return stats


def quality_check(**context):
    stats = context["ti"].xcom_pull(task_ids="run_etl")
    partitions = list(Path(f"{OUTPUT_ROOT}/valid").glob("date=*"))
    if not partitions:
        raise AssertionError("No date partitions written")

    rate = stats["quarantine_rate"]
    if rate > 0.30:
        raise AssertionError(f"Quarantine rate {rate:.1%} exceeds 30% threshold")
    print(f"Quality OK ;  {len(partitions)} partitions, quarantine rate {rate:.2%}")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="iot_etl",
    default_args=default_args,
    start_date=datetime(2026, 4, 14),
    schedule="@daily",
    catchup=False,
    tags=["practice", "etl"],
) as dag:

    t_validate = PythonOperator(task_id="validate_input", python_callable=validate_input)
    t_etl      = PythonOperator(task_id="run_etl",        python_callable=run_etl_task)
    t_check    = PythonOperator(task_id="quality_check",  python_callable=quality_check)

    t_validate >> t_etl >> t_check
```

Trigger from the UI. Watch the Graph view color as tasks progress. Open each task's log to see the print output.
</details>

---

**Q18.** Force a failure to watch Airflow's retry behavior. Rename `/app/data/sensors_raw.csv` to something else so `validate_input` fails. Trigger the DAG again and observe: how many attempts do you see? How is the delay between them?

<details>
<summary>Hint</summary>

```bash
docker compose exec airflow mv /app/data/sensors_raw.csv /app/data/sensors_raw.csv.bak
```

In the UI: task instance → Try Number badge goes 1, 2, 3 across the 30-second intervals. Rename back when done:

```bash
docker compose exec airflow mv /app/data/sensors_raw.csv.bak /app/data/sensors_raw.csv
```

Manually clear the failed task run to restart.
</details>

---

**Q19.** Look at the Graph view while a run is in progress. Identify:

- Which task is currently running (color?)
- Which tasks have succeeded (color?)
- What colors represent queued / up_for_retry / failed states?

*No code ;  just read the UI.*

---

### Exercise 7: TaskFlow Rewrite (bonus, 5 min)

**Q20.** Rewrite `iot_etl.py` using the TaskFlow API (`@dag`/`@task` decorators). The ETL stats should flow from `run_etl` to `quality_check` via the function return value instead of manual XCom.

<details>
<summary>Answer</summary>

```python
# dags/iot_etl_taskflow.py
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

INPUT_PATH  = "/app/data/sensors_raw.csv"
OUTPUT_ROOT = "/app/data/output"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="iot_etl_taskflow",
    default_args=default_args,
    start_date=datetime(2026, 4, 14),
    schedule="@daily",
    catchup=False,
    tags=["practice", "etl", "taskflow"],
)
def iot_etl():

    @task
    def validate() -> str:
        if not os.path.exists(INPUT_PATH):
            raise FileNotFoundError(INPUT_PATH)
        return INPUT_PATH

    @task
    def etl(input_path: str) -> dict:
        from etl import run_etl
        return run_etl(input_path, OUTPUT_ROOT)

    @task
    def check(stats: dict):
        partitions = list(Path(f"{OUTPUT_ROOT}/valid").glob("date=*"))
        if not partitions:
            raise AssertionError("No date partitions written")
        if stats["quarantine_rate"] > 0.30:
            raise AssertionError(f"Quarantine rate too high: {stats['quarantine_rate']:.1%}")
        print(f"Quality OK ;  {len(partitions)} partitions, rate {stats['quarantine_rate']:.2%}")

    check(etl(validate()))


iot_etl()
```

Notice:
- **No manual XCom.** `validate()` returns a string → Airflow pushes it → `etl(input_path)` pulls it as its first argument.
- **No explicit dependencies.** `check(etl(validate()))` encodes the DAG via function composition.
- **Fewer task_id strings.** They default to the function name.
</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| Exploring messy data | Null counts, duplicate detection, format inconsistency |
| Cleaning | Parsing mixed timestamps, casting, normalising strings |
| Deduplication | Exact dupes, key collisions, window-based policies |
| Data quality | Validity rules, quarantine vs drop |
| Idempotent writes | Partitioning, the static-vs-dynamic trap |
| ETL packaging | Refactoring a notebook into an importable module |
| Airflow basics | `airflow standalone`, DAG directory, the UI |
| Classic operators | `PythonOperator`, `BashOperator`, manual XCom |
| TaskFlow API | `@dag`/`@task`, automatic data passing |
| Retries & observation | `default_args`, graph view, task logs |

> **The core idea**: clean code in Spark, orchestrate in Airflow, and **never trust a rerun that you haven't made idempotent.**
> Next session: Streaming with Apache Kafka.

---

## Cleanup

```bash
docker compose down
```

To also remove generated data:

```bash
docker compose down -v
rm -rf data/*.csv data/output dags/etl.py dags/__pycache__
```

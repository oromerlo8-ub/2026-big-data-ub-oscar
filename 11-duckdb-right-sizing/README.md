# DuckDB & Right-Sizing Practice

The previous Spark sessions ran every analysis through a distributed engine, regardless of dataset size. Today we flip that. The whole point is to show how far you can get **without a cluster** when the data fits on one machine, and to make the cost of over-engineering concrete by running the same query through Pandas, DuckDB, and Spark side by side.

## Setup

```bash
cd 11-duckdb-right-sizing
docker compose up -d --build
```

Open your browser at **http://localhost:8888**.

The **Spark UI** (when we use it in Exercise 5) appears at **http://localhost:4040** once a SparkSession is running.

---

## Data

We're using the **NYC Yellow Taxi 2024** dataset; twelve monthly Parquet files, ~3.5 GB total. It's already cached if you ran session 07 or
if you've started the assignment 2:

```bash
cp ../07-spark-sql/data/yellow_tripdata_2024-*.parquet ./data/
```

Otherwise download from the public TLC bucket (the URLs are public, no AWS credentials needed):

```bash
mkdir -p data
for m in $(seq -w 1 12); do
    curl -L -o "data/yellow_tripdata_2024-${m}.parquet" \
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-${m}.parquet"
done
```

You should end up with twelve `yellow_tripdata_2024-MM.parquet` files. Confirm from your host:

```bash
ls -lh data/yellow_tripdata_2024-*.parquet | head -3
```

Inside the Jupyter container these files live at `/app/data/yellow_tripdata_2024-MM.parquet`.

---

## Part 1: DuckDB Fundamentals (45 min)

### Exercise 1: Hello DuckDB (3 min)

Open **http://localhost:8888** and create a notebook under `/app/notebooks/`. Use this as your first cell:

```python
import duckdb
print(duckdb.__version__)
```

**Q1.** Run a couple of trivial queries against DuckDB. There is no server to start, no port to bind, no config file. Show three things:

1. The module-level `duckdb.sql(...)` call (uses an implicit in-memory database).
2. An explicit in-memory connection via `duckdb.connect()`.
3. A persistent file-backed connection at `/app/data/scratch.duckdb`. Create a table in it, close the connection, reopen, and confirm the table is still there.

<details>
<summary>Answer</summary>

```python
duckdb.sql("SELECT 42 AS answer, current_date AS today, version() AS v").show()

con_mem = duckdb.connect()
con_mem.sql("SELECT 'in-memory' AS where_am_i").show()
con_mem.close()

con_disk = duckdb.connect('/app/data/scratch.duckdb')
con_disk.sql("CREATE OR REPLACE TABLE greetings AS SELECT 'hola' AS msg")
con_disk.close()

con_disk = duckdb.connect('/app/data/scratch.duckdb')
con_disk.sql("SELECT * FROM greetings").show()
con_disk.close()
```

The whole "database" is the single `scratch.duckdb` file on disk. No server, no daemon, no `pg_hba.conf`. That's the embedded story; it's why people call DuckDB "SQLite for analytics".

</details>

---

### Exercise 2: Query a Parquet File Directly (7 min)

**Q2.** Without using `pandas.read_parquet` or any DataFrame loader, query one month of taxi data:

1. Show the first 5 rows of `yellow_tripdata_2024-01.parquet`.
2. Print the schema with `DESCRIBE`.
3. Count the rows.
4. Run `SUMMARIZE` on the file.

<details>
<summary>Hint</summary>

DuckDB lets you put a Parquet path directly in the `FROM` clause; the string is interpreted as a table source. No `read_parquet()` step needed.

</details>

<details>
<summary>Answer</summary>

```python
duckdb.sql("SELECT * FROM '/app/data/yellow_tripdata_2024-01.parquet' LIMIT 5").show()

duckdb.sql("DESCRIBE SELECT * FROM '/app/data/yellow_tripdata_2024-01.parquet'").show()

duckdb.sql("SELECT COUNT(*) AS rows FROM '/app/data/yellow_tripdata_2024-01.parquet'").show()

duckdb.sql("SUMMARIZE SELECT * FROM '/app/data/yellow_tripdata_2024-01.parquet'").show()
```

Note what just happened: there is **no DataFrame in memory**. DuckDB read the Parquet metadata (column names, types, row counts, per-column min/max stats) and then only the row groups it actually needs to answer the query. `COUNT(*)` reads zero data rows; the count comes from Parquet's footer. `SUMMARIZE` walks the data once and gives you per-column stats; useful for a first look at any new dataset.

</details>

---

### Exercise 3: Glob Across All 12 Months (8 min)

> **What's new vs Session 03 / Assignment 1**: you've already measured **column pruning** with `pq.read_table(columns=[...])`. Here we're testing the other half of the story; **row-group skipping via predicate pushdown**, where a `WHERE` clause gets pushed into the Parquet reader so entire row groups are skipped without ever being decoded. Same dataset, different mechanism.

**Q3a.** Count rows across the entire 2024 dataset using a glob pattern. How many trips happened in 2024?

**Q3b.** Now count only the trips with `fare_amount > 200`. Time both queries. Why is the second one not 200× slower than the first, given that you're filtering down to a tiny fraction of rows?

<details>
<summary>Hint</summary>

`'/app/data/yellow_tripdata_2024-*.parquet'` resolves to all twelve files. Use `import time; t0 = time.perf_counter(); ...; print(time.perf_counter() - t0)` to time things.

</details>

<details>
<summary>Answer</summary>

```python
import time

t0 = time.perf_counter()
duckdb.sql("""
    SELECT COUNT(*) AS total_trips
    FROM '/app/data/yellow_tripdata_2024-*.parquet'
""").show()
print(f"Total count: {time.perf_counter() - t0:.2f}s")

t0 = time.perf_counter()
duckdb.sql("""
    SELECT COUNT(*) AS expensive_trips
    FROM '/app/data/yellow_tripdata_2024-*.parquet'
    WHERE fare_amount > 200
""").show()
print(f"Filtered count: {time.perf_counter() - t0:.2f}s")
```

The second query is roughly the same speed as the first (or faster, depending on caching). It's not "200× slower" because:

1. **Column pruning**: only `fare_amount` is read from disk. The other ~17 columns are skipped entirely; Parquet is columnar, so this is just "don't open those byte ranges".
2. **Row-group skipping (predicate pushdown)**: each Parquet row group has a min/max statistic for `fare_amount` in its footer. DuckDB checks those first. If the row group's max is ≤ 200, the whole group is skipped without ever decoding it.

This is the magic that makes DuckDB feel fast on Parquet. **The query plan reads only the bytes it needs**; everything else stays on disk.

</details>

---

### Exercise 4: CTE + Window + QUALIFY (12 min)

**Q4.** For each month of 2024, find the **top 3 pickup zones** (`PULocationID`) ranked by total fare revenue. Use:

- a CTE to compute monthly revenue per zone,
- a window function for the per-month ranking,
- `QUALIFY` to filter on the rank without wrapping in another subquery.

Show one row per (month, top-3-zone), 36 rows total, ordered by month and rank.

<details>
<summary>Hint</summary>

`QUALIFY` is to window functions what `HAVING` is to aggregates: it lets you filter on the result of the window expression without a wrapping subquery. Standard SQL would need:

```sql
SELECT * FROM (... RANK() OVER (...) AS rnk ...) WHERE rnk <= 3
```

DuckDB lets you write:

```sql
SELECT ... RANK() OVER (...) AS rnk ... QUALIFY rnk <= 3
```

</details>

<details>
<summary>Answer</summary>

```python
duckdb.sql("""
    WITH monthly_zone_revenue AS (
        SELECT
            DATE_TRUNC('month', tpep_pickup_datetime) AS month,
            PULocationID,
            SUM(total_amount) AS revenue
        FROM '/app/data/yellow_tripdata_2024-*.parquet'
        WHERE total_amount > 0
          AND tpep_pickup_datetime >= '2024-01-01'
          AND tpep_pickup_datetime <  '2025-01-01'
        GROUP BY month, PULocationID
    )
    SELECT
        month,
        PULocationID,
        revenue,
        RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk
    FROM monthly_zone_revenue
    QUALIFY rnk <= 3
    ORDER BY month, rnk
""").show(max_rows=36)
```

A few things to notice:

- The CTE produces ~3000 rows (~250 zones × 12 months); the window then ranks within each month; `QUALIFY` keeps only the top 3 per partition.
- `DATE_TRUNC('month', ts)` truncates a timestamp to the start of its month; convenient for grouping.
- The same query in PostgreSQL would need a wrapping subquery because PostgreSQL doesn't have `QUALIFY`. DuckDB borrows it from Snowflake / Teradata; it's strictly nicer.
- We need that `tpep_pickup_datetime` filter because the raw TLC data has a handful of rows with timestamps from 2002, 2008, 2025, even 2026; data-entry errors and bad GPS clocks. Without the filter, your "12 months of 2024" output picks up phantom months. **Always check, always filter** when you cross a real-world dataset.
- `show()` is keyword-only in the DuckDB Python API; use `show(max_rows=N)`, not `show(N)`. Passing a positional integer raises `TypeError`.

Most months show the same one or two zones at the top; that's JFK and LaGuardia airports (locations 132 and 138 in the TLC zone lookup).

</details>

---

### Exercise 5: Same Query, Three Engines (15 min)

This is the right-sizing argument made concrete. We'll compute average fare and trip count per pickup zone, on the same data, three different ways.

**Q5a.** Run the aggregation in **Pandas** on a subset of the data (we'll use the first two months; the full year does not fit comfortably in memory after `pd.concat`). Time it.

<details>
<summary>Answer</summary>

```python
import pandas as pd
import glob, time

t0 = time.perf_counter()
files = sorted(glob.glob('/app/data/yellow_tripdata_2024-0[12].parquet'))
df = pd.concat(
    [pd.read_parquet(f, columns=['PULocationID', 'fare_amount']) for f in files],
    ignore_index=True,
)
result_pd = (df.groupby('PULocationID')
             .agg(trips=('fare_amount', 'count'), avg_fare=('fare_amount', 'mean'))
             .sort_values('trips', ascending=False))
elapsed_pd = time.perf_counter() - t0
print(f"Pandas (2 months): {elapsed_pd:.2f}s")
print(result_pd.head())
```

Notice we had to:

- Glob the files manually.
- Read with explicit `columns=` to keep memory under control.
- Concat into a single DataFrame in memory before grouping.

Trying this on all 12 months would either be very slow or OOM in our 6 GB container. That's the wall: Pandas wants the whole thing in RAM.

</details>

**Q5b.** Run the same aggregation in **DuckDB** on the **full 12 months**. Time it.

<details>
<summary>Answer</summary>

```python
import duckdb, time

t0 = time.perf_counter()
result_duck = duckdb.sql("""
    SELECT PULocationID,
           COUNT(*) AS trips,
           AVG(fare_amount) AS avg_fare
    FROM '/app/data/yellow_tripdata_2024-*.parquet'
    GROUP BY PULocationID
    ORDER BY trips DESC
""").df()
elapsed_duck = time.perf_counter() - t0
print(f"DuckDB (12 months): {elapsed_duck:.2f}s")
print(result_duck.head())
```

DuckDB chews through six times more data and is typically faster than Pandas on two months. That's the columnar engine, the parallel scan across files, and column pruning all paying off at once.

</details>

**Q5c.** Run the same aggregation in **Spark** on the full 12 months. Time the whole thing including the SparkSession startup; that's the cost you actually pay.

<details>
<summary>Hint</summary>

`spark.read.parquet('/app/data/yellow_tripdata_2024-*.parquet')` accepts the same glob. Action methods (`.show()`, `.count()`) trigger evaluation; transformations are lazy.

</details>

<details>
<summary>Answer</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col
import time

t0 = time.perf_counter()
spark = (SparkSession.builder
         .appName("RightSizing")
         .master("local[*]")
         .config("spark.driver.memory", "4g")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

result_spark = (spark.read.parquet('/app/data/yellow_tripdata_2024-*.parquet')
                .groupBy("PULocationID")
                .agg(count("*").alias("trips"), avg("fare_amount").alias("avg_fare"))
                .orderBy(col("trips").desc()))

result_spark.show(5)
elapsed_spark = time.perf_counter() - t0
print(f"Spark (12 months, including session startup): {elapsed_spark:.2f}s")
```

A typical run on a laptop:

| Engine | Data | Wall time |
|--------|------|-----------|
| Pandas | 2 months | a few seconds (mostly I/O + concat) |
| DuckDB | 12 months | a few seconds |
| Spark  | 12 months | ~30 seconds, almost all of it SparkSession startup |

The point is not "Spark is bad". Spark is excellent at what it's designed for; horizontally scaled data that no single machine can hold. The point is that for a 3.5 GB Parquet aggregation, you spent more time waiting for the JVM than answering the question.

Run `spark.stop()` before moving on so the next exercises don't compete for memory:

```python
spark.stop()
```

</details>

---

## Part 2: Interop, Arrow, and ETL (45 min)

### Exercise 6: Persistent Database (5 min)

So far every query has gone against the raw Parquet files; the result evaporates when the connection closes. Sometimes you want to **materialize** an aggregate so subsequent queries don't recompute it.

**Q6.** Open a persistent DuckDB file at `/app/data/taxi_analytics.duckdb`. Create a table `pickup_summary` containing trip counts and average fare per pickup zone for the full year. Close the connection. Reopen it and verify the table is still there.

<details>
<summary>Answer</summary>

```python
con = duckdb.connect('/app/data/taxi_analytics.duckdb')
con.sql("""
    CREATE OR REPLACE TABLE pickup_summary AS
    SELECT PULocationID,
           COUNT(*) AS trips,
           AVG(fare_amount) AS avg_fare,
           SUM(total_amount) AS revenue
    FROM '/app/data/yellow_tripdata_2024-*.parquet'
    WHERE total_amount > 0
      AND tpep_pickup_datetime >= '2024-01-01'
      AND tpep_pickup_datetime <  '2025-01-01'
    GROUP BY PULocationID
""")
con.close()

con = duckdb.connect('/app/data/taxi_analytics.duckdb')
con.sql("SHOW TABLES").show()
con.sql("SELECT * FROM pickup_summary ORDER BY revenue DESC LIMIT 10").show()
con.close()
```

The raw Parquet files stay where they were; the `.duckdb` file holds only the aggregate. This is a good pattern when you have an expensive aggregation that downstream notebooks will hit repeatedly; build it once, query it many times. From the host you can see the file on disk:

```bash
ls -lh data/taxi_analytics.duckdb
```

</details>

---

### Exercise 7: Pandas / Polars Interop (10 min)

DuckDB can query a DataFrame **that already lives in your notebook** without an explicit `register()` call. It looks up the local namespace.

**Q7.** Read the first 100k rows of January with Pandas. Then run a SQL query directly against that variable; group by `payment_type` and compute average fare and trip count. Return the result as Pandas, then as Polars, then as a PyArrow Table.

<details>
<summary>Hint</summary>

The result of `duckdb.sql(...)` is a `DuckDBPyRelation`. Call `.df()` for Pandas, `.pl()` for Polars, `.arrow()` for a PyArrow Table, `.fetchall()` for a list of tuples.

</details>

<details>
<summary>Answer</summary>

```python
import pandas as pd

sample = pd.read_parquet('/app/data/yellow_tripdata_2024-01.parquet').head(100_000)
print(type(sample), len(sample))

rel = duckdb.sql("""
    SELECT payment_type,
           COUNT(*) AS trips,
           AVG(fare_amount) AS avg_fare
    FROM sample
    GROUP BY payment_type
    ORDER BY trips DESC
""")

print("As Pandas:");  print(rel.df())
print("As Polars:");  print(rel.pl())
print("As Arrow:");   print(rel.to_arrow_table())
```

DuckDB found `sample` in the local namespace; no `register()` call needed. Same trick works for Polars DataFrames and PyArrow Tables. This is what people mean when they say "DuckDB is the SQL engine your notebook is missing"; you don't have to leave Python to run SQL on the data you already have.

A quick note on Arrow: in current DuckDB versions `.arrow()` returns a streaming `pyarrow.RecordBatchReader` (consume-once). Use **`.to_arrow_table()`** when you want a materialized `pyarrow.Table` you can re-use; or call `.read_all()` on the reader. `.fetchall()` is also there if you want plain Python rows; you almost never want that for analytics, but it's the right shape for a single value or a small lookup.

</details>

---

### Exercise 8: Arrow Zero-Copy (10 min)

Arrow is the in-memory format that lets DuckDB, Polars, and PyArrow share data without copying. Each engine sees the same buffers; no serialization.

**Q8.** Pull a slice of January's data through DuckDB into a PyArrow Table. Convert that to a Polars DataFrame. Then go back the other way; query the same Arrow Table with DuckDB, having registered it as a view.

<details>
<summary>Answer</summary>

```python
import pyarrow as pa
import polars as pl

arrow_table = duckdb.sql("""
    SELECT PULocationID, fare_amount, tip_amount
    FROM '/app/data/yellow_tripdata_2024-01.parquet'
    WHERE fare_amount BETWEEN 10 AND 100
""").to_arrow_table()

print(type(arrow_table))
print(arrow_table.schema)
print(f"Rows: {arrow_table.num_rows:,}")

pl_df = pl.from_arrow(arrow_table)
print(pl_df.head())

duckdb.register('my_arrow_view', arrow_table)
duckdb.sql("""
    SELECT PULocationID,
           COUNT(*)              AS n,
           AVG(tip_amount)       AS avg_tip,
           AVG(tip_amount / fare_amount) AS avg_tip_pct
    FROM my_arrow_view
    GROUP BY PULocationID
    ORDER BY n DESC
    LIMIT 5
""").show()
```

Notice we never wrote that Arrow Table to disk; never serialized it across a wire; and never made a deep copy. It's literally the same memory buffers being read by Polars and re-queried by DuckDB. That's why the Arrow story matters for medium data: **the cost of moving data between tools collapses to zero**, so you can mix the right engine for each step of a pipeline without paying conversion overhead.

</details>

---

### Exercise 9: Mini ETL;  Write Partitioned Parquet (10 min)

> **What's new vs Session 03 / Assignment 1**: you've already written partitioned Parquet on this same dataset using `pq.write_to_dataset(partition_cols=[...])`. The outcome is the same here; the **how** is different. PyArrow's writer needs the full Arrow Table loaded in memory first, so it's bounded by your RAM. DuckDB's `COPY ... TO ... (PARTITION_BY (...))` runs as a **streaming pipeline** in an embedded engine: scan the input Parquet files, aggregate, and write the partitioned output without ever materializing the full dataset. It scales past RAM, runs in parallel, and is one SQL statement instead of three Python steps. That's the embedded-database angle.

A common pattern: read raw data, aggregate it, write the aggregate as partitioned Parquet for downstream consumers (BI tools, the next pipeline stage, an Iceberg table). DuckDB does this in one statement.

**Q9.** Build a daily-per-zone summary for the full year, written to `/app/data/output/daily_zone_metrics/`, partitioned by `year` and `month`. Then verify by reading the output back.

<details>
<summary>Hint</summary>

`COPY (<query>) TO '<path>' (FORMAT PARQUET, PARTITION_BY (col1, col2), OVERWRITE_OR_IGNORE true)` writes the result of the query to the target directory, with one Parquet file per partition combination.

</details>

<details>
<summary>Answer</summary>

```python
import os
os.makedirs('/app/data/output', exist_ok=True)

duckdb.sql("""
    COPY (
        SELECT
            YEAR(tpep_pickup_datetime)                  AS year,
            MONTH(tpep_pickup_datetime)                 AS month,
            DATE_TRUNC('day', tpep_pickup_datetime)::DATE AS trip_date,
            PULocationID,
            COUNT(*)                                    AS trips,
            SUM(total_amount)                           AS revenue,
            AVG(trip_distance)                          AS avg_distance
        FROM '/app/data/yellow_tripdata_2024-*.parquet'
        WHERE tpep_pickup_datetime >= '2024-01-01'
          AND tpep_pickup_datetime <  '2025-01-01'
          AND total_amount > 0
        GROUP BY year, month, trip_date, PULocationID
    ) TO '/app/data/output/daily_zone_metrics' (
        FORMAT PARQUET,
        PARTITION_BY (year, month),
        OVERWRITE_OR_IGNORE true
    )
""")
```

`COPY ... TO` will create the **leaf** directory (`daily_zone_metrics`) and the partition subdirs (`year=2024/month=1/...`), but it won't recursively create missing parents. The `os.makedirs(..., exist_ok=True)` call ensures `/app/data/output` exists first. Same gotcha as `pathlib.Path.mkdir(parents=True)` vs plain `mkdir`.

Inspect the layout from the host:

```bash
find data/output/daily_zone_metrics -type f | head
```

You should see a Hive-style directory tree (`year=2024/month=1/...`, `year=2024/month=2/...`). Now read it back:

```python
duckdb.sql("""
    SELECT year, month, COUNT(*) AS rows, SUM(revenue) AS total_revenue
    FROM '/app/data/output/daily_zone_metrics/**/*.parquet'
    GROUP BY year, month
    ORDER BY year, month
""").show()
```

That's an end-to-end ETL: raw monthly Parquet files in, partitioned aggregate out, all in DuckDB, all in seconds. Compare the lines of code to the equivalent Spark job; this is the right-sizing payoff in practice.

The `WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2025-01-01'` filter is defensive: real taxi data has occasional rows with timestamps from previous or future years (data entry errors, GPS clock issues). Always check, always filter.

</details>

---

### Exercise 10 (bonus): httpfs;  Query a Public URL Directly (10 min)

If we have time at the end. The point: DuckDB can read Parquet over HTTPS the same way it reads it from disk; predicate pushdown and column pruning work over the network too.

**Q10.** Install and load the `httpfs` extension, then run a query directly against a Parquet file at a public URL (no download step). Inspect how many bytes had to come over the network.

<details>
<summary>Hint</summary>

The TLC publishes the same files we have locally at `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-MM.parquet`. Install httpfs once, then put the URL in the `FROM` clause.

</details>

<details>
<summary>Answer</summary>

```python
duckdb.sql("INSTALL httpfs; LOAD httpfs;")

duckdb.sql("""
    SELECT COUNT(*) AS rows,
           AVG(fare_amount) AS avg_fare
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
""").show()
```

This is the same data as `/app/data/yellow_tripdata_2024-01.parquet` but pulled over the network. DuckDB issued **HTTP range requests** for just the Parquet footer first (to read the schema and stats), then range requests for only the column chunks it needed to answer the query. That's a tiny fraction of the file's 50 MB.

The same pattern works for `s3://...` URLs once you've configured credentials; `INSTALL httpfs` covers both. We're avoiding the AWS credential setup in class today, but the mechanics are identical.

This is what people mean by "the data lake is the database": you don't move the data into a warehouse, you query it where it sits.

</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| Embedded engine | `import duckdb`, in-memory vs file-backed connections |
| Direct Parquet querying | Path in `FROM` clause, no DataFrame loader |
| Glob patterns | `*` and `**` over many files |
| Predicate / column pushdown | Why filtered scans aren't proportionally slower |
| Modern SQL | CTEs, window functions, `QUALIFY` |
| Right-sizing | Same query in Pandas vs DuckDB vs Spark |
| Persistent storage | `connect('file.duckdb')`, `CREATE TABLE` |
| DataFrame interop | Querying a Pandas / Polars / Arrow object directly with SQL |
| Arrow zero-copy | DuckDB ↔ PyArrow ↔ Polars without conversion |
| Partitioned writes | `COPY ... TO ... (PARTITION_BY (...))` |
| Network reads | `httpfs` over public HTTPS (and the `s3://` analogue) |

> **The core idea**: most analytical workloads at most companies fit on one machine. When they do, an embedded columnar engine like DuckDB will outrun a distributed cluster, with a fraction of the operational complexity. Use Spark when you actually need it; reach for DuckDB first and let the data tell you when you've outgrown it.

Next session: **Apache Iceberg**;  bringing ACID, time travel, and schema evolution to data lakes.

---

## Cleanup

```bash
docker compose down
```

To also wipe the persistent DuckDB file and the partitioned output:

```bash
rm -rf data/scratch.duckdb data/taxi_analytics.duckdb data/output
```

The raw Parquet files in `data/` stay; you'll want them again for next session if it pulls from the same source.

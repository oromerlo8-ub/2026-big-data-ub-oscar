# Spark Performance Optimization Practice

## Setup

Reuses the same Docker environment as Sessions 05-07. From your local git repository:

```bash
cd 08-spark-performance
docker compose up -d --build
```

Open your browser at **http://localhost:8888**.

The **Spark UI** will be available at **http://localhost:4040** once you start a SparkSession.

---

## Data

We use the same e-commerce orders dataset (`orders.csv`) from previous sessions.
If you do not have it cached locally, download it inside the notebook:

```python
import urllib.request

url = "https://huggingface.co/datasets/BigDataUB/synthetic-columnar/resolve/main/orders.csv"
print("Downloading dataset (~830 MB, this will take a few minutes)...")
urllib.request.urlretrieve(url, "/app/data/orders.csv")
print("Done!")
```

If you have it, copy it from a previous session to avoid downloading again:

`cp ../07-spark-sql/data/orders.csv ./data/orders.csv`

---

## Notebook Setup

Use the following cell as the first cell in your notebook:

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, avg, count, desc,
    year, month, to_date, date_format,
    when, row_number, rank, lag,
    broadcast, rand, concat, floor
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SparkPerformance") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", True) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
```

Load the dataset:

```python
df = spark.read.csv("/app/data/orders.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("orders")
df.printSchema()
print(f"Rows: {df.count()}")
```

---

## Part 1: Reading Execution Plans (25 min)

### Exercise 1: Your First explain()

**Q1.** Run `explain()` on a simple filter query. How many stages does it have? Is there a shuffle?

```python
simple = df.filter(col("status") == "completed")
simple.explain()
```

<details>
<summary>Answer</summary>

```
== Physical Plan ==
*(1) Filter (isnotnull(status) AND (status = completed))
+- FileScan csv [...]
```

One stage, no shuffle. `filter` is a **narrow transformation**, each partition
is processed independently. The `*(1)` prefix means everything runs in Stage 1
with whole-stage code generation.

</details>

---

**Q2.** Now run `explain()` on a `groupBy` aggregation. Compare with Q1.

```python
revenue_by_category = df.groupBy("category").agg(F.sum("total_amount").alias("revenue"))
revenue_by_category.explain()
```

<details>
<summary>Answer</summary>

```
== Physical Plan ==
*(2) HashAggregate(keys=[category], functions=[sum(total_amount)])
+- Exchange hashpartitioning(category, 200)          ← SHUFFLE
   +- *(1) HashAggregate(keys=[category], functions=[partial_sum(total_amount)])
      +- FileScan csv [...]
```

Two stages separated by an `Exchange` (shuffle):
- **Stage 1**: partial aggregation per partition (local combine)
- **Exchange**: shuffle by `category` key
- **Stage 2**: final aggregation

The `partial_sum` in Stage 1 means Spark pre-aggregates locally before
shuffling, it sends category totals per partition, not every row.

</details>

---

**Q3.** Run `explain(True)` on the same query from Q2 to see all plan levels.
What optimizations does Catalyst apply between the parsed and physical plans?

<details>
<summary>Hint</summary>

`explain(True)` shows: Parsed → Analyzed → Optimized → Physical.
Look for differences between the Optimized and Physical plans.

</details>

<details>
<summary>Answer</summary>

```python
revenue_by_category.explain(True)
```

Key observations:
- **Parsed plan**: raw representation of your code
- **Analyzed plan**: types resolved, column references validated
- **Optimized plan**: Catalyst pushes the projection down, only `category` and
  `total_amount` are read, not all columns
- **Physical plan**: concrete execution strategy chosen (HashAggregate, Exchange type)

The column pruning is visible in the `FileScan` line: `ReadSchema` only lists
the columns actually needed, not the full schema.

</details>

---

### Exercise 2: Identifying Shuffles and Pushdowns

**Q4.** Run `explain()` on this query and identify: (a) how many shuffles, (b)
whether the filter is pushed down, (c) which columns are read from the file.

```python
result = (df
    .filter(col("status") == "completed")
    .filter(col("total_amount") > 100)
    .groupBy("country", "category")
    .agg(
        F.sum("total_amount").alias("revenue"),
        count("*").alias("num_orders")
    )
    .orderBy(col("revenue").desc()))

result.explain()
```

<details>
<summary>Answer</summary>

```python
result.explain()
```

You should see:
- **Two shuffles** (two `Exchange` nodes):
  1. `hashpartitioning(country, category, 200)` for the `groupBy`
  2. `rangepartitioning(revenue DESC, 200)` for the `orderBy`
- **Filter pushed down**: the `FileScan` line shows `PushedFilters`
  including the `total_amount > 100` predicate (Spark pushes numeric
  comparisons). The `status = 'completed'` filter may also appear there,
  but CSV doesn't support true pushdown, it's applied at scan time.
- **Column pruning**: `ReadSchema` lists only `status`, `total_amount`,
  `country`, `category`, not all columns.

Two stages boundaries = three stages total.

</details>

---

**Q5.** Write the cleaned data as Parquet and repeat Q4 on the Parquet version.
What changes in the plan?

```python
df.write.mode("overwrite").parquet("/app/data/orders.parquet")
df_parquet = spark.read.parquet("/app/data/orders.parquet")
```

<details>
<summary>Answer</summary>

```python
result_parquet = (df_parquet
    .filter(col("status") == "completed")
    .filter(col("total_amount") > 100)
    .groupBy("country", "category")
    .agg(
        F.sum("total_amount").alias("revenue"),
        count("*").alias("num_orders")
    )
    .orderBy(col("revenue").desc()))

result_parquet.explain()
```

Key differences from CSV:
- `FileScan parquet` instead of `FileScan csv`
- **PushedFilters** now includes `GreaterThan(total_amount, 100)` and
  `IsNotNull(status)`, Parquet supports predicate pushdown at the
  row-group level, so entire chunks of data can be skipped
- **Column pruning** is more efficient: Parquet is columnar, so only the
  4 needed columns are physically read from disk (CSV must scan all columns)

The number of shuffles stays the same, that depends on the query logic,
not the file format.

</details>

---

## Part 2: Partitioning (20 min)

### Exercise 3: Partition Counts

**Q6.** Check the number of partitions for the CSV and Parquet DataFrames.
Then experiment with `repartition` and `coalesce`.

```python
print(f"CSV partitions: {df.rdd.getNumPartitions()}")
print(f"Parquet partitions: {df_parquet.rdd.getNumPartitions()}")
```

<details>
<summary>Answer</summary>

```python
print(f"CSV partitions: {df.rdd.getNumPartitions()}")
print(f"Parquet partitions: {df_parquet.rdd.getNumPartitions()}")

# Repartition to 8 partitions
df_8 = df_parquet.repartition(8)
print(f"After repartition(8): {df_8.rdd.getNumPartitions()}")

# Coalesce to 2 partitions (no shuffle)
df_2 = df_parquet.coalesce(2)
print(f"After coalesce(2): {df_2.rdd.getNumPartitions()}")
```

`repartition(8)` triggers a full shuffle to distribute data evenly across
8 partitions. `coalesce(2)` combines partitions without a shuffle (just
merges adjacent partitions).

Check with `explain()`:

```python
df_8.explain()   # Shows Exchange (shuffle)
df_2.explain()   # No Exchange
```

</details>

---

### Exercise 4: Partitioned Writes and Reads

**Q7.** Write the orders as Parquet partitioned by `country`. Then read back
only one country and check the plan for partition pruning.

```python
df_parquet.write.mode("overwrite").partitionBy("country") \
    .parquet("/app/data/orders_by_country.parquet")
```

<details>
<summary>Answer</summary>

```python
# Write partitioned
df_parquet.write.mode("overwrite").partitionBy("country") \
    .parquet("/app/data/orders_by_country.parquet")

# Read back
df_partitioned = spark.read.parquet("/app/data/orders_by_country.parquet")

# Filter on partition column
spain = df_partitioned.filter(col("country") == "ES")
spain.explain()
```

The plan should show:
```
PartitionFilters: [isnotnull(country), (country = ES)]
```

This means Spark only reads the `country=ES` directory, it never touches
data from other countries. This is **partition pruning**.

Compare the timing:

```python
# Non-partitioned: must scan everything
start = time.time()
df_parquet.filter(col("country") == "ES").count()
print(f"Non-partitioned: {time.time() - start:.2f}s")

# Partitioned: reads only one directory
start = time.time()
df_partitioned.filter(col("country") == "ES").count()
print(f"Partitioned: {time.time() - start:.2f}s")
```

</details>

---

## Part 3: Join Strategies (25 min)

For these exercises we need two DataFrames to join. We'll create a small
"countries" dimension table from the data.

```python
# Fact table: all orders (from Parquet)
orders = df_parquet

# Dimension table: one row per country with summary stats
countries = (df_parquet
    .groupBy("country")
    .agg(count("*").alias("total_orders"))
    .cache())

countries.count()  # Trigger cache
print(f"Countries rows: {countries.count()}")
countries.show()
```

---

### Exercise 5: Broadcast Join

**Q8.** Join orders with countries using a broadcast hint. Check `explain()`
to confirm Spark uses a BroadcastHashJoin.

<details>
<summary>Answer</summary>

```python
joined_broadcast = orders.join(broadcast(countries), "country")
joined_broadcast.explain()
```

You should see `BroadcastHashJoin` in the plan. The countries table is small
enough to broadcast to all executors, no shuffle of the orders table needed.

</details>

---

**Q9.** Now disable auto-broadcast and join again. What strategy does Spark
choose? Compare the plans.

<details>
<summary>Answer</summary>

```python
# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

joined_no_broadcast = orders.join(countries, "country")
joined_no_broadcast.explain()

# Re-enable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB default
```

Without broadcast, Spark falls back to **SortMergeJoin**:
- Both sides are shuffled by `country`
- Both sides are sorted
- Then merged

You'll see two `Exchange` nodes (one per table) instead of a `BroadcastExchange`.
For a small dimension table, this is wasteful. Broadcast avoids shuffling
the large fact table entirely.

</details>

---

### Exercise 6: Join Performance Comparison

**Q10.** Measure the execution time of both join strategies. Run each 3 times
and report the average.

<details>
<summary>Answer</summary>

```python
# Broadcast join
times = []
for i in range(3):
    start = time.time()
    orders.join(broadcast(countries), "country").count()
    times.append(time.time() - start)
print(f"Broadcast join avg: {sum(times)/len(times):.2f}s")

# Sort-merge join (disable broadcast)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
times = []
for i in range(3):
    start = time.time()
    orders.join(countries, "country").count()
    times.append(time.time() - start)
print(f"Sort-merge join avg: {sum(times)/len(times):.2f}s")

# Re-enable
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
```

Broadcast should be faster because it avoids shuffling the large orders table.
However, on a local machine (`local[*]`) the difference may be small (e.g.,
1.6s vs 1.75s) because shuffle "network transfer" is just local disk I/O,
there is no real network involved. On a real cluster with multiple nodes,
the gap is much larger because sort-merge shuffle moves data across the
network.

The countries table is also very small (~10 rows), so even the sort-merge
shuffle finishes quickly. The key takeaway is in the `explain()` output:
broadcast eliminates the shuffle of the large table entirely. The performance
difference becomes dramatic when the fact table is large and the cluster has
multiple nodes.

</details>

---

## Part 4: Caching (20 min)

### Exercise 7: Cache Effectiveness

**Q11.** Pick a query that you run multiple times: compute revenue per
country per category, ordered by revenue. Measure execution time with
and without caching.

<details>
<summary>Answer</summary>

```python
# Without cache
times = []
for i in range(3):
    start = time.time()
    (df_parquet
        .filter(col("status") == "completed")
        .groupBy("country", "category")
        .agg(F.sum("total_amount").alias("revenue"))
        .orderBy(col("revenue").desc())
        .count())
    times.append(time.time() - start)
print(f"No cache avg: {sum(times)/len(times):.2f}s")
```

```python
# With cache on the filtered base DataFrame
df_completed = df_parquet.filter(col("status") == "completed").cache()
df_completed.count()  # Trigger caching

times = []
for i in range(3):
    start = time.time()
    (df_completed
        .groupBy("country", "category")
        .agg(F.sum("total_amount").alias("revenue"))
        .orderBy(col("revenue").desc())
        .count())
    times.append(time.time() - start)
print(f"Cached avg: {sum(times)/len(times):.2f}s")

df_completed.unpersist()
```

You should see a modest improvement (e.g., 0.60s vs 0.49s). The gain is
real but not dramatic because Parquet is already efficient at reading
columnar data from local disk. Caching saves the re-read and re-filter
cost, but on a local SSD the difference is small.

Caching pays off more when:
- The source is expensive to read (e.g., CSV, remote storage, complex joins)
- The DataFrame is reused many times (not just 3)
- The cluster reads from network-attached storage (S3, HDFS) where I/O
  latency is higher

Check the **Storage** tab in the Spark UI to see how much memory the
cached DataFrame uses.

</details>

---

### Exercise 8: When Caching Doesn't Help

**Q12.** Cache the full DataFrame and run a single aggregation query once.
Is caching faster? Why or why not?

<details>
<summary>Answer</summary>

```python
# Cache everything
df_cached = df_parquet.cache()
df_cached.count()  # Trigger caching, this itself is slow!

# Single query
start = time.time()
df_cached.groupBy("category").agg(avg("total_amount")).show()
print(f"From cache: {time.time() - start:.2f}s")

df_cached.unpersist()

# Same query without cache
start = time.time()
df_parquet.groupBy("category").agg(avg("total_amount")).show()
print(f"No cache: {time.time() - start:.2f}s")
```

You may see the cached version is slightly faster (e.g., 0.63s vs 0.75s)
even for a single query. That's because once the data is in memory, reading
it back is fast regardless of how many columns the query needs.

But look at the **total cost**: the `count()` to trigger caching had to read
the entire dataset first. If you add that initial cost, caching is a net loss
for a single query. The point is not that the cached read is slower, but that
the upfront cost of populating the cache is wasted if you only query once.

Also note that caching stores **all columns** in memory, but this query only
needs `category` and `total_amount`. Parquet's columnar format already reads
only the needed columns from disk, caching trades that efficiency for the
benefit of faster repeated access.

**Rule**: only cache DataFrames that are (a) reused multiple times (enough
to amortize the cost of populating the cache) AND (b) expensive to recompute.

</details>

---

## Part 5: Data Skew (20 min)

### Exercise 9: Detecting Skew

**Q13.** Check the distribution of orders across countries. Is the data skewed?

<details>
<summary>Answer</summary>

```python
df_parquet.groupBy("country") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()
```

Look at the ratio between the most frequent and least frequent country.
If the top country has 10x more rows than the bottom, that's skew.

For a `groupBy("country")` aggregation, the partition handling "country X"
will process far more data than others, becoming a straggler.

</details>

---

**Q14.** Check the distribution of `category`. Is it more or less skewed
than `country`?

<details>
<summary>Answer</summary>

```python
df_parquet.groupBy("category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()
```

Categories are almost perfectly uniform (~1M rows each), so there is
essentially no skew. Compare with the country distribution where US has
3.5M rows vs IN with 200K (a 17x ratio). A `groupBy("country")` will
produce one partition 17x larger than another, a straggler risk.
A `groupBy("category")` distributes work evenly.

</details>

---

### Exercise 10: Salting a Skewed Join

**Q15.** The country distribution is skewed (US has 3.5M rows vs IN with
200K, a 17x ratio). Create a "country info" dimension table and join it
with orders on `country`. First, do a normal join. Then apply the salting
technique to spread the load.

<details>
<summary>Hint</summary>

Salting steps:
1. Add a random salt (0 to N-1) to the fact table's join key
2. Explode the dimension table to have N copies per key (one per salt)
3. Join on the salted key

</details>

<details>
<summary>Answer</summary>

```python
# Create a small dimension table
country_info = (df_parquet
    .select("country")
    .distinct()
    .withColumn("country_rank", row_number().over(
        Window.orderBy("country"))))

# Disable broadcast to force shuffle join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Normal join
start = time.time()
normal = df_parquet.join(country_info, "country")
normal.count()
print(f"Normal join: {time.time() - start:.2f}s")

# Salted join
num_salts = 10

orders_salted = df_parquet.withColumn(
    "salted_key",
    concat(col("country"), lit("_"), (rand() * num_salts).cast("int"))
)

country_info_salted = (country_info
    .crossJoin(spark.range(num_salts).withColumnRenamed("id", "salt"))
    .withColumn("salted_key",
        concat(col("country"), lit("_"), col("salt"))))

start = time.time()
salted = orders_salted.join(country_info_salted, "salted_key")
salted.count()
print(f"Salted join: {time.time() - start:.2f}s")

# Re-enable broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
```

The skew in this dataset is moderate (US is 17x larger than IN), so you
may not see a dramatic improvement from salting. In production, skew is
often much worse, e.g., one key holding 80% of all rows. That's where
salting makes a real difference: it splits one overloaded partition into
N smaller ones that run in parallel.

The tradeoff: salting increases the size of the dimension table by N×
and adds complexity. Use it only when you've confirmed skew is the
bottleneck (check the Spark UI for straggler tasks).

</details>

---

## Part 6: Putting It All Together (15 min)

### Exercise 11: Optimize a Query Step by Step

**Q16.** Start with this deliberately slow query and optimize it step by step.
After each optimization, run `explain()` and measure the time.

```python
# The "slow" baseline, uses CSV, no broadcast, no cache
df_csv = spark.read.csv("/app/data/orders.csv", header=True, inferSchema=True)

countries_dim = (df_csv
    .groupBy("country")
    .agg(count("*").alias("country_total")))

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

start = time.time()
result = (df_csv
    .filter(col("status") == "completed")
    .join(countries_dim, "country")
    .groupBy("country", "category")
    .agg(
        F.sum("total_amount").alias("revenue"),
        avg("total_amount").alias("avg_order")
    )
    .orderBy(col("revenue").desc()))
result.count()
baseline = time.time() - start
print(f"Baseline: {baseline:.2f}s")
```

Apply these optimizations one at a time, measuring after each:

1. **Switch to Parquet** instead of CSV
2. **Broadcast** the small countries dimension table
3. **Cache** the filtered DataFrame if reusing it
4. **Reduce shuffle partitions** (try `spark.sql.shuffle.partitions` = 8)

<details>
<summary>Answer</summary>

```python
# Step 1: Parquet
df_pq = spark.read.parquet("/app/data/orders.parquet")
countries_pq = df_pq.groupBy("country").agg(count("*").alias("country_total"))

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

start = time.time()
(df_pq
    .filter(col("status") == "completed")
    .join(countries_pq, "country")
    .groupBy("country", "category")
    .agg(F.sum("total_amount").alias("revenue"), avg("total_amount").alias("avg_order"))
    .orderBy(col("revenue").desc())
    .count())
print(f"Step 1 (Parquet): {time.time() - start:.2f}s")
```

```python
# Step 2: Broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

start = time.time()
(df_pq
    .filter(col("status") == "completed")
    .join(broadcast(countries_pq), "country")
    .groupBy("country", "category")
    .agg(F.sum("total_amount").alias("revenue"), avg("total_amount").alias("avg_order"))
    .orderBy(col("revenue").desc())
    .count())
print(f"Step 2 (+ Broadcast): {time.time() - start:.2f}s")
```

```python
# Step 3: Cache the filtered base
df_completed = df_pq.filter(col("status") == "completed").cache()
df_completed.count()  # Trigger cache

start = time.time()
(df_completed
    .join(broadcast(countries_pq), "country")
    .groupBy("country", "category")
    .agg(F.sum("total_amount").alias("revenue"), avg("total_amount").alias("avg_order"))
    .orderBy(col("revenue").desc())
    .count())
print(f"Step 3 (+ Cache): {time.time() - start:.2f}s")

df_completed.unpersist()
```

```python
# Step 4: Reduce shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "8")

start = time.time()
(df_pq
    .filter(col("status") == "completed")
    .join(broadcast(countries_pq), "country")
    .groupBy("country", "category")
    .agg(F.sum("total_amount").alias("revenue"), avg("total_amount").alias("avg_order"))
    .orderBy(col("revenue").desc())
    .count())
print(f"Step 4 (+ 8 shuffle partitions): {time.time() - start:.2f}s")

# Reset
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

Each step should show improvement. The biggest wins are typically:
- CSV → Parquet (columnar reads, predicate pushdown)
- Broadcast join (eliminates a shuffle of the large table)
- Reducing shuffle partitions on a local machine (200 partitions for a
  dataset that fits in memory creates scheduling overhead)

</details>

---

### Exercise 12: Query Plan Analysis (mirrors Assignment 4.1)

**Q17.** Write a query that computes daily revenue with a running total
and 7-day moving average (using window functions). Run `explain(True)` and
identify: stages, shuffles, and which columns are read.

<details>
<summary>Answer</summary>

```python
daily_revenue = spark.sql("""
    WITH daily AS (
        SELECT CAST(order_date AS DATE) as day,
               SUM(total_amount) as daily_revenue
        FROM orders
        GROUP BY day
    )
    SELECT day, daily_revenue,
        SUM(daily_revenue) OVER (ORDER BY day) as running_total,
        AVG(daily_revenue) OVER (
            ORDER BY day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d
    FROM daily
    ORDER BY day
""")

daily_revenue.explain(True)
```

You should see:
- **Stage 1**: scan + partial aggregation (groupBy day)
- **Shuffle 1**: Exchange by `day`
- **Stage 2**: final aggregation
- **Shuffle 2**: Exchange for the window function (single partition, ORDER BY day)
- **Stage 3**: window computations (running total + moving avg)
- **Shuffle 3**: Exchange for the final ORDER BY

Only `order_date` and `total_amount` are read from the file (column pruning).

</details>

---

### Exercise 13: Caching Measurement (mirrors Assignment 4.2)

**Q18.** Using the query from Q17, measure execution time with and without
caching the base DataFrame. Run 3 times each and compare averages. Explain
what you observe.

<details>
<summary>Hint</summary>

Think about what you're caching. If the base table is read from Parquet and
the aggregation reduces millions of rows to ~365 daily rows, is caching the
base table the right thing to cache? What about caching the daily aggregation
instead?

</details>

<details>
<summary>Answer</summary>

```python
# Without cache
times = []
for i in range(3):
    start = time.time()
    daily_revenue.count()
    times.append(time.time() - start)
print(f"No cache avg: {sum(times)/len(times):.2f}s")
```

```python
# Cache the base DataFrame
df.cache()
df.count()  # Trigger

times = []
for i in range(3):
    start = time.time()
    daily_revenue.count()
    times.append(time.time() - start)
print(f"Cached base avg: {sum(times)/len(times):.2f}s")
df.unpersist()
```

You may find that caching the full base DataFrame doesn't help much (or at all)
for this query. Reasons:
- Parquet is already efficient at reading only needed columns
- The aggregation step (millions → ~365 rows) is the cheap part
- Caching stores all columns, wasting memory

A better approach: cache the **daily aggregation** if you're running multiple
window queries on it.

</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| explain() | Physical plans, all plan levels |
| Shuffle identification | Exchange nodes, stage boundaries |
| Column pruning | ReadSchema, PushedFilters |
| Partitioning | repartition, coalesce, partition counts |
| Partitioned writes | partitionBy, partition pruning |
| Broadcast joins | broadcast() hint, plan verification |
| Sort-merge joins | Disabled broadcast, compared plans |
| Caching | cache/unpersist, when it helps/hurts |
| Data skew | Distribution checks, salting technique |
| Optimization workflow | Iterative improve + measure approach |

> **Key lesson**: Always `explain()` before and after optimizing. Measure
> execution time to confirm your optimization actually helped. Not every
> "optimization" makes things faster, understanding *why* is more important
> than memorizing rules.

---

## Cleanup

```bash
docker compose down
```

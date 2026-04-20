# Spark DataFrames Practice

## Setup

Reuses the same Docker environment as Session 05. From your local git repository:

```bash
cd 06-spark-dataframes
docker compose up -d --build
```

Open your browser at **http://localhost:8888**.

The **Spark UI** will be available at **http://localhost:4040** once you start a SparkSession.
Not yet available.

---

## Data

The e-commerce orders dataset (`orders.csv`) from Session 03 is used throughout.
If you do not have it cached locally, download it inside the notebook:

```python
import urllib.request

url = "https://huggingface.co/datasets/BigDataUB/synthetic-columnar/resolve/main/orders.csv"
print("Downloading dataset (~830 MB, this will take a few minutes)...")
urllib.request.urlretrieve(url, "/app/data/orders.csv")
print("Done!")
```

If you have it copy it under the `06-spark-dataframes/data` directory to avoid downloading again.

---

## Notebook Setup

Use the following cell as the first cell in your notebook:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, avg, count, min, max, desc

spark = SparkSession.builder \
    .appName("DataFramesPractice") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
```

Note we use `SparkSession` now, not `SparkContext`. SparkSession is the unified
entry point for DataFrames and SQL. It creates a SparkContext internally.

---

## Part 1: Why DataFrames? (RDD Pain Points)

These exercises open the session by demonstrating the pain points of RDDs on
structured data, motivating DataFrames before introducing the API.

First, load the data as an RDD (same as Session 05):

```python
raw = spark.sparkContext.textFile("/app/data/orders.csv")
header = raw.first()
rows = raw.filter(lambda line: line != header)

def parse(line):
    fields = line.split(",")
    return {
        "order_id":       fields[0],
        "order_date":     fields[1],
        "customer_id":    fields[2],
        "country":        fields[3],
        "category":       fields[4],
        "product":        fields[5],
        "quantity":       int(fields[6]),
        "unit_price":     float(fields[7]),
        "total_amount":   float(fields[8]),
        "payment_method": fields[9],
        "status":         fields[10],
    }

records = rows.map(parse)
```

---

### Exercise 1: Verbosity

**Q1.** Compute the average order value (`quantity * unit_price`) per product
category, for orders with `status == "completed"` only.
Print the top 5 categories by average value.

Write this **using RDDs first**, then compare to the DataFrame version.

<details>
<summary>RDD version</summary>

```python
result = (records
    .filter(lambda r: r["status"] == "completed")
    .map(lambda r: (r["category"], (r["quantity"] * r["unit_price"], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda x: x[0] / x[1])
    .sortBy(lambda x: -x[1]))

for cat, avg_val in result.take(5):
    print(f"{cat:20s}  {avg_val:,.2f}")
```

</details>

<details>
<summary>DataFrame version</summary>

```python
df = spark.read.csv("/app/data/orders.csv", header=True, inferSchema=True)

(df.filter(col("status") == "completed")
   .withColumn("order_value", col("quantity") * col("unit_price"))
   .groupBy("category")
   .agg(avg("order_value").alias("avg_value"))
   .orderBy(col("avg_value").desc())
   .show(5))
```

Same result. Easier to understand, reads like SQL, and Spark understands the schema and
can optimise automatically.

</details>

---

### Exercise 2: No Optimizer. Spark Can't Help You With RDDs

**Q2.** Time the two pipelines below. They produce the same result.
Which is faster, and why?

**Pipeline A** (filter late):
```python
import time

t0 = time.time()
result_a = (records
    .map(lambda r: (r["category"], r["quantity"] * r["unit_price"]))
    .reduceByKey(lambda a, b: a + b)
    .filter(lambda x: x[0] in ["Electronics", "Clothing"]))
result_a.count()
t1 = time.time()
print(f"Pipeline A: {t1 - t0:.2f}s")
```

**Pipeline B** (filter early):
```python
t0 = time.time()
result_b = (records
    .filter(lambda r: r["category"] in ["Electronics", "Clothing"])
    .map(lambda r: (r["category"], r["quantity"] * r["unit_price"]))
    .reduceByKey(lambda a, b: a + b))
result_b.count()
t1 = time.time()
print(f"Pipeline B: {t1 - t0:.2f}s")
```

<details>
<summary>Answer</summary>

**Pipeline B is faster.** Filtering early reduces the records entering the shuffle.
In Pipeline A, `reduceByKey` shuffles all 10 categories before discarding most of them.

**Spark does not reorder RDD pipelines.** You are the optimizer.
If you write the filter late, Spark runs it late.

With DataFrames, the **Catalyst optimizer** pushes filters down automatically,
you get Pipeline B's performance even if you write Pipeline A's code.

Now run the same query as a DataFrame and call `.explain()` to see Catalyst's plan:

```python
from pyspark.sql.functions import sum as spark_sum

(df.filter(col("category").isin("Electronics", "Clothing"))
   .groupBy("category")
   .agg(spark_sum(col("quantity") * col("unit_price")).alias("revenue"))
   .explain())
```

Look for `PushedFilters` in the output. Catalyst already applied the filter
before touching the data.

</details>

---

### Exercise 3: The Catalyst Plan

**Q3.** Download the classic literature books (same as Session 05) and run
WordCount as a DataFrame. Inspect the query plan.

```python
import urllib.request

books = {
    "frankenstein.txt": "https://www.gutenberg.org/files/84/84-0.txt",
    "dracula.txt":      "https://www.gutenberg.org/files/345/345-0.txt",
    "sherlock.txt":     "https://www.gutenberg.org/files/1661/1661-0.txt",
}

for filename, url in books.items():
    dest = f"/app/data/{filename}"
    print(f"Downloading {filename}...")
    urllib.request.urlretrieve(url, dest)
```

```python
from pyspark.sql.functions import explode, split, lower, regexp_replace, desc

df_books = spark.read.text("/app/data/*.txt")

word_counts_df = (df_books
    .select(explode(split(lower(regexp_replace(col("value"), "[^a-z ]", " ")), " ")).alias("word"))
    .filter(col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(desc("count")))

word_counts_df.show(10)
word_counts_df.explain()
```

What stages does Catalyst plan? Compare to the two-stage RDD DAG from Session 05.

<details>
<summary>Answer</summary>

The physical plan shows Catalyst's decisions: which operations to fuse, where
to sort, how to partition the shuffle. With RDDs there is no plan. Spark
executes exactly what you wrote.

- **RDD**: you specify *how*. Spark obeys
- **DataFrame**: you specify *what*. Spark decides *how*

</details>

---

## Part 2: DataFrame API Basics

Load the orders dataset as a DataFrame:

```python
df = spark.read.csv("/app/data/orders.csv", header=True, inferSchema=True)
```

---

### Exercise 4: Schema

**Q4.** Print the schema of the DataFrame. What types did `inferSchema` assign?
How many rows are there?

<details>
<summary>Answer</summary>

```python
df.printSchema()
print(f"Rows: {df.count()}")
```

`inferSchema=True` reads a sample of the data to guess types. It should detect
integers for `order_id`, `customer_id`, `quantity`; doubles for `unit_price`,
`total_amount`; and strings for the rest.

Compare this to the RDD approach: with RDDs you had no schema at all. Spark
treated every line as an opaque string and you had to parse manually.

</details>

---

**Q5.** What happens if you load the CSV without `inferSchema=True`?
Print the schema again.

<details>
<summary>Answer</summary>

```python
df_no_infer = spark.read.csv("/app/data/orders.csv", header=True)
df_no_infer.printSchema()
```

All columns are `string`. Without `inferSchema`, Spark does not read a sample
to guess types, it treats everything as strings. This is faster to load but
means arithmetic operations will fail unless you cast explicitly.

</details>

---

### Exercise 5: select and withColumn

**Q6.** Select only the columns `order_id`, `product`, `quantity`, and `unit_price`.
Add a computed column `revenue` = `quantity * unit_price`.

<details>
<summary>Answer</summary>

```python
df.select("order_id", "product", "quantity", "unit_price",
          (col("quantity") * col("unit_price")).alias("revenue")
).show(5)
```

</details>

---

**Q7.** Using `withColumn`, add a column `year` extracted from `order_date`.

<details>
<summary>Hint</summary>

```python
from pyspark.sql.functions import year
```

`year()` extracts the year from a date column. But `order_date` might be a string.
You may need to cast it first.

</details>

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import year, to_date

df_with_year = df.withColumn("order_date", to_date(col("order_date"))) \
                 .withColumn("year", year(col("order_date")))

df_with_year.select("order_id", "order_date", "year").show(5)
```

</details>

---

### Exercise 6: filter / where

**Q8.** Filter for completed orders from Spain (`country == "ES"`) with
`quantity >= 3`. How many are there?

<details>
<summary>Answer</summary>

```python
spain_bulk = df.filter(
    (col("status") == "completed") &
    (col("country") == "ES") &
    (col("quantity") >= 3)
)
print(spain_bulk.count())
spain_bulk.show(5)
```

</details>

---

**Q9.** Find all orders where `total_amount` is null. Are there any?

<details>
<summary>Answer</summary>

```python
nulls = df.filter(col("total_amount").isNull())
print(f"Null total_amount: {nulls.count()}")
```

</details>

---

### Exercise 7: groupBy and agg

**Q10.** Count the number of orders per country. Show the top 5 countries.

<details>
<summary>Answer</summary>

```python
df.groupBy("country") \
  .count() \
  .orderBy(col("count").desc()) \
  .show(5)
```

</details>

---

**Q11.** For each product category, compute:
- Total number of orders
- Total revenue (`sum of total_amount`)
- Average order value
- Maximum single order value

Sort by total revenue descending.

<details>
<summary>Answer</summary>

```python
df.groupBy("category").agg(
    count("*").alias("num_orders"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value"),
    max("total_amount").alias("max_order_value")
).orderBy(col("total_revenue").desc()).show()
```

</details>

---

**Q12.** Compute total revenue per country per year. Which country+year
combination had the highest revenue?

<details>
<summary>Hint</summary>

Use the `df_with_year` DataFrame from Q7, or extract the year inline.

</details>

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import year, to_date

revenue_by_country_year = (df
    .withColumn("year", year(to_date(col("order_date"))))
    .groupBy("country", "year")
    .agg(sum("total_amount").alias("revenue"))
    .orderBy(col("revenue").desc()))

revenue_by_country_year.show(10)
```

</details>

---

### Exercise 8: orderBy and limit

**Q13.** Find the 10 most expensive individual orders (by `total_amount`).
Show the order_id, product, country, and total_amount.

<details>
<summary>Answer</summary>

```python
df.select("order_id", "product", "country", "total_amount") \
  .orderBy(col("total_amount").desc()) \
  .show(10)
```

Try calling `.explain()` on the query with and without `limit(10)`:

```python
# With limit: optimized, no full shuffle
df.select("order_id", "product", "country", "total_amount") \
  .orderBy(col("total_amount").desc()) \
  .limit(10) \
  .explain()

# Without limit: full shuffle
df.select("order_id", "product", "country", "total_amount") \
  .orderBy(col("total_amount").desc()) \
  .explain()
```

With `limit(10)`, Spark uses `TakeOrderedAndProject`, picks the top 10 per
partition locally and merges them, no full shuffle needed.

Without `limit`, Spark must do a full global sort: `Sort + Exchange`
(shuffle all data across partitions). Check the Spark UI to see the
two stages separated by the shuffle boundary.

</details>

---

## Part 3: Joins

The orders dataset has `customer_id` but no customer details. We will build a
customers DataFrame from the orders data, then practice joins.

### Building a Customers DataFrame

```python
from pyspark.sql.functions import first, countDistinct

customers = (df
    .groupBy("customer_id")
    .agg(
        first("country").alias("country"),
        countDistinct("order_id").alias("total_orders"),
        sum("total_amount").alias("lifetime_value")
    ))

print(f"Customers: {customers.count()}")
customers.show(5)
```

This gives us one row per customer with their country, order count, and total spend.

---

### Exercise 9: Inner Join

**Q14.** Join orders with customers on `customer_id` (inner join).
Select `order_id`, `product`, `total_amount`, and the customer's
`lifetime_value`. Show 5 rows.

<details>
<summary>Answer</summary>

```python
orders_with_customers = df.join(customers, "customer_id")

orders_with_customers.select(
    "customer_id", "order_id", "product", "total_amount", "lifetime_value"
).show(5)
```

When you join on a column name string (`"customer_id"`), Spark avoids
duplicating the join column. If you use an expression
(`df.customer_id == customers.customer_id`), both columns appear and you
need to drop one.

</details>

---

**Q15.** Call `.explain()` on the join. What join strategy does Spark choose?
Is it a `BroadcastHashJoin` or `SortMergeJoin`?

<details>
<summary>Answer</summary>

```python
orders_with_customers.explain()
```

Spark checks the size of each side. If `customers` is small enough
(default threshold: 10 MB), Spark broadcasts it to all executors
(`BroadcastHashJoin`). Otherwise it uses `SortMergeJoin` (both sides
shuffled and sorted by key).

Since our customers DataFrame has ~500K rows, it exceeds the broadcast
threshold and Spark chooses `SortMergeJoin`.

Notice that `FileScan csv` appears **twice** in the plan, Spark reads
`orders.csv` from scratch for both sides of the join, once for orders and
once to recompute the customers aggregation. Try caching the customers
DataFrame and compare:

```python
customers.cache()
customers.count()  # materialise the cache

orders_with_customers = df.join(customers, "customer_id")
orders_with_customers.explain()
```

With caching, the right side reads from memory instead of re-reading
and re-aggregating the CSV.

</details>

---

### Exercise 10: Left Join and Left Anti Join

**Q16.** Create a small DataFrame of "VIP countries" and use a left join to tag
orders from those countries.

```python
vip_countries = spark.createDataFrame([
    ("ES", "Gold"),
    ("UK", "Gold"),
    ("DE", "Silver"),
    ("FR", "Silver"),
], ["country", "vip_tier"])
```

Left join orders with `vip_countries` on `country`. Orders from non-VIP
countries should have `NULL` in the `vip_tier` column.

<details>
<summary>Answer</summary>

```python
orders_tagged = df.join(vip_countries, "country", "left")
orders_tagged.select("order_id", "country", "product", "vip_tier").show(10)
```

Rows from countries not in `vip_countries` (e.g., ES, JP) will have
`vip_tier = NULL`.

</details>

---

**Q17.** Use a left anti join to find orders from countries that are NOT in the
VIP list. How many are there? Which countries are they from?

<details>
<summary>Answer</summary>

```python
non_vip_orders = df.join(vip_countries, "country", "left_anti")
print(f"Non-VIP orders: {non_vip_orders.count()}")

non_vip_orders.select("country").distinct().show()
```

Left anti returns rows from the left table that have **no match** in the right.
This is useful for finding missing or unmatched records.

</details>

---

### Exercise 11: Join + Aggregation

**Q18.** Using the `customers` DataFrame, find the top 10 customers by
`lifetime_value`. Then join back with orders to see what products those
top customers bought most frequently.

<details>
<summary>Answer</summary>

```python
top_customers = customers.orderBy(col("lifetime_value").desc()).limit(10)

top_customer_products = (df
    .join(top_customers.select("customer_id"), "customer_id")
    .groupBy("product")
    .count()
    .orderBy(col("count").desc()))

top_customer_products.show(10)
```

</details>

---

## Part 4: Reading Query Plans

### Exercise 12: Comparing Plans

**Q19.** Run the following query and inspect the full plan with `explain(True)`:

```python
result = (df
    .filter(col("status") == "completed")
    .filter(col("country") == "ES")
    .select("category", "total_amount")
    .groupBy("category")
    .agg(sum("total_amount").alias("revenue")))

result.explain(True)
```

Look at each plan stage:
- **Parsed Logical Plan**: direct translation of your code
- **Analyzed Logical Plan**: types resolved
- **Optimized Logical Plan**: what did Catalyst change?
- **Physical Plan**: what algorithms were chosen?

<details>
<summary>Answer</summary>

Key things to notice:
- The two consecutive `filter` calls are **combined** into a single filter
  with `AND` in the optimized plan
- **Column pruning**: only `status`, `country`, `category`, `total_amount`
  are read from the CSV, not all 11 columns
- The physical plan shows `HashAggregate` with `partial_sum` (local pre-aggregation)
  followed by `Exchange` (shuffle) and final `HashAggregate`, same two-phase
  pattern as `reduceByKey`

</details>

---

**Q20.** Write the filter AFTER the groupBy (logically wrong order but
same result). Does `explain()` show the same physical plan?

```python
result_late_filter = (df
    .select("status", "country", "category", "total_amount")
    .groupBy("status", "country", "category")
    .agg(sum("total_amount").alias("revenue"))
    .filter(col("status") == "completed")
    .filter(col("country") == "ES"))

result_late_filter.explain(True)
```

<details>
<summary>Answer</summary>

Catalyst is smarter than you might expect here. Look at the optimized logical
plan: the filters are **pushed below the aggregation**, even though you wrote
them above it. Catalyst can do this because `status` and `country` are grouping
keys, filtering before or after grouping by those columns gives the same result.

However, the plan is still **different** from Q19. The `groupBy` includes
`status`, `country`, and `category` as grouping keys (3 keys), so the shuffle
partitions by all three. In Q19, Spark filters first and groups only by
`category` (1 key), less data shuffled, fewer partitions.

The takeaway: Catalyst is smart and can push filters past aggregations when
the filter columns are grouping keys. But writing the query cleanly (Q19 style)
still produces a more efficient plan because the groupBy itself is simpler.

</details>

---

## Part 5: Writing DataFrames

### Exercise 13: Write to Parquet

**Q21.** Filter for completed orders from 2024, select `order_id`, `country`,
`category`, `product`, `total_amount`, and write the result as Parquet.

```python
from pyspark.sql.functions import year, to_date

completed_2024 = (df
    .withColumn("order_date", to_date(col("order_date")))
    .filter((col("status") == "completed") & (year(col("order_date")) == 2024))
    .select("order_id", "country", "category", "product", "total_amount"))
```

Write it to `/app/data/output/completed_2024.parquet`.

<details>
<summary>Answer</summary>

```python
completed_2024.write.mode("overwrite").parquet("/app/data/output/completed_2024.parquet")
```

</details>

---

**Q22.** Read the Parquet file back. Print the schema. How does it compare
to reading the original CSV?

<details>
<summary>Answer</summary>

```python
df_parquet = spark.read.parquet("/app/data/output/completed_2024.parquet")
df_parquet.printSchema()
print(f"Rows: {df_parquet.count()}")
```

The Parquet file preserves the schema (column names and types) in its metadata.
No need for `inferSchema` or `header=True`. The types are exactly what was
written, not re-guessed from string representations.

</details>

---

### Exercise 14: Partitioned Writes

**Q23.** Write the full orders DataFrame as Parquet, partitioned by `country`.
Then read back just the data for Spain (`country = "ES"`).

<details>
<summary>Answer</summary>

```python
# Write partitioned
df.write.mode("overwrite").partitionBy("country").parquet("/app/data/output/orders_by_country")
```

```python
# Read back just Spain
df_spain = spark.read.parquet("/app/data/output/orders_by_country/country=ES")
print(f"Spain orders: {df_spain.count()}")
df_spain.show(5)
```

When you partition by `country`, Spark creates a directory per country value.
Reading a single partition only touches that directory, skipping all other
countries entirely. This is **partition pruning**, and it is visible in the
physical plan when you filter on the partition column.

```python
# Partition pruning in action
df_all = spark.read.parquet("/app/data/output/orders_by_country")
df_all.filter(col("country") == "ES").explain()
# Look for PartitionFilters in the plan
```

</details>

---

## Part 6: RDD vs DataFrame Performance

### Exercise 15: Same Query, Both APIs

**Q24.** Compute total revenue per category for completed orders.
Time both the RDD and DataFrame versions.

<details>
<summary>Answer</summary>

```python
import time

# --- RDD version ---
t0 = time.time()
rdd_result = (records
    .filter(lambda r: r["status"] == "completed")
    .map(lambda r: (r["category"], r["quantity"] * r["unit_price"]))
    .reduceByKey(lambda a, b: a + b)
    .collect())
t1 = time.time()
print(f"RDD:       {t1 - t0:.2f}s")

# --- DataFrame version ---
t0 = time.time()
df_result = (df
    .filter(col("status") == "completed")
    .groupBy("category")
    .agg(sum(col("quantity") * col("unit_price")).alias("revenue"))
    .collect())
t1 = time.time()
print(f"DataFrame: {t1 - t0:.2f}s")
```

The DataFrame version should be noticeably faster because:
1. **No JVM-Python serialization**: computation stays in the JVM (Tungsten)
2. **Catalyst optimizations**: predicate pushdown, column pruning
3. **Tungsten binary format**: no Python object overhead

</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| RDD pain points | Verbosity, no optimizer, serialization overhead |
| Schema | `printSchema`, `inferSchema`, type safety |
| DataFrame API | select, filter, withColumn, groupBy, agg, orderBy |
| Joins | inner, left, left_anti, join + aggregation |
| Query plans | `explain()`, `explain(True)`, predicate pushdown, column pruning |
| Writing | Parquet, write modes, partitioned writes, partition pruning |
| Performance | RDD vs DataFrame timing comparison |

> **DataFrames are the standard API for Spark.** RDDs are the foundation
> underneath, but you should use DataFrames for all structured data work.
> Next session: Spark SQL and window functions.

---

## Cleanup

```bash
docker compose down
```

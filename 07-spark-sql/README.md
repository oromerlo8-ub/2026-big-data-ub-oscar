# Spark SQL & Window Functions Practice

## Setup

Reuses the same Docker environment as Sessions 05 and 06. From your local git repository:

```bash
cd 07-spark-sql
docker compose up -d --build
```

Open your browser at **http://localhost:8888**.

The **Spark UI** will be available at **http://localhost:4040** once you start a SparkSession.

---

## Data

The e-commerce orders dataset (`orders.csv`) from previous sessions is used throughout.
If you do not have it cached locally, download it inside the notebook:

```python
import urllib.request

url = "https://huggingface.co/datasets/BigDataUB/synthetic-columnar/resolve/main/orders.csv"
print("Downloading dataset (~830 MB, this will take a few minutes)...")
urllib.request.urlretrieve(url, "/app/data/orders.csv")
print("Done!")
```

If you have it, copy it under the `07-spark-sql/data` directory to avoid downloading again.

`cp ../06-spark-dataframes/data/orders.csv ./data/orders.csv`

---

## Notebook Setup

Use the following cell as the first cell in your notebook.
This will import functions we will be using during the exercise:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum, avg, count, min, max, desc,
    year, month, to_date, date_format, date_trunc,
    lower, upper, trim, split, regexp_extract, regexp_replace,
    explode, collect_list, collect_set, array_contains, size,
    when, row_number, rank, dense_rank, ntile, lag, lead,
    first, last, pandas_udf
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SparkSQLPractice") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
```

Load the dataset:

```python
df = spark.read.csv("/app/data/orders.csv", header=True, inferSchema=True)
df.printSchema()
print(f"Rows: {df.count()}")
```

---

## Part 1: SQL Fundamentals (25 min)

### Exercise 1: Temp Views and Basic SQL

**Q1.** Register the DataFrame as a temporary view called `orders`.
Then use `spark.sql()` to select the first 10 rows.

<details>
<summary>Answer</summary>

```python
df.createOrReplaceTempView("orders")

spark.sql("SELECT * FROM orders LIMIT 10").show()
```

`createOrReplaceTempView` gives the DataFrame a table name that SQL can reference.
The result of `spark.sql()` is itself a DataFrame.

</details>

---

**Q2.** Write a SQL query to find the top 10 customers by total revenue
(`SUM(total_amount)`). Show `customer_id` and `total_revenue`.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT customer_id, SUM(total_amount) as total_revenue
    FROM orders
    GROUP BY customer_id
    ORDER BY total_revenue DESC
    LIMIT 10
""").show()
```

</details>

---

**Q3.** Write a SQL query to compute total revenue per product category,
but only for completed orders. Sort by revenue descending.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT category, SUM(total_amount) as revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY category
    ORDER BY revenue DESC
""").show()
```

</details>

---

### Exercise 2: SQL ↔ DataFrame Interop

**Q4.** Start with a SQL query that computes average order value per country.
Store the result in a variable. Then use the **DataFrame API** (not SQL) to
filter for countries with average value > 179 and sort descending.

<details>
<summary>Answer</summary>

```python
avg_by_country = spark.sql("""
    SELECT country, AVG(total_amount) as avg_value
    FROM orders
    GROUP BY country
""")

# Continue with DataFrame API
avg_by_country.filter(col("avg_value") > 179) \
    .orderBy(col("avg_value").desc()) \
    .show()
```

The result of `spark.sql()` is a regular DataFrame. You can chain DataFrame
operations on it or register it as another view.

</details>

---

### Exercise 3: Subqueries and CTEs

**Q5.** Write a SQL subquery to find all orders from customers who have
more than 30 total orders. How many such orders are there?

<details>
<summary>Hint</summary>

Use a subquery in the WHERE clause:
```sql
WHERE customer_id IN (SELECT ... FROM ... GROUP BY ... HAVING ...)
```

</details>

<details>
<summary>Answer</summary>

```python
result = spark.sql("""
    SELECT *
    FROM orders
    WHERE customer_id IN (
        SELECT customer_id
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) > 30
    )
""")
print(f"Orders from frequent customers: {result.count()}")
```

</details>

---

**Q6.** Rewrite Q5 using a CTE (WITH clause). Then extend it: compute the
total revenue for these frequent customers, grouped by category.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    WITH frequent_customers AS (
        SELECT customer_id
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) > 30
    )
    SELECT o.category, SUM(o.total_amount) as revenue
    FROM orders o
    JOIN frequent_customers fc ON o.customer_id = fc.customer_id
    GROUP BY o.category
    ORDER BY revenue DESC
""").show()
```

CTEs make complex queries readable by breaking them into named steps.

</details>

---

### Exercise 4: CASE Expressions

**Q7.** Write a SQL query that adds a `price_tier` column to orders:
- `total_amount > 500` → 'premium'
- `total_amount > 100` → 'standard'
- otherwise → 'budget'

Show 10 rows with `order_id`, `product`, `total_amount`, and `price_tier`.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT order_id, product, total_amount,
        CASE WHEN total_amount > 500 THEN 'premium'
             WHEN total_amount > 100 THEN 'standard'
             ELSE 'budget' END as price_tier
    FROM orders
    LIMIT 10
""").show()
```

CASE returns a new column. The original columns are unchanged.

</details>

---

### Exercise 5: ROLLUP

**Q8.** Use ROLLUP to compute total revenue with subtotals by country and
category. Look at the rows where `category` is NULL. What do they represent?
What about the row where both are NULL?

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT country, category, SUM(total_amount) as revenue
    FROM orders
    GROUP BY ROLLUP(country, category)
    ORDER BY country, category
""").show(30)
```

- Rows with `category = NULL`: subtotal for that country (all categories)
- Row with both NULL: grand total (all countries, all categories)

ROLLUP produces hierarchical subtotals: detail → country subtotal → grand total.

</details>

---

## Part 2: Window Functions (45 min)

For these exercises, we'll create a simpler view to make results easier to read.

```python
spark.sql("""
    SELECT customer_id, country, category, total_amount,
           CAST(order_date AS DATE) as order_date
    FROM orders
    WHERE status = 'completed'
""").createOrReplaceTempView("completed_orders")
```

---

### Exercise 6: Ranking

**Q9.** For each country, rank customers by their total spend. Use
ROW_NUMBER, RANK, and DENSE_RANK in the same query. Show the top 5 per
country for 3 countries of your choice.

<details>
<summary>Hint</summary>

```sql
ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_spend DESC)
```

First compute total spend per customer per country, then apply rankings.

</details>

<details>
<summary>Answer</summary>

```python
spark.sql("""
    WITH customer_spend AS (
        SELECT country, customer_id, SUM(total_amount) as total_spend
        FROM completed_orders
        GROUP BY country, customer_id
    ),
    ranked AS (
        SELECT country, customer_id, total_spend,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_spend DESC) as row_num,
            RANK()       OVER (PARTITION BY country ORDER BY total_spend DESC) as rnk,
            DENSE_RANK() OVER (PARTITION BY country ORDER BY total_spend DESC) as dense_rnk
        FROM customer_spend
    )
    SELECT * FROM ranked
    WHERE country IN ('ES', 'UK', 'DE') AND row_num <= 5
    ORDER BY country, row_num
""").show(15)
```

You'll notice all three columns show the same values. No ties exist because
`total_spend` is a sum of floats (exact matches are nearly impossible).

To actually **see** the difference, round the spend to the nearest 1000 to force ties:

```python
spark.sql("""
    WITH customer_spend AS (
        SELECT country, customer_id,
               ROUND(SUM(total_amount), -3) as spend_bucket
        FROM completed_orders
        GROUP BY country, customer_id
    ),
    ranked AS (
        SELECT country, customer_id, spend_bucket,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY spend_bucket DESC) as row_num,
            RANK()       OVER (PARTITION BY country ORDER BY spend_bucket DESC) as rnk,
            DENSE_RANK() OVER (PARTITION BY country ORDER BY spend_bucket DESC) as dense_rnk
        FROM customer_spend
    )
    SELECT * FROM ranked
    WHERE country = 'ES' AND row_num <= 10
    ORDER BY row_num
""").show()
```

Now you should see ties, and the difference becomes clear:
- ROW_NUMBER: different numbers even for same `spend_bucket` (arbitrary tiebreak)
- RANK: same number for ties, gap after (e.g. 1,1,3)
- DENSE_RANK: same number for ties, no gap (e.g. 1,1,2)

</details>

---

**Q10.** Use NTILE(4) to divide customers into spending quartiles (per country).
How many customers are in each quartile for Spain?

<details>
<summary>Answer</summary>

```python
spark.sql("""
    WITH customer_spend AS (
        SELECT country, customer_id, SUM(total_amount) as total_spend
        FROM completed_orders
        GROUP BY country, customer_id
    ),
    quartiled AS (
        SELECT country, customer_id, total_spend,
            NTILE(4) OVER (PARTITION BY country ORDER BY total_spend) as quartile
        FROM customer_spend
    )
    SELECT quartile, COUNT(*) as num_customers,
           MIN(total_spend) as min_spend, MAX(total_spend) as max_spend
    FROM quartiled
    WHERE country = 'ES'
    GROUP BY quartile
    ORDER BY quartile
""").show()
```

NTILE(4) distributes rows as evenly as possible into 4 groups.
Quartile 1 = lowest spenders, quartile 4 = highest.

</details>

---

### Exercise 7: Running Totals

**Q11.** Compute daily revenue (sum of `total_amount` per day) and a
running total that accumulates over time.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    WITH daily AS (
        SELECT order_date, SUM(total_amount) as daily_revenue
        FROM completed_orders
        GROUP BY order_date
    )
    SELECT order_date, daily_revenue,
        SUM(daily_revenue) OVER (ORDER BY order_date) as running_total
    FROM daily
    ORDER BY order_date
""").show(20)
```

`SUM() OVER (ORDER BY date)` accumulates from the first row to the current row.

</details>

---

### Exercise 8: Moving Average

**Q12.** Using the same daily revenue data, compute a 7-day moving average.
For the first 6 days, the average will be based on fewer than 7 values.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    WITH daily AS (
        SELECT order_date, SUM(total_amount) as daily_revenue
        FROM completed_orders
        GROUP BY order_date
    )
    SELECT order_date, daily_revenue,
        AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d
    FROM daily
    ORDER BY order_date
""").show(20)
```

`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` = the current row plus up to
6 rows before it = a 7-row window. For the first row, only 1 row is
available, so the average equals that day's revenue.

</details>

---

### Exercise 9: LAG and LEAD

**Q13.** For each day, compute the day-over-day change in revenue and the
percentage change. Use LAG to access the previous day's revenue.

<details>
<summary>Hint</summary>

```sql
LAG(daily_revenue, 1) OVER (ORDER BY order_date) as prev_day_revenue
```

The first day will have NULL for prev_day (no previous row exists).
You can use `LAG(daily_revenue, 1, 0)` to default to 0 instead.

</details>

<details>
<summary>Answer</summary>

```python
spark.sql("""
    WITH daily AS (
        SELECT order_date, SUM(total_amount) as daily_revenue
        FROM completed_orders
        GROUP BY order_date
    )
    SELECT order_date, daily_revenue,
        LAG(daily_revenue, 1) OVER (ORDER BY order_date) as prev_day,
        daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY order_date) as change,
        ROUND(
            (daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY order_date))
            / LAG(daily_revenue, 1) OVER (ORDER BY order_date) * 100, 1
        ) as pct_change
    FROM daily
    ORDER BY order_date
""").show(20)
```

The first row has NULL for `prev_day`, `change`, and `pct_change` because
there is no previous row to compare to.

</details>

---

### Exercise 10: FIRST_VALUE and LAST_VALUE

**Q14.** For each category, show every order along with the highest and
lowest `total_amount` in that category (using FIRST_VALUE and LAST_VALUE).

<details>
<summary>Hint</summary>

Remember the LAST_VALUE gotcha! The default frame only sees up to the current
row, so LAST_VALUE returns the current row instead of the actual last.
You need: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`

</details>

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT category, customer_id, total_amount,
        FIRST_VALUE(total_amount) OVER (
            PARTITION BY category ORDER BY total_amount DESC
        ) as highest_in_category,
        LAST_VALUE(total_amount) OVER (
            PARTITION BY category ORDER BY total_amount DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as lowest_in_category
    FROM completed_orders
    LIMIT 20
""").show()
```

Without the ROWS clause on LAST_VALUE, you would get the current row's value
instead of the actual minimum. Try removing it to see the wrong result.

</details>

---

## Part 3: Dates, Strings & Arrays (20 min)

### Exercise 11: Date Operations

**Q15.** Extract year and month from `order_date`. Compute monthly revenue
and show it sorted by year and month.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT YEAR(order_date) as yr, MONTH(order_date) as mo,
           CAST(SUM(total_amount) AS DECIMAL(12,2)) as monthly_revenue
    FROM completed_orders
    GROUP BY yr, mo
    ORDER BY yr, mo
""").show(24)
```

</details>

---

**Q16.** Use `date_trunc` to group revenue by week. Which week had the
highest revenue?

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT DATE_TRUNC('week', order_date) as week_start,
           SUM(total_amount) as weekly_revenue
    FROM completed_orders
    GROUP BY week_start
    ORDER BY weekly_revenue DESC
    LIMIT 5
""").show()
```

`DATE_TRUNC('week', date)` truncates to the Monday of that week.

</details>

---

### Exercise 12: String Operations (DataFrame API)

**Q17.** Using the **DataFrame API** (not SQL), create a `product_code` column
by combining the uppercase first 3 letters of the category with the uppercase
first 3 letters of the product, separated by a dash
(e.g. "ELE-LAP" for Electronics / Laptop).
Then count orders per product code. Show the top 10.

<details>
<summary>Hint</summary>

Use `upper()`, `substring()`, and `concat_ws()` from `pyspark.sql.functions`.

```python
from pyspark.sql.functions import upper, substring, concat_ws
```

</details>

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import upper, substring, concat_ws

df_codes = (df
    .withColumn("product_code",
        concat_ws("-",
            upper(substring(col("category"), 1, 3)),
            upper(substring(col("product"), 1, 3))
        ))
    .groupBy("product_code")
    .count()
    .orderBy(col("count").desc()))

df_codes.show(10)
```

</details>

---

### Exercise 13: Arrays (explode and collect)

**Q18.** Each customer has bought from multiple categories. Use
`collect_set` to create an array of distinct categories per customer.
Then use `size()` to find which customers bought from the most categories.

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import collect_set, size

customer_categories = (df
    .groupBy("customer_id")
    .agg(collect_set("category").alias("categories"))
    .withColumn("num_categories", size("categories"))
    .orderBy(col("num_categories").desc()))

customer_categories.show(10, truncate=False)
```

</details>

---

**Q19.** Take the result from Q18 and use `explode` to go back to one row
per customer per category. Verify the row count matches the original
distinct (customer_id, category) pairs.

<details>
<summary>Answer</summary>

```python
exploded = customer_categories.select(
    "customer_id",
    explode("categories").alias("category")
)

original_pairs = df.select("customer_id", "category").distinct().count()
exploded_count = exploded.count()

print(f"Original distinct pairs: {original_pairs}")
print(f"After collect_set + explode: {exploded_count}")
```

The counts should match. `collect_set` aggregates to arrays of unique values,
`explode` flattens them back; round-tripping the data.

</details>

---

## Part 4: SQL vs DataFrame Comparison (10 min)

### Exercise 14: Same Query, Both Ways

**Q20.** Write the following query in both SQL and DataFrame API:

> For each country, find the top 3 product categories by revenue
> (completed orders only). Show country, category, revenue, and rank.

<details>
<summary>SQL version</summary>

```python
spark.sql("""
    WITH category_revenue AS (
        SELECT country, category, SUM(total_amount) as revenue,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY SUM(total_amount) DESC) as rn
        FROM completed_orders
        GROUP BY country, category
    )
    SELECT country, category, revenue, rn as rank
    FROM category_revenue
    WHERE rn <= 3
    ORDER BY country, rank
""").show(30)
```

</details>

<details>
<summary>DataFrame version</summary>

```python
window = Window.partitionBy("country").orderBy(col("revenue").desc())

result = (df
    .filter(col("status") == "completed")
    .groupBy("country", "category")
    .agg(sum("total_amount").alias("revenue"))
    .withColumn("rank", row_number().over(window))
    .filter(col("rank") <= 3)
    .orderBy("country", "rank"))

result.show(30)
```

</details>

<details>
<summary>Discussion</summary>

Both produce the same result and the same Catalyst plan. Choose based on
readability for your team:

- SQL: often more readable for complex queries with CTEs and window functions
- DataFrame API: better for building dynamic pipelines in code (e.g., variable column names, conditional logic)

You can mix and match. Use SQL for the complex query, then DataFrame API
for the next step. The result of `spark.sql()` is always a DataFrame.

</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| Temp views | `createOrReplaceTempView`, `spark.sql()` |
| SQL basics | SELECT, WHERE, GROUP BY, ORDER BY, LIMIT |
| Subqueries & CTEs | IN subquery, WITH clause, chaining CTEs |
| CASE | Conditional computed columns |
| ROLLUP | Hierarchical subtotals |
| Window functions | ROW_NUMBER, RANK, DENSE_RANK, NTILE |
| Running totals | SUM() OVER (ORDER BY) |
| Moving averages | AVG() OVER (ROWS BETWEEN) |
| LAG/LEAD | Day-over-day comparisons |
| FIRST/LAST_VALUE | With correct frame specification |
| Date functions | YEAR, MONTH, DATE_TRUNC |
| String functions | SPLIT, REGEXP_EXTRACT |
| Array functions | collect_set, explode, size |
| SQL vs DataFrame | Same query both ways |

> **Spark SQL and the DataFrame API share the same engine (Catalyst).** Choose
> whichever syntax is clearest for each task, and mix them freely.
> Next session: Spark Performance Optimization (after Easter break).

---

## Cleanup

```bash
docker compose down
```

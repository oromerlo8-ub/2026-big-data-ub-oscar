# Spark RDDs Practice

## Setup

From your local git repository:

```bash
cd 05-spark-rdds
```

### Build and start the environment

Review the `Dockerfile` and `compose.yaml` files.

```bash
docker compose up -d --build
```

This starts a Jupyter notebook server with Python 3.14 and PySpark pre-installed.

Open your browser at **http://localhost:8888**.

The **Spark UI** will be available at **http://localhost:4040** once you start a SparkContext inside a notebook.

---

## Exercises

Create a new notebook in the `notebooks/` directory for your work.

Use the following cell as the first cell in your notebook to initialise a local SparkContext:

```python
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDDPractice").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

print(f"Spark version: {sc.version}")
print(f"Default parallelism: {sc.defaultParallelism}")
```

`local[*]` tells Spark to run locally using all available CPU cores — one executor thread per core.

---

## Part 1: Classic Literature (Project Gutenberg)

We will use the same books from the MapReduce session. Download them inside the notebook:

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

print("Done!")
```

---

### Exercise 1: Creating RDDs and Partitions

**Q1.** Load all three books into a single RDD using `sc.textFile` with a glob pattern.
How many lines does it contain?

<details>
<summary>Answer</summary>

```python
lines = sc.textFile("/app/data/*.txt")
print(lines.count())
```

Each file is read as a separate set of lines. The glob merges them into one RDD.

</details>

---

**Q2.** How many partitions does the RDD have? How does that relate to the number of files?

<details>
<summary>Answer</summary>

```python
print(lines.getNumPartitions())
```

In local mode Spark creates at least one partition per file, so you should see 3 partitions (one per book). On a real HDFS cluster you would see one partition per 128 MB block.

</details>

---

**Q3.** Force a repartition to 8 partitions. Does `count()` return the same result?
What does `.repartition()` trigger?

<details>
<summary>Answer</summary>

```python
lines8 = lines.repartition(8)
print(lines8.getNumPartitions())  # 8
print(lines8.count())             # same number of lines
```

`repartition()` triggers a **shuffle**, data is redistributed across 8 partitions. The line count does not change because repartitioning only changes how data is distributed, not what data exists.

</details>

---

### Exercise 2: Transformations and Lazy Evaluation

**Q4.** Create an RDD of all individual words from the three books (lowercase, stripped of punctuation).
Do not use an action yet, just define the transformation chain.
Print the RDD object. You will see something like `PythonRDD[9] at RDD at PythonRDD.scala:58`,  **no words, no computation**. Why?

<details>
<summary>Answer</summary>

```python
import re

words = lines.flatMap(lambda line: re.findall(r"[a-z]+", line.lower()))
print(words)
# DO NOT WRITE THE Following, you will see something like:
# PythonRDD[...] at RDD at PythonRDD.scala:53
```

Printing the RDD does **not** execute anything. Spark records the transformation in the lineage graph but waits for an action before doing any work.

</details>

---

**Q5.** Now trigger execution with `.count()`. How many words are there in total?
Open the Spark UI at http://localhost:4040, what do you see in the Jobs tab?

<details>
<summary>Answer</summary>

```python
print(words.count())
```

After the execution of counts you should see a new job that shows one job triggered by `count()`.
Each job is split into stages; in this case there is one stage because `flatMap` is a narrow transformation (no shuffle needed).

</details>

---

**Q6.** Filter the words RDD to keep only words longer than 8 characters.
How many such words are there? Is a new stage created in the Spark UI?

<details>
<summary>Hint</summary>

`filter` is a narrow transformation, it does not move data between partitions.

</details>

<details>
<summary>Answer</summary>

```python
long_words = words.filter(lambda w: len(w) > 8)
print(long_words.count())
```

No new stage is created. `filter` is narrow (each partition is processed independently), so Spark fuses it with the previous `flatMap` into the same stage.

</details>

---

### Exercise 3: WordCount, MapReduce vs Spark

In Session 4 you implemented WordCount with a Hadoop Streaming job (~50 lines of setup, mapper, reducer). Here is the Spark version:

**Q7.** Implement WordCount, count how many times each word appears across all three books.
Print the 10 most common words.

<details>
<summary>Answer</summary>

```python
word_counts = (words
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b))

top10 = word_counts.takeOrdered(10, key=lambda x: -x[1])
for word, count in top10:
    print(f"{word:20s} {count}")
```

In the Spark UI you will now see **two stages** for that job separated by a shuffle boundary, `reduceByKey` is a wide transformation that groups all counts for the same word onto the same executor.

</details>

---

**Q8.** How many distinct words are there? How many words appear only once?

<details>
<summary>Answer</summary>

```python
print(f"Distinct words:     {word_counts.count()}")
print(f"Words seen once:    {word_counts.filter(lambda x: x[1] == 1).count()}")
```

</details>

---

### Exercise 4: reduceByKey vs groupByKey

**Q9.** Generate a synthetic dataset of 20 million word occurrences with a vocabulary of 10 000 words and compare `reduceByKey` vs `groupByKey`.

```python
import time

# Generate data inside the RDD — workers produce their own pairs locally.
# This avoids serializing a huge Python list through the driver.
pairs_large = (sc.parallelize(range(20_000_000), numSlices=8)
    .map(lambda i: (f"word_{i % 10_000}", 1)))
pairs_large.cache()
pairs_large.count()  # materialise cache before timing
```

Compare execution time between the two approaches, `reduceByKey` and `groupByKey`.

<details>
<summary>Hint</summary>

```python
t0 = time.time()
pairs_large.reduceByKey(lambda a, b: a + b).count()
t1 = time.time()

pairs_large.groupByKey().mapValues(sum).count()
t2 = time.time()

print(f"reduceByKey: {t1 - t0:.2f}s")
print(f"groupByKey:  {t2 - t1:.2f}s")
```

</details>

<details>
<summary>Answer</summary>

You will see `groupByKey` writing more shuffle data and taking slightly longer, but the difference is modest in `local[*]` mode as there is no real network, and Spark's serialization is compact.

The shuffle write difference is real but not dramatic here because the values being shuffled are just integers (`1`). The more important distinction is **memory**:

- `reduceByKey` reduces locally first, at most 10 000 partial sums per partition in memory at any time
- `groupByKey` collects all raw values per key before reducing, with 2 000 values per key across 20M records, this means buffering large lists in executor memory

In production, `groupByKey` on a large dataset with many values per key is a common cause of **executor OOM errors**. `reduceByKey` avoids this entirely because it never materialises the full list.

**Rule of thumb**: if you can express the aggregation as a combine function (sum, max, count…), always use `reduceByKey` or `aggregateByKey` instead of `groupByKey`.

</details>

---

### Exercise 5: Caching

**Q10.** Without caching, run the WordCount pipeline 5 times in a loop and measure the total time.

<details>
<summary>Answer</summary>

```python
import time

t0 = time.time()
for i in range(5):
    word_counts = (words
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b))
    word_counts.count()
t1 = time.time()
print(f"Without cache: {t1 - t0:.2f}s")
```

Each iteration re-reads the files from disk and re-runs all transformations from scratch.

</details>

---

**Q11.** Now cache the `words` RDD and repeat the loop. How much faster is it?
Check the Storage tab in the Spark UI, what do you see?

<details>
<summary>Hint</summary>

```python
words_cached = lines.flatMap(lambda line: re.findall(r"[a-z]+", line.lower())).cache()
words_cached.count()  # materialise the cache on first pass
```

</details>

<details>
<summary>Answer</summary>

```python
words_cached = lines.flatMap(lambda line: re.findall(r"[a-z]+", line.lower())).cache()

t0 = time.time()
for i in range(5):
    word_counts = (words_cached
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b))
    word_counts.count()
t1 = time.time()
print(f"With cache: {t1 - t0:.2f}s")
```

The Storage tab shows the cached RDD with its size in memory and the number of cached partitions.

After the first iteration, Spark reads `words_cached` from memory instead of re-reading the files and re-running `flatMap`. This is exactly the benefit Spark provides for iterative algorithms (K-Means, PageRank) that reuse the same dataset in every iteration.

</details>

The difference in time won't be really noticeable, we are only processing 3 books but imagine having thousands or millions of books.

---

### Exercise 6: DAG and the Spark UI

**Q12.** Build the following pipeline and trigger it with `count()`:

```python
result = (lines
    .flatMap(lambda line: re.findall(r"[a-z]+", line.lower()))
    .filter(lambda w: len(w) > 5)
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
    .filter(lambda x: x[1] > 10))
result.count()
```

In the Spark UI, open the DAG visualization for this job.

- How many stages are there?
- Where is the stage boundary?
- Which transformations are in Stage 1? Which are in Stage 2?

<details>
<summary>Answer</summary>

There are **two stages**:

- **Stage 1**: `textFile → flatMap → filter → map`, all narrow transformations, fused into a single pass per partition
- **Stage 2**: `reduceByKey → filter`, after the shuffle, narrow transformations are again fused

The stage boundary sits at `reduceByKey` because it is a wide transformation (shuffle required). The `filter` after `reduceByKey` is folded into Stage 2, Spark does not create a new stage for it because it is narrow.

</details>

---

## Part 2: E-commerce Orders: RDD Limitations

Download the same dataset used in Session 3:

```python
import urllib.request

url = "https://huggingface.co/datasets/BigDataUB/synthetic-columnar/resolve/main/orders.csv"
print("Downloading dataset (~830 MB, this will take a few minutes)...")
urllib.request.urlretrieve(url, "/app/data/orders.csv")
print("Done!")
```

---

### Exercise 7: Schema Blindness

**Q13.** Load `orders.csv` as an RDD of raw strings. Print the first 3 lines.
Can you ask Spark what columns exist? What data types are they?

<details>
<summary>Answer</summary>

```python
raw = sc.textFile("/app/data/orders.csv")
for line in raw.take(3):
    print(line)
```

There is no `.schema`, no `.columns`, no `.dtypes` on an RDD. Spark treats each line as an opaque string, it has no idea the file has columns, let alone what they are or what types they hold. **You** are the schema.

</details>

---

**Q14.** Parse the CSV manually into dicts. Then compute the total revenue (quantity × unit_price) per country.

<details>
<summary>Parsing</summary>

```python
header = raw.first()
rows = raw.filter(lambda line: line != header)

# order_id, order_date, customer_id, country, category, product,
# quantity, unit_price, total_amount, payment_method, status
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

</details>

<details>
<summary>Full Answer</summary>

```python
header = raw.first()
rows = raw.filter(lambda line: line != header)

# order_id, order_date, customer_id, country, category, product,
# quantity, unit_price, total_amount, payment_method, status
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

revenue_by_country = (records
    .map(lambda r: (r["country"], r["quantity"] * r["unit_price"]))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: -x[1]))

for country, revenue in revenue_by_country.take(5):
    print(f"{country:20s}  {revenue:,.2f}")
```

Notice how much boilerplate was needed: manual header removal, manual CSV parsing, manual field indexing. If the CSV changes column order, your code silently breaks.

</details>

## Summary

> **RDDs are the foundation.** Everything in Spark (DataFrames, SparkSQL, MLlib) is built on top of them. Understanding RDDs means you understand *why* DataFrames are faster and what happens underneath when you run a query.

---

## Cleanup

```bash
docker compose down
```

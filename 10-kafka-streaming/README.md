# Kafka & Streaming Practice

## Setup

```bash
cd 10-kafka-streaming
docker compose up -d --build
```

Wait until Kafka is healthy (about 10 seconds on first start):

```bash
docker compose logs kafka | tail -5
# Look for "Kafka Server started"
```

Two services are now running:

- **Kafka broker** (KRaft single node); reachable as `kafka:9094` from inside the Docker network and `localhost:9092` from the host machine
- **Jupyter + Spark**; we'll open it when we need it (Exercise 2)

---

## Part 1: Kafka Basics (25 min)

### Exercise 1: Create a Topic from the CLI (5 min)

Before any Python, use the shell tools shipped inside the Kafka image to create the topic we'll feed for the rest of the session. No notebook needed yet; this is a `docker exec` command run from your terminal.

> Note: a fresh broker has no user topics. We set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` in `compose.yaml`, so producers and consumers can't lazily summon a topic into existence; we have to create it explicitly.

**Q1.** Create a topic called `clicks` with **3 partitions** and replication factor 1. Then describe it and identify which broker is the **leader** for each partition.

<details>
<summary>Hint</summary>

`kafka-topics.sh` supports `--create`, `--describe`, and `--partitions`. Replication factor must be ≤ the number of brokers; we have only one.

</details>

<details>
<summary>Answer</summary>

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9094 \
    --create --topic clicks \
    --partitions 3 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9094 \
    --describe --topic clicks
```

The describe output shows three `Partition: 0/1/2` lines, each with `Leader: 1` (our only broker is `node.id=1`). With more brokers you'd see leaders distributed across them; that's how Kafka spreads write load.

</details>

---

### Exercise 2: A Python Producer (5 min)

Now leave the CLI behind and switch to Python. Open **http://localhost:8888** and create a notebook under `/app/notebooks/`. Use this as your first cell:

```python
import json, os
from kafka import KafkaProducer

# Inside the Jupyter container the broker is at kafka:9094 (Docker network).
# From the host machine you'd use localhost:9092 instead.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9094")
print(f"Bootstrap: {BOOTSTRAP}")
```

**Q2.** Send **five keyed events** to the `clicks` topic. Use `user_id` as the message key. Print the partition each event landed on (the `RecordMetadata` future tells you).

<details>
<summary>Hint</summary>

`producer.send(...)` returns a `Future`. Call `.get(timeout=5)` to block until the broker acks; the result is a `RecordMetadata` object with `.partition`, `.offset`, `.timestamp`.

</details>

<details>
<summary>Answer</summary>

```python
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

for i in range(5):
    user = f"user_{i:03d}"
    md = producer.send("clicks", key=user, value={"user_id": user, "page": "/"}).get(timeout=5)
    print(f"  user={user}  partition={md.partition}  offset={md.offset}")

producer.flush()
producer.close()
```

Different keys hash to different partitions (you'll typically see at least 2 of the 3 partitions used). If you re-run with the *same* keys, each user always lands on the *same* partition; that's the whole point of keying.

</details>

---

### Exercise 3: A Python Consumer (5 min)

For this exercise you need a steady event stream rather than a handful of one-off sends. Start the synthetic **clickstream producer** in a separate terminal and leave it running for the rest of the session:

```bash
docker compose exec jupyter python generate_clicks.py --rate 10
```

This emits ~10 JSON events per second to the `clicks` topic, each shaped like:

```json
{
  "user_id": "user_017",
  "page": "/products",
  "ts": "2026-04-27T10:30:00.123+00:00",
  "event_id": "5f3a..."
}
```

Users follow a Zipf distribution and pages are weighted, so you'll see realistic skew across partitions.

**Q3.** Run two consumers against the `clicks` topic, **back to back, with different consumer groups**. The first uses `auto_offset_reset='latest'` (Kafka's default for new groups); the second uses `auto_offset_reset='earliest'`. Compare what each one actually receives.

<details>
<summary>Hint</summary>

`auto_offset_reset` only matters when the consumer group has **no committed offset yet**, i.e. when the group is brand new. Use a fresh `group_id` each run (e.g. include a `uuid`) so the setting actually kicks in; otherwise Kafka resumes from the last committed offset and ignores the flag.

</details>

<details>
<summary>Answer</summary>

```python
from kafka import KafkaConsumer
import json, uuid

def run_consumer(reset, n=5):
    c = KafkaConsumer(
        "clicks",
        bootstrap_servers=BOOTSTRAP,
        group_id=f"demo-{uuid.uuid4()}",   # fresh group every run
        auto_offset_reset=reset,
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=5000,           # exit after 5s of silence
    )
    print(f"--- reset={reset} ---")
    for i, msg in enumerate(c):
        print(f"  P{msg.partition} O{msg.offset}  {msg.value['user_id']} -> {msg.value['page']}")
        if i + 1 >= n:
            break
    c.close()

run_consumer("latest")     # only events produced after we connect
run_consumer("earliest")   # all retained history from the topic
```

The **`latest`** consumer sees only events produced *after* it joined the group; everything before is invisible to that group forever. The **`earliest`** consumer replays from the very first message Kafka still retains.

This is the single most common production gotcha: you start a consumer, see no messages, conclude "the producer is broken." The producer is fine; your consumer just started after the messages were already past.

</details>

---

### Exercise 4: Consumer Groups (5 min)

**Q4.** Start **two consumers in the same `group_id`** and watch Kafka rebalance the partitions between them. Then stop one and watch the survivor take over. Then add a third and a fourth.

For this exercise we'll use multiple host-side terminals running short Python snippets, since two consumers inside the same Python process would just share one connection.

<details>
<summary>Hint</summary>

Open a second (and third, and fourth) terminal. From each, run:

```bash
docker compose exec jupyter python -c "
import os, json
from kafka import KafkaConsumer
c = KafkaConsumer('clicks',
    bootstrap_servers=os.environ['KAFKA_BOOTSTRAP'],
    group_id='cg-rebalance',
    value_deserializer=lambda b: json.loads(b.decode()))
c.poll(timeout_ms=3000)
print(f'Assigned: {sorted(p.partition for p in c.assignment())}', flush=True)
for msg in c:
    print(f'  P{msg.partition}', flush=True)
"
```

Watch the `Assigned: [...]` output in each terminal as you start, stop, and restart consumers. Use `Ctrl-C` to stop a consumer.

</details>

<details>
<summary>Answer</summary>

You should observe:

- **One consumer**: `Assigned: [0, 1, 2]`;  owns all three partitions.
- **Two consumers (same group)**: rebalance, e.g. `Assigned: [0, 1]` and `Assigned: [2]`. Kafka split the work.
- **Three consumers**: each gets exactly one partition, `[0]`, `[1]`, `[2]`.
- **Kill one of three**: rebalance again;  surviving consumers take the dropped partition.
- **Start a fourth**: it joins the group but stays *idle* (`Assigned: []`). With three partitions, only three consumers can be active at any time.

**Partitions are the unit of parallelism.** To scale beyond 3 consumers, you need to have created the topic with more partitions in the first place. You can add partitions later, but only new keyed messages get the new layout;  existing keyed messages stay in their original partition.

</details>

---

### Exercise 5: Offsets and Replay (5 min)

**Q5.** Show that **resume vs replay** is decided by the consumer group, not by Kafka:

1. Run a consumer with `group_id='demo-resume'` and `auto_offset_reset='earliest'`. Read 5 messages and exit (the group commits offsets on close).
2. Run the *same* group again. Where does it pick up? Note: `auto_offset_reset` is now irrelevant;  the group already has committed offsets.
3. Run with a *different* `group_id` and `auto_offset_reset='earliest'`. What do you see?

<details>
<summary>Answer</summary>

```python
import time

def consume_n(group, n=5, reset="earliest"):
    c = KafkaConsumer(
        "clicks",
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset=reset,
        enable_auto_commit=True,        # commits periodically and on close
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=5000,
    )
    seen = []
    for msg in c:
        seen.append((msg.partition, msg.offset))
        if len(seen) >= n:
            break
    c.close()
    return seen

print("First run  (resume group):", consume_n("demo-resume"))
print("Second run (resume group):", consume_n("demo-resume"))
print("Fresh group:               ", consume_n(f"demo-fresh-{int(time.time())}"))
```

- The **first run** of `demo-resume` reads from the start of the topic.
- The **second run** of the *same* group resumes at the offset just past where the first run stopped;  it sees newer messages, not the same ones.
- The **fresh group** with `earliest` replays from the very beginning of the topic, regardless of what `demo-resume` has read.

**Kafka stores the data forever (until retention). The consumer group stores the position.** This is the foundation of "rerun a pipeline against historical data": you don't recover the data from somewhere else, you just create a new consumer group.

</details>

---

## Part 2: Spark Structured Streaming (40 min)

Keep `generate_clicks.py` running in its terminal so the streaming queries below have something to consume.

Create a **new notebook** (the SparkSession is heavy enough that mixing it with the kafka-python consumer above would be awkward). Use this as your first cell; the SparkSession takes a few seconds to start:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, window, count

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9094")

spark = (SparkSession.builder
    .appName("KafkaStreaming")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
```

The Kafka connector jar is pre-baked into the Docker image; no Maven download at startup. The Spark UI appears at http://localhost:4040 now that the session is running.

---

### Exercise 6: First Streaming Read (10 min)

**Q6.** Read the `clicks` topic as a streaming DataFrame and **print the raw Kafka envelope** (key, value, partition, offset, timestamp) to the console for ~30 seconds. What columns does Spark expose?

<details>
<summary>Hint</summary>

`spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("subscribe", "clicks").load()`

Default `startingOffsets` for streaming queries is `latest`; only events written *after* the query starts will appear. Use `earliest` to backfill.

</details>

<details>
<summary>Answer</summary>

```python
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "clicks")
    .option("startingOffsets", "latest")
    .load())

raw.printSchema()
# key, value, topic, partition, offset, timestamp, timestampType

query = (raw.selectExpr(
            "CAST(key AS STRING)   AS key",
            "CAST(value AS STRING) AS value",
            "partition", "offset", "timestamp")
    .writeStream
    .format("console")
    .option("truncate", False)
    .start())

import time; time.sleep(30)
query.stop()
```

The schema is the **Kafka envelope**, not your event payload. `value` is `binary` and you have to cast and parse it yourself; Spark has no idea your events are JSON.

</details>

---

### Exercise 7: Parse JSON (5 min)

**Q7.** Parse the JSON `value` into a typed DataFrame with columns `user_id`, `page`, `ts`. Print the parsed rows to the console.

<details>
<summary>Answer</summary>

```python
schema = "user_id STRING, page STRING, ts TIMESTAMP, event_id STRING"

parsed = (raw.select(
            from_json(col("value").cast("string"), schema).alias("e"))
        .select("e.*"))

query = (parsed.writeStream
    .format("console")
    .option("truncate", False)
    .start())

import time; time.sleep(15)
query.stop()
```

`ts` arrives as a proper `TimestampType` because the producer emits ISO-8601 strings. Watch the console; you should see ~10 rows/sec landing in micro-batches.

</details>

---

### Exercise 8: Tumbling Window (10 min)

**Q8.** Compute a **tumbling-window count of clicks per page**, with 10-second windows. Run it first with `outputMode("update")`, observe what's emitted per batch, then switch to `outputMode("complete")` and contrast.

<details>
<summary>Hint</summary>

```python
windowed = (parsed
    .groupBy(window(col("ts"), "10 seconds"), col("page"))
    .agg(count("*").alias("clicks")))
```

</details>

<details>
<summary>Answer</summary>

```python
windowed = (parsed
    .groupBy(window(col("ts"), "10 seconds"), col("page"))
    .agg(count("*").alias("clicks")))

# update mode: only rows whose count changed since the last batch
q = (windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start())

import time; time.sleep(30)
q.stop()
```

You should see incremental updates per page per window. Now switch:

```python
# complete mode: the entire result table, re-emitted every batch
q = (windowed.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start())

import time; time.sleep(30)
q.stop()
```

`complete` reprints the whole result table every micro-batch (only viable when the result is small;  here, ~9 pages × a handful of windows). `update` prints only rows that changed since the previous batch;  the right choice when the result table is large.

You'll also notice **previous windows still appear** in updates: Spark is keeping their state in case more events arrive for them. Without a watermark to expire that state, it grows unboundedly with stream length;  that's the trade-off you take on by skipping watermarks here, and is its own topic.

</details>

---

### Exercise 9: Parquet Sink + Checkpoint (10 min)

**Q9.** Stream the parsed events directly to **Parquet** under `/app/data/output/clicks/`, using a checkpoint at `/app/data/checkpoint/clicks/`. Stop the query, restart it, and confirm Spark resumes from the right offset (no duplicate events in the output).

<details>
<summary>Hint</summary>

Append mode on a non-aggregated stream is straightforward: you don't need anything beyond a `checkpointLocation`. The producer puts a unique `event_id` on every event; use it to verify there are no duplicates after restart.

</details>

<details>
<summary>Answer</summary>

```python
q = (parsed.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path",               "/app/data/output/clicks/")
    .option("checkpointLocation", "/app/data/checkpoint/clicks/")
    .start())

import time; time.sleep(30)
q.stop()

# Inspect what we wrote
df = spark.read.parquet("/app/data/output/clicks/")
total    = df.count()
distinct = df.select("event_id").distinct().count()
print(f"First run;  total: {total}, distinct event_ids: {distinct}")
```

Now **restart the Spark query** (re-run the `q = (parsed.writeStream ... .start())` cell;  leave `generate_clicks.py` running in its terminal) and let it run another 30 seconds. Re-check:

```python
df = spark.read.parquet("/app/data/output/clicks/")
total    = df.count()
distinct = df.select("event_id").distinct().count()
print(f"After restart;  total: {total}, distinct event_ids: {distinct}")
```

You should see:

- `total` grew (new events from the second run were added)
- `total == distinct` (no duplicates;  events from the first run weren't reprocessed)

Spark's checkpoint stored the last-processed Kafka offset; on restart it resumed from that offset rather than re-reading the whole topic.

</details>

---

### Exercise 10 (bonus): Replay from the Beginning (5 min)

**Q10.** Show that **the data lives in Kafka, not in Spark**: delete the checkpoint and the previous Parquet output, set `startingOffsets="earliest"`, and watch Spark reprocess the entire history of the topic.

<details>
<summary>Answer</summary>

```python
import shutil
shutil.rmtree("/app/data/checkpoint/clicks/", ignore_errors=True)
shutil.rmtree("/app/data/output/clicks/",     ignore_errors=True)

raw_replay = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "clicks")
    .option("startingOffsets", "earliest")    # the magic word
    .load())

parsed_replay = (raw_replay
    .select(from_json(col("value").cast("string"), schema).alias("e"))
    .select("e.*"))

q = (parsed_replay.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path",               "/app/data/output/clicks/")
    .option("checkpointLocation", "/app/data/checkpoint/clicks/")
    .start())

import time; time.sleep(30)
q.stop()

spark.read.parquet("/app/data/output/clicks/").count()
```

The count will be much higher than the runs in Exercise 9;  Spark just reprocessed every retained event in the topic from the very beginning. **Kafka is the source of truth; Spark's checkpoint is just a cursor.**

This is exactly how production systems handle reprocessing: keep raw events in Kafka with a long retention (e.g. 7 days, or forever for compacted topics). Any consumer that wants to rebuild its derived state just creates a new consumer group / new checkpoint and replays from `earliest`. No need to recover the data from somewhere else; it's still in Kafka.

</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| Kafka CLI | Creating and describing topics, partition leaders |
| Producers | `kafka-python` API, keyed writes, partition routing via `RecordMetadata` |
| Consumers | `auto_offset_reset='earliest'` vs `'latest'`, the missed-messages gotcha |
| Consumer groups | Rebalance across multiple consumers, partitions = max parallelism |
| Offsets | Resume (same group_id) vs replay (new group_id), the data/position split |
| Structured Streaming | Kafka source, raw envelope vs parsed JSON, micro-batch model |
| Windowing | Tumbling windows, `update` vs `complete` output modes |
| Checkpointing | Fault-tolerant restart, no duplicate events on resume (verified via `event_id`) |
| Replay | Throw away the checkpoint + `startingOffsets="earliest"` = full reprocess |

> **The core idea**: a Kafka topic is an unbounded, replayable log. Spark Structured Streaming turns it into an unbounded DataFrame. Checkpoints make a streaming query restartable; Kafka's retention is what makes it *reprocessable*;  drop the checkpoint, point at `earliest`, and rebuild.

---

## Cleanup

```bash
docker compose down
```

To also wipe the Kafka log and any Parquet output you wrote:

```bash
docker compose down -v
rm -rf data/output data/checkpoint
```

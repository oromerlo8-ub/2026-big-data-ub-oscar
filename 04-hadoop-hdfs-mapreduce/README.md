# Hadoop HDFS & MapReduce Practice: WordCount on Classic Literature

## Setup

From your local git repository:

```bash
cd 04-hadoop-hdfs-mapreduce
```

### Build and start the cluster

Review the `Dockerfile` and `compose.yaml` files.

```bash
docker compose up -d --build
```

This starts four services:
- **namenode**: manages HDFS metadata (UI at http://localhost:9870)
- **datanode**: stores actual data blocks
- **resourcemanager**: manages YARN cluster resources (UI at http://localhost:8088)
- **nodemanager**: executes MapReduce tasks

The NameNode needs ~20–30 seconds to format and start. Watch its logs:

```bash
docker logs -f namenode
```

Wait until you see `NameNode RPC up at: namenode/172.x.x.x:8020` before proceeding.

> **Note**: The block size is set to 1 MB in `config` (instead of the 128 MB default)
> so that our small text files will be split into multiple visible blocks.

---

## Exercises

All HDFS and MapReduce commands run inside the `namenode` container:

```bash
docker exec -it namenode bash
```

---

### Exercise 1: Explore the empty HDFS

**Q1.** List the root of the HDFS filesystem.

```bash
hdfs dfs -ls /
```

A freshly formatted HDFS is empty, no output is expected here.

**Q2.** Create a directory structure for our data:

```bash
hdfs dfs -mkdir -p /user/student/books
```

Verify it was created:

```bash
hdfs dfs -ls /user/student/
```

**Q3.** Check the cluster health report.


```bash
hdfs dfsadmin -report
```

How many DataNodes are connected? How much storage is available?

Note: `dfsadmin -report` shows storage stats, not configuration. To verify the
block size is set correctly:

```bash
hdfs getconf -confKey dfs.blocksize
```

You should see `1048576` (1 MB). The block size only becomes visible per-file
once you upload data and inspect it with `hdfs fsck`.

---

### Exercise 2: Load data into HDFS

**Q4.** The `data/` directory is mounted inside the container at `/data`. Check that
the sample file is there:

```bash
ls /data/
```

You should see `sample.txt`. Now download three classic books from Project Gutenberg
into `/data/`:

```bash
curl -o /data/moby-dick.txt       https://www.gutenberg.org/files/2701/2701-0.txt
curl -o /data/pride-prejudice.txt https://www.gutenberg.org/files/1342/1342-0.txt
curl -o /data/sherlock-holmes.txt https://www.gutenberg.org/files/1661/1661-0.txt
```

Check their sizes:

```bash
ls -lh /data/*.txt
```

**Q5.** Upload all the books to HDFS:

```bash
hdfs dfs -put /data/*.txt /user/student/books/
```

<details>
<summary><b>Hint, if you want to upload one at a time</b></summary>

```bash
hdfs dfs -put /data/sample.txt       /user/student/books/
hdfs dfs -put /data/moby-dick.txt    /user/student/books/
hdfs dfs -put /data/pride-prejudice.txt /user/student/books/
hdfs dfs -put /data/sherlock-holmes.txt /user/student/books/
```
</details>

**Q6.** List the files in HDFS. What sizes does HDFS report?

<details>
<summary><b>Hint</b></summary>

```bash
hdfs dfs -ls -h /user/student/books/
```
</details>

---

### Exercise 3: Block distribution

**Q7.** Inspect how Hadoop stored `moby-dick.txt` across blocks:

```bash
hdfs fsck /user/student/books/moby-dick.txt -files -blocks -locations
```

The output is verbose, focus on these lines:

```
/user/student/books/moby-dick.txt 1234609 bytes, replicated: replication=1, 2 block(s):  OK
0. blk_... len=1048576 Live_repl=1  [DatanodeInfoWithStorage[172.x.x.x:9866,...]]
1. blk_... len=186033  Live_repl=1  [DatanodeInfoWithStorage[172.x.x.x:9866,...]]
```

- Block 0 is exactly 1 MB (the configured block size)
- Block 1 holds the remainder
- In a real cluster each block would be on a **different** DataNode

**Q8.** Run fsck on the whole books directory and look at the summary:

```bash
hdfs fsck /user/student/books/ -files -blocks
```

<details>
<summary><b>What to look for</b></summary>

Look at the per-file block counts at the top, then the summary at the bottom:
- Only files **larger than 1 MB** get split into multiple blocks
- Files smaller than 1 MB use exactly 1 block regardless of their size
  (a 1.8 KB file and a 900 KB file each occupy one full block slot)
- `Total blocks (validated): 5`, 4 files but 5 blocks because `moby-dick.txt` is split
- Replication factor: 1 (production default is 3)
- Status: HEALTHY

</details>

**Q9.** Open the NameNode web UI at http://localhost:9870. Navigate to
**Utilities → Browse the file system** and find your books. Click on a file to see
its block IDs and their locations.

**Q10.** With the default block size of 128 MB, how many blocks would `moby-dick.txt`
use? How does the 1 MB block size change the number of MapReduce mappers?

---

### Exercise 4: WordCount with Unix pipes (baseline)

Before running MapReduce, let's do the same thing with basic Unix tools.
This is the "single machine" baseline.

**Q11.** Count words in `sample.txt` using Unix pipes:

```bash
cat /data/sample.txt | tr '[:upper:]' '[:lower:]' | tr -cs '[:alpha:]' '\n' | sort | uniq -c | sort -rn | head -20
```

What are the top 10 most frequent words?

**Q12.** Now run the same operation using our `mapper.py` and `reducer.py` scripts
directly with pipes (no Hadoop):

```bash
cat /data/sample.txt | python /mapreduce/mapper.py | sort | python /mapreduce/reducer.py | sort -t$'\t' -k2 -rn | head -20
```

<details>
<summary><b>What's happening here?</b></summary>

This simulates what MapReduce does, but on a single machine:
1. `mapper.py`: reads lines from stdin, emits `word\t1` for each word
2. `sort`: simulates the **shuffle & sort** phase (groups by key)
3. `reducer.py`: sums counts per word (uses `groupby`, which requires sorted input)

</details>

**Q13.** Time how long it takes on all the books combined:

```bash
time cat /data/*.txt | python /mapreduce/mapper.py | sort | python /mapreduce/reducer.py > /dev/null
```

Note the elapsed time. You'll compare this to MapReduce in Exercise 5.

---

### Exercise 5: WordCount with Hadoop MapReduce Streaming

Now run the same WordCount as a distributed MapReduce job using the
[Hadoop Streaming API](https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html).
Streaming lets you write mappers and reducers in any language that reads stdin and
writes to stdout.

**Q14.** Run the MapReduce job:

```bash
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  /user/student/books/ \
    -output /user/student/wordcount \
    -mapper  "python /mapreduce/mapper.py" \
    -reducer "python /mapreduce/reducer.py"
```

Watch the output. You'll see:
- Job submitted to YARN
- Map progress and Reduce progress percentages
- Final counters

**Q15.** While the job is running (or after it completes), open the YARN UI at
http://localhost:8088. Click on the running/completed application. What information
can you see about the job?

**Q16.** Check that the output was written to HDFS:

```bash
hdfs dfs -ls /user/student/wordcount/
```

You'll see a `_SUCCESS` file (empty, signals job completion) and one or more
`part-XXXXX` files containing the results.

**Q17.** View the top 20 words by frequency:

```bash
hdfs dfs -cat /user/student/wordcount/part-* | sort -t$'\t' -k2 -rn | head -20
```

<details>
<summary><b>Expected top words</b></summary>

With Moby Dick, Pride and Prejudice, and Sherlock Holmes combined, you'll see
common English words dominating: `the`, `of`, `and`, `to`, `a`, `in`...

Try filtering them out:
```bash
hdfs dfs -cat /user/student/wordcount/part-* | \
    grep -vE $'^(the|of|and|to|a|in|it|that|was|he|his|i|with|had|her|she|is|for|at|as|not|but)\t' | \
    sort -t$'\t' -k2 -rn | head -20
```
</details>

**Q18.** Look at the job counters in the terminal output. Find:
- How many Map input records were there?
- How many bytes were read from HDFS?
- How many Map output records were emitted?
- How many Reduce input groups (unique words)?

---

### Exercise 6: The shuffle phase

**Q19.** Run the job again on just `sample.txt` so it finishes quickly, and observe
the log output carefully:

```bash
hdfs dfs -rm -r /user/student/wordcount

hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  /user/student/books/sample.txt \
    -output /user/student/wordcount \
    -mapper  "python /mapreduce/mapper.py" \
    -reducer "python /mapreduce/reducer.py"
```

How many mappers ran? How many reducers?

**Q20.** The shuffle is the most expensive phase in MapReduce. Look at the counters
and compare local disk I/O to actual output:

```
FILE: Number of bytes written = 942,070   ← local disk (shuffle intermediate)
HDFS: Number of bytes written =   1,422   ← actual output
Spilled Records = 586                     ← 2 × 293 map output records
```

Nearly **1 MB written to local disk** to produce **1.4 KB of output**. Every map
output record hits disk twice: once at map output, once during shuffle. This is
why MapReduce is slow for iterative algorithms, each iteration repeats this cost.

**Q21.** How does the number of mappers relate to the number of blocks? Why?

<details>
<summary><b>Answer</b></summary>

Hadoop assigns **one mapper per input split**. By default one split = one block, but
Hadoop's TextInputFormat can split within a block at line boundaries. You may see
more splits than blocks for very small files.

For `sample.txt` (1803 bytes, 1 block): Hadoop still created **2 splits**, it split
the file at the midpoint line boundary, running 2 mappers on ~15 lines each.

For `moby-dick.txt` (1.2 MB, 2 blocks): 2 blocks → 2 splits → 2 mappers.

This is the essence of **data locality**: bring computation to the data, not data
to computation. Each mapper runs on the node that holds that block.

</details>

**Q22.** Compare the MapReduce execution time to your Unix pipes baseline from Q13.
MapReduce is slower on our tiny dataset, why? When would MapReduce actually be faster?

<details>
<summary><b>Answer</b></summary>

On `sample.txt` (30 lines), MapReduce took ~25 seconds. Unix pipes on all 4 books
combined took ~1 second. The overhead is constant regardless of data size:
- Launch YARN Application Master (~7s)
- Allocate containers for each mapper/reducer
- Localize job files to each container
- Write intermediate data to local disk (shuffle)
- Clean up containers

MapReduce starts winning when:
- Data is **too large** to fit on one machine
- Data is already **distributed** across many nodes
- The job can run **truly in parallel** across hundreds of machines

The crossover point is roughly when data exceeds what one machine can process in the
startup time, typically tens of GB on modern hardware.

</details>

---

### Summary

| Approach | Handles large data? | Fault tolerant? | Overhead |
|----------|---------------------|-----------------|---------- |
| Unix pipes | No (single machine) | No | Minimal |
| Hadoop MapReduce | Yes (distributed) | Yes (retry failed tasks) | High |
| Apache Spark (next session) | Yes | Yes | Medium |

Key takeaways:
1. HDFS splits files into blocks and distributes them, even on a single-node cluster
2. `hdfs fsck` lets you inspect block placement and replication
3. Hadoop Streaming runs any stdin/stdout program as a mapper or reducer
4. The shuffle (sort + transfer) is the expensive part of MapReduce
5. MapReduce overhead makes it slow for small data, it shines at petabyte scale

---

### Cleanup

```bash
docker compose down
```

To also remove the stored HDFS data:

```bash
docker compose down -v
```

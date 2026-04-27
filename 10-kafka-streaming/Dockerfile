FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    "kafka-python>=2.0.5" \
    jupyter \
    pandas \
    pyarrow

# Pre-cache the Spark Kafka connector so the first streaming notebook does not
# stall downloading ~50 MB of jars from Maven Central in front of the class.
RUN python -c "from pyspark.sql import SparkSession; \
    SparkSession.builder \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
        .getOrCreate().stop()"

COPY generate_clicks.py .

EXPOSE 4040 8888

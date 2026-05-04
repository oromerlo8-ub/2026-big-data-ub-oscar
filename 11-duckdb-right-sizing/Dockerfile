FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb \
    polars \
    pyspark==3.5.3 \
    pyarrow \
    pandas \
    jupyter

EXPOSE 4040 8888

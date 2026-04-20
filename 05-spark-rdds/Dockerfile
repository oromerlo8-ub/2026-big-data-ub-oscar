FROM python:3.14-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark \
    jupyter \
    pandas

EXPOSE 4040 8888

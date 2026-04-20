FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

ARG AIRFLOW_VERSION=3.2.0
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-apache-spark" \
    --constraint "${CONSTRAINT_URL}"

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    jupyter \
    pandas \
    pyarrow

COPY generate_data.py .

EXPOSE 4040 8080 8888

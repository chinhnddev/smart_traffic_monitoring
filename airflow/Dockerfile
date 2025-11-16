FROM apache/airflow:2.7.0

USER root
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir --user kafka-python psycopg2-binary requests

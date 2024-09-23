FROM apache/airflow:2.10.1

USER root
RUN pip install pyspark
USER airflow

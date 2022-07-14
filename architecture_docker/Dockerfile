FROM apache/airflow:2.3.0 AS airflow

USER root

RUN apt-get update

USER airflow
ENV PYTHONPATH "./venv/:/opt/airflow/"
COPY requirements.txt .

RUN pip install --user -r requirements.txt

COPY . .
FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y unzip

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

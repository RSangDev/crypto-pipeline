from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

RAW_PATH = "/tmp/cripto_hour"

def load_raw_to_postgres():

    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    for file in os.listdir(RAW_PATH):
        if file.endswith(".csv"):
            df = pd.read_csv(f"{RAW_PATH}/{file}")

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO raw_crypto (datetime, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['datetime'], row['open'], row['high'],
                    row['low'], row['close'], row['volume']
                ))

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    "crypto_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    download_zip = BashOperator(
        task_id="download_dataset",
        bash_command=(
            "curl -L -o /tmp/cripto-hour.zip "
            "https://www.kaggle.com/api/v1/datasets/download/nandodmelo/cripto-hour"
        )
    )

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command="unzip -o /tmp/cripto-hour.zip -d /tmp/cripto_hour/"
    )

    load_raw = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_to_postgres
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command="spark-submit /opt/spark/jobs/transform_crypto.py"
    )

    download_zip >> extract_zip >> load_raw >> spark_transform

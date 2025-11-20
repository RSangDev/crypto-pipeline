from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

RAW_PATH = "/opt/airflow/tmp/cripto_hour"
ZIP_PATH = "/opt/airflow/tmp/cripto-hour.zip"

def prepare_database():
    """Cria a base e a tabela caso não existam."""
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS crypto;")
    cursor.close()
    conn.close()

    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto (
            datetime TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()


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
    "crypto_pipeline_docker",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    prepare_db = PythonOperator(
        task_id="prepare_database",
        python_callable=prepare_database
    )

    download_zip = BashOperator(
        task_id="download_dataset",
        bash_command=f"mkdir -p /opt/airflow/tmp && curl -L -o {ZIP_PATH} https://www.kaggle.com/api/v1/datasets/download/nandodmelo/cripto-hour"
    )

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f"unzip -o {ZIP_PATH} -d {RAW_PATH}"
    )

    load_raw = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_to_postgres
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command="spark-submit /opt/spark/jobs/transform_crypto.py"
    )

    prepare_db >> download_zip >> extract_zip >> load_raw >> spark_transform

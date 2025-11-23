from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

RAW_DIR = "/opt/airflow/tmp"
EXPECTED_COLS = [
    "asset", "open", "close", "low", "high", "volume",
    "sma7", "sma25", "sma99", "bb_bbm", "bb_bbh", "bb_bbl",
    "psar", "rsi"
]


def create_table():
    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS raw_crypto;")  # 👈 Adicionado


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto (
            asset TEXT,
            timestamp TEXT,
            open TEXT,
            close TEXT,
            low TEXT,
            high TEXT,
            volume TEXT,
            sma7 TEXT,
            sma25 TEXT,
            sma99 TEXT,
            bb_bbm TEXT,
            bb_bbh TEXT,
            bb_bbl TEXT,
            psar TEXT,
            rsi TEXT
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()


def to_float(val):
    """ Converte valores numéricos ou retorna None """
    try:
        return float(val)
    except:
        return None


def load_raw_to_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    for filename in os.listdir(RAW_DIR):
        if not filename.endswith(".csv"):
            continue

        print("Carregando arquivo:", filename)
        df = pd.read_csv(f"{RAW_DIR}/{filename}")

        # Remove coluna inútil do Kaggle
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        # Nome do ativo
        asset = filename.replace(".csv", "")
        df["asset"] = asset

        # snake_case
        df.columns = [c.lower() for c in df.columns]

        # Força colunas esperadas; se faltar, vira string vazia
        for col in EXPECTED_COLS:
            if col not in df.columns:
                df[col] = ""

        # Converte TODO mundo para string (mantendo RAW)
        df = df.astype(str)

        # Cria timestamp incremental como string
        df["timestamp"] = df.index.astype(str)

        # Ordena colunas
        df = df[[
            "asset", "timestamp", "open", "close", "low", "high", "volume",
            "sma7", "sma25", "sma99", "bb_bbm", "bb_bbh", "bb_bbl", "psar", "rsi"
        ]]

        # Insere linha a linha
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO raw_crypto (
                    asset, timestamp, open, close, low, high, volume,
                    sma7, sma25, sma99, bb_bbm, bb_bbh, bb_bbl, psar, rsi
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()



with DAG(
    "crypto_pipeline_docker",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    make_tmp_dir = BashOperator(
        task_id="make_tmp_dir",
        bash_command="mkdir -p /opt/airflow/tmp"
    )

    download_zip = BashOperator(
        task_id="download_dataset",
        bash_command="kaggle datasets download -d nandodmelo/cripto-hour -p /opt/airflow/tmp --unzip"
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw_to_postgres
    )

    transform_crypto = SparkSubmitOperator(
        task_id='transform_crypto',
        application='/opt/airflow/spark/jobs/transform_crypto.py',
        name='crypto_transform',
        verbose=True,
        conf={"spark.master": "local[*]"},  # 👈 aqui definimos o modo local
        dag=dag,
    )





    make_tmp_dir >> download_zip >> create_table_task >> load_raw >> transform_crypto

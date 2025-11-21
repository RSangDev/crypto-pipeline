from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

RAW_DIR = "/opt/airflow/tmp"

def create_table():
    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto (
            asset TEXT,
            timestamp INT,
            open FLOAT,
            close FLOAT,
            low FLOAT,
            high FLOAT,
            volume FLOAT,
            sma7 FLOAT,
            sma25 FLOAT,
            sma99 FLOAT,
            bb_bbm FLOAT,
            bb_bbh FLOAT,
            bb_bbl FLOAT,
            psar FLOAT,
            rsi FLOAT
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

    for filename in os.listdir(RAW_DIR):
        if not filename.endswith(".csv"):
            continue

        print("Carregando arquivo:", filename)

        df = pd.read_csv(f"{RAW_DIR}/{filename}")

        # Remover coluna inútil
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
        
        # Adicionar o nome do ativo baseado no nome do arquivo
        asset = filename.replace(".csv", "")

        df["asset"] = asset

        # Renomear para snake_case
        df.columns = [c.lower() for c in df.columns]

        # Forçar presença das colunas esperadas
        expected_cols = [
            "asset", "open", "close", "low", "high", "volume",
            "sma7", "sma25", "sma99", "bb_bbm", "bb_bbh", "bb_bbl",
            "psar", "rsi"
        ]

        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        # Criar timestamp incremental
        df["timestamp"] = range(len(df))

        # Reordena para o INSERT
        df = df[[
            "asset", "timestamp", "open", "close", "low", "high", "volume",
            "sma7", "sma25", "sma99", "bb_bbm", "bb_bbh", "bb_bbl", "psar", "rsi"
        ]]

        # Inserir linha por linha
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

    make_tmp_dir >> download_zip >> create_table_task >> load_raw

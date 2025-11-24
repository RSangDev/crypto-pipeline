from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import pandas as pd
import psycopg2

# ----------------------- CONFIGURAÇÕES -----------------------
RAW_DIR = "/opt/airflow/tmp"
EXPECTED_COLS = [
    "asset", "open", "close", "low", "high", "volume",
    "sma7", "sma25", "sma99", "bb_bbm", "bb_bbh", "bb_bbl",
    "psar", "rsi"
]

# ----------------------- FUNÇÕES PYTHON -----------------------
def create_table():
    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS raw_crypto;")
    cur.execute("""
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
    cur.close()
    conn.close()


def load_raw_to_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="crypto",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO raw_crypto (
            asset, timestamp, open, close, low, high, volume,
            sma7, sma25, sma99, bb_bbm, bb_bbh, bb_bbl, psar, rsi
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    for filename in os.listdir(RAW_DIR):
        if not filename.endswith(".csv"):
            continue

        filepath = os.path.join(RAW_DIR, filename)
        print(f"Carregando arquivo: {filename}")

        df = pd.read_csv(filepath)

        # Remove coluna inútil do Kaggle
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        asset = filename.replace(".csv", "").upper()
        df["asset"] = asset
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Garante exatamente as colunas esperadas (na ordem correta!)
        df = df.reindex(columns=[
            "open", "close", "high", "low", "volume",
            "sma7", "sma25", "sma99",
            "bb_bbm", "bb_bbh", "bb_bbl",
            "psar", "rsi"
        ], fill_value="")

        # Adiciona asset e timestamp
        df.insert(0, "asset", asset)
        df.insert(1, "timestamp", df.index.astype(str))

        # Converte tudo pra string (RAW)
        df = df.astype(str)

        # Insere em lote (muito mais rápido e seguro)
        records = df.values.tolist()
        cur.executemany(insert_sql, records)

    conn.commit()
    cur.close()
    conn.close()
    print("Todos os arquivos carregados com sucesso no PostgreSQL!")
# ----------------------- DAG -----------------------
with DAG(
    dag_id="crypto_pipeline_docker",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["crypto", "spark", "kaggle"],
) as dag:

    make_tmp_dir = BashOperator(
        task_id="make_tmp_dir",
        bash_command="""
            mkdir -p /opt/airflow/tmp
            # Não precisamos mais do chmod 777 → o volume já está montado com permissão correta
        """.strip(),
    )

    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command="kaggle datasets download -d nandodmelo/cripto-hour -p /opt/airflow/tmp --unzip",
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw_to_postgres,
    )

        # TASK SPARK – VERSÃO CORRETA PARA AIRFLOW 2.9+ (2025)
    transform_crypto = SparkSubmitOperator(
        task_id="transform_crypto",
        conn_id="spark_local",  # <- Isso força local[*] sem default yarn
        application="/opt/airflow/spark/jobs/transform_crypto.py",
        name="Crypto-Transform-Local",
        verbose=True,
        env_vars={
            "PYSPARK_PYTHON": "/usr/local/bin/python3",  # Garante Python correto
            "SPARK_HOME": "/opt/spark"  # Se precisar, ajuste pro seu path
        },
    )

    # ----------------------- DEPENDÊNCIAS -----------------------
    make_tmp_dir >> download_dataset >> create_table_task >> load_raw >> transform_crypto
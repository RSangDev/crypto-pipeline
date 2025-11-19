from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2

spark = SparkSession.builder.appName("TransformCrypto").getOrCreate()

# 1. Lê os dados diretamente do Postgres (RAW)
df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres:5432/crypto",
    driver="org.postgresql.Driver",
    dbtable="raw_crypto",
    user="airflow",
    password="airflow"
).load()

df_clean = df \
    .withColumn("date", col("datetime")) \
    .withColumn("price_usd", col("close")) \
    .select("date", "price_usd")

# Converte para coleção de linhas para inserção no Postgres
rows = df_clean.collect()

conn = psycopg2.connect(
    host="postgres",
    database="crypto",
    user="airflow",
    password="airflow"
)
cursor = conn.cursor()

cursor.execute("DELETE FROM curated_crypto")

for r in rows:
    cursor.execute("""
        INSERT INTO curated_crypto (date, price_usd)
        VALUES (%s, %s)
    """, (r['date'], r['price_usd']))

conn.commit()
cursor.close()
conn.close()

print("Transformação Spark concluída!")

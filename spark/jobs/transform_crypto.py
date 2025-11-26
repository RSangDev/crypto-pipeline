from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

spark = SparkSession.builder.appName("TransformCrypto").getOrCreate()

df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres:5432/crypto",
    driver="org.postgresql.Driver",
    dbtable="raw_crypto",
    user="airflow",
    password="airflow"
).load()
spark.sql("DROP TABLE IF EXISTS curated_crypto")


decimal_cols = ["close", "volume", "sma7", "sma25", "sma99",
                "bb_bbm", "bb_bbh", "bb_bbl", "psar", "rsi"]

# Seleciona colunas
df_curated = df.select(
    col("timestamp").alias("date"),
    col("asset"),
    col("close").alias("price_usd"),
    col("volume"),
    "sma7", "sma25", "sma99",
    "bb_bbm", "bb_bbh", "bb_bbl",
    "psar", "rsi"
)

# Formata para duas casas decimais
for c in decimal_cols:
    if c in df_curated.columns:  # garante que a coluna existe
        df_curated = df_curated.withColumn(c, round(col(c), 2))



df_curated.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto") \
    .option("dbtable", "curated_crypto") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Curated finalizada com sucesso!")

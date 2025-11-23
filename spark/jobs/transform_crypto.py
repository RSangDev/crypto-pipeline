from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Cria SparkSession
spark = SparkSession.builder.appName("TransformCrypto").getOrCreate()

# Lê dados RAW diretamente do Postgres
df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres:5432/crypto",
    driver="org.postgresql.Driver",
    dbtable="raw_crypto",
    user="airflow",
    password="airflow"
).load()

# Adiciona coluna de data (processamento)
df_clean = df \
    .withColumn("date", current_timestamp()) \
    .withColumn("price_usd", col("close")) \
    .select("date", "price_usd")  # Apenas o que queremos no curated

# Escreve direto no Postgres na tabela curated_crypto
df_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto") \
    .option("dbtable", "curated_crypto") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Transformação Spark concluída!")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# Seleciona e renomeia colunas
df_clean = df \
    .withColumn("date", col("datetime")) \
    .withColumn("price_usd", col("close")) \
    .select("date", "price_usd")

# Escreve direto no Postgres na tabela curated_crypto
df_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "curated_crypto") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .mode("overwrite").save() # sobrescreve a tabela
    

print("Transformação Spark concluída!")

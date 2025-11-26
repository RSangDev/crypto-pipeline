FROM apache/airflow:2.10.2

# ---------------------------------------------------
# ROOT - Instala dependências do sistema e Spark
# ---------------------------------------------------
USER root

# Instala Java + curl + unzip
RUN apt-get update && apt-get install -y \
    unzip \
    openjdk-17-jdk \
    curl \
 && apt-get clean

# Instala Spark 3.5.1
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Instala driver JDBC do PostgreSQL dentro do Spark
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
     -o /opt/spark/jars/postgresql.jar

# Ajusta permissões do Spark para o usuário airflow
RUN chown -R airflow:root /opt/spark

# ---------------------------------------------------
# ENV - Variáveis de ambiente (Java + Spark)
# ---------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

# ---------------------------------------------------
# AIRFLOW - Instala bibliotecas Python
# ---------------------------------------------------
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ---------------------------------------------------
# KAGGLE
# ---------------------------------------------------
USER root
RUN mkdir -p /home/airflow/.kaggle
COPY .kaggle/kaggle.json /home/airflow/.kaggle/kaggle.json
RUN chown airflow:root /home/airflow/.kaggle/kaggle.json && \
    chmod 600 /home/airflow/.kaggle/kaggle.json

# Volta para usuário airflow
USER airflow

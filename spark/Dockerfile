FROM apache/spark:3.5.1

# Copia o job PySpark
COPY jobs /opt/spark/jobs

# Instalar JDBC driver do Postgres
USER root
RUN apt-get update && apt-get install -y curl \
    && curl -L -o /opt/spark/jars/postgresql.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER spark

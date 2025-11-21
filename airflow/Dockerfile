FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y unzip

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Cria diretório kaggle
USER root
RUN mkdir -p /home/airflow/.kaggle

# Copia o JSON
COPY .kaggle/kaggle.json /home/airflow/.kaggle/kaggle.json

# Ajusta permissões (apenas dono)
RUN chown airflow: /home/airflow/.kaggle/kaggle.json && chmod 600 /home/airflow/.kaggle/kaggle.json

USER airflow

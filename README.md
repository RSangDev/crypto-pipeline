# Crypto Data Pipeline (Airflow + PySpark + Docker + Postgres)

Pipeline completa de Engenharia de Dados usando:

- Airflow para orquestração
- PySpark para processamento distribuído
- Postgres como banco RAW e CURATED
- Docker Compose como infraestrutura
- Dataset cripto-hour (Kaggle)

## Fluxo do pipeline

1. Airflow baixa o dataset do Kaggle
2. Extrai o ZIP para /tmp
3. Carrega os CSVs para o Postgres (tabela RAW)
4. Spark processa e grava resultados na tabela CURATED

## Como rodar

```bash
docker compose up -d --build

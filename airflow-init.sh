#!/bin/bash

# aguarda o postgres estar acessível
echo "⏳ Waiting for Postgres..."
until pg_isready -h postgres -p 5432 -U airflow; do
  sleep 2
done

# cria o banco airflow se não existir
echo "🛢️ Checking database..."
psql -h postgres -U airflow -tc "SELECT 1 FROM pg_database WHERE datname = 'crypto'" | grep -q 1 \
  || psql -h postgres -U airflow -c "CREATE DATABASE crypto"

# migra o airflow
echo "🔧 Initializing Airflow DB..."
airflow db migrate

# cria usuário admin se não existir
echo "👤 Creating admin user..."
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || echo "User already exists"

# sobe o webserver
airflow webserver

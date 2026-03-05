# Crypto Data Pipeline (Airflow + PySpark + Docker + Postgres)

Este projeto implementa um pipeline completo de engenharia de dados para coletar, transformar e armazenar dados de criptomoedas utilizando ferramentas amplamente usadas no mercado.

- Airflow para orquestração de tarefas
- PySpark para processamento distribuído e transformação
- Postgres como banco (RAW e CURATED)
- Docker Compose como infraestrutura e ambiente
- Dataset cripto-hour (Kaggle)

## Arquitetura do pipeline
### Fluxo resumido


1. Airflow faz o download do dataset usando a API do Kaggle
2. O arquivo ZIP é extraído em /tmp
3. Os CSVs são carregados no PostgreSQL (tabela RAW)
4. Spark transforma os dados (limpeza, e validação)
5. Os dados transformados são gravados na tabela CURATED

Resultado final: tabela tratada e pronta para analytics.

## Como Executar o Projeto
1) Clonar o repositório
 ```bash

git clone https://github.com/SEU_USUARIO/crypto-pipeline.git


cd crypto-pipeline
```

3) Subir os serviços
  ```bash

docker compose up -d --build
4) Acessar o Airflow:
```bash
URL: http://localhost:8080
```

Usuário/Senha: airflow / airflow

4) Ativar e rodar a DAG:
```bash

DAG: crypto_pipeline_docker
```
5) Acessar a porta do Metabase em:
```bash

URL: http://localhost:3000
```
7) Fazer Login no metabase
  
Usuário: admin@example.com
Senha: admin123


9) Acessar Our analytics pelo menu lateral, ou usar a URL
  ```bash

http://localhost:3000/collection/root

Encontrar preco-criptomoeda 
```

OU
```bash

Em database > crypto> curatec_crypto
```

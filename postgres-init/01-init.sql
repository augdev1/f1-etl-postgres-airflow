-- Cria o banco f1db automaticamente na inicializacao do container
-- e concede permissao ao usuario airflow (ja criado pelo env POSTGRES_USER)

CREATE DATABASE f1db;
GRANT ALL PRIVILEGES ON DATABASE f1db TO airflow;

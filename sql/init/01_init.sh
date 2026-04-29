#!/bin/bash
set -e

# Create airflow and clinical_trials databases if they don't exist.
# This script runs once on first Postgres container startup.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow'
      WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

    SELECT 'CREATE DATABASE clinical_trials'
      WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'clinical_trials')\gexec
EOSQL

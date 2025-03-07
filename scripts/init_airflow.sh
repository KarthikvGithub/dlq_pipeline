#!/bin/bash

# Load secrets from Docker secrets using proper handling
export POSTGRES_HOST="postgres"
export POSTGRES_PORT="5432"

# echo "Setting up permissions..."
# chmod 600 /run/secrets/*
# chown airflow:airflow /run/secrets/*

# Load secrets safely without word splitting
# if [ -f /run/secrets/postgres_config ]; then
#     source /run/secrets/postgres_config
#     export POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB
# fi

# if [ -f /run/secrets/aws_config ]; then
#     source /run/secrets/aws_config
#     export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION
# fi

# if [ -f /run/secrets/airflow_config ]; then
#     source /run/secrets/airflow_config
#     export AIRFLOW__CORE__FERNET_KEY AIRFLOW__CORE__SQL_ALCHEMY_CONN
# fi

export $(grep -v '^#' /run/secrets/postgres_config | xargs)
export $(grep -v '^#' /run/secrets/aws_config | xargs)
export $(grep -v '^#' /run/secrets/aws_config | xargs)


# Wait for PostgreSQL using service name
echo "Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"
do
    echo "PostgreSQL not ready. Retrying in 5 seconds..."
    sleep 5
done
echo "PostgreSQL connection established."

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db migrate || {
    echo "Failed to initialize Airflow database"
    exit 1
}

# Create admin user with non-interactive password
echo "Creating admin user..."
airflow users create \
    --username "$POSTGRES_USER" \
    --password "$POSTGRES_PASSWORD" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --use-random-password=false || {
    echo "Failed to create Airflow user"
    exit 1
}
# Set up connections using secrets
echo "Configuring connections..."
airflow connections add 'aws_default' \
  --conn-type 'aws' \
  --conn-login "$AWS_ACCESS_KEY_ID" \
  --conn-password "$AWS_SECRET_ACCESS_KEY" \
  --conn-extra "{\"region_name\":\"$AWS_REGION\"}" || {
  echo "Failed to create AWS connection"
}

airflow connections add 'postgres_default' \
  --conn-type 'postgres' \
  --conn-host "$POSTGRES_HOST" \
  --conn-port "$POSTGRES_PORT" \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-schema "$POSTGRES_DB" || {
  echo "Failed to create PostgreSQL connection"
}

airflow connections add 'kafka_default' \
  --conn-type 'kafka' \
  --conn-host 'kafka' \
  --conn-port '29092' || {
  echo "Failed to create Kafka connection"
}

echo "Airflow initialization completed successfully!"
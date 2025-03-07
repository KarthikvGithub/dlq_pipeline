#!/bin/bash

# Load database credentials securely
source /run/secrets/postgres_config
export PGHOST=postgres PGPORT=5432 PGPASSWORD="$POSTGRES_PASSWORD"

# Wait for PostgreSQL with credentials
echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD="$POSTGRES_PASSWORD" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
do
    echo "PostgreSQL not ready. Retrying in 5 seconds..."
    sleep 5
done

# Run migrations with explicit credentials
echo "Running database migrations..."
for migration in $(find /migrations -name "*.sql" | sort -V); do
    echo "Applying migration: ${migration}"
    psql -v ON_ERROR_STOP=1 \
         -U "$POSTGRES_USER" \
         -d "$POSTGRES_DB" \
         -h postgres \
         -p 5432 \
         -f "${migration}"
    if [ $? -ne 0 ]; then
        echo "Migration ${migration} failed!"
        exit 1
    fi
done
#!/bin/bash
set -e

MAX_RETRIES=30
RETRY_COUNT=0

echo "Waiting for PostgreSQL to be ready..."
until nc -z metastore-db 5432; do
  RETRY_COUNT=$((RETRY_COUNT+1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "ERROR: PostgreSQL did not become ready in time"
    exit 1
  fi
  echo "PostgreSQL is unavailable - sleeping (attempt $RETRY_COUNT/$MAX_RETRIES)"
  sleep 2
done

echo "PostgreSQL is up - checking Hive Metastore schema..."

sleep 5

SCHEMA_EXISTS=0
if PGPASSWORD=hive psql -h metastore-db -U hive -d metastore -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='VERSION';" 2>/dev/null | grep -q "1"; then
  SCHEMA_EXISTS=1
fi

if [ "$SCHEMA_EXISTS" = "0" ]; then
  echo "Initializing Hive Metastore schema..."
  if /opt/hive/bin/schematool -dbType postgres -initSchema; then
    echo "Schema initialized successfully!"
  else
    echo "ERROR: Failed to initialize schema"
    exit 1
  fi
else
  echo "Hive Metastore schema already exists, skipping initialization..."
fi

echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
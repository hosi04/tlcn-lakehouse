#!/bin/bash
set -e

ADMIN_USER=${ADMIN_USER:-admin}
ADMIN_PASSWORD=${ADMIN_PASSWORD:-admin}
ADMIN_EMAIL=${ADMIN_EMAIL:-admin@superset.com}

echo "Starting Superset initialization..."

echo "Waiting for PostgreSQL..."
MAX_RETRIES=30
RETRY_COUNT=0

until PGPASSWORD=password psql -h postgres -U superset -d superset -c '\q' 2>/dev/null; do
  RETRY_COUNT=$((RETRY_COUNT+1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "ERROR: PostgreSQL did not become ready in time"
    exit 1
  fi
  echo "PostgreSQL is unavailable - sleeping (attempt $RETRY_COUNT/$MAX_RETRIES)"
  sleep 2
done

echo "PostgreSQL is ready!"

echo "Running database migrations..."
superset db upgrade

TABLE_EXISTS=$(PGPASSWORD=password psql -h postgres -U superset -d superset -tAc \
  "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'ab_user');" 2>/dev/null || echo "f")

if [ "$TABLE_EXISTS" = "t" ]; then
    USER_COUNT=$(PGPASSWORD=password psql -h postgres -U superset -d superset -tAc \
      "SELECT COUNT(*) FROM ab_user;" 2>/dev/null || echo "0")
    
    if [ "$USER_COUNT" -gt "0" ]; then
        echo "Database already initialized (found $USER_COUNT users). Skipping admin creation."
    else
        echo "Table exists but no users found. Creating admin user..."
        superset fab create-admin \
            --username "$ADMIN_USER" \
            --firstname Superset \
            --lastname Admin \
            --email "$ADMIN_EMAIL" \
            --password "$ADMIN_PASSWORD"
        
        superset init
    fi
else
    echo "First time setup - creating admin user and initializing..."
    superset fab create-admin \
        --username "$ADMIN_USER" \
        --firstname Superset \
        --lastname Admin \
        --email "$ADMIN_EMAIL" \
        --password "$ADMIN_PASSWORD"
    
    superset init
fi

echo "Superset initialization finished!"

echo "Starting Gunicorn..."
exec gunicorn \
  --bind "0.0.0.0:8088" \
  --workers 1 \
  --worker-class gthread \
  --threads 20 \
  --timeout 60 \
  --limit-request-line 4094 \
  --limit-request-field_size 8190 \
  "superset.app:create_app()"
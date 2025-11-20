#!/bin/bash

set -e

ADMIN_USER=${ADMIN_USER:-admin}
ADMIN_PASSWORD=${ADMIN_PASSWORD:-admin}
ADMIN_EMAIL=${ADMIN_EMAIL:-admin@superset.com}

echo "Starting Superset initialization..."

superset db upgrade

if ! flask fab list-users | grep -q $ADMIN_USER; then
    echo "Creating admin user ${ADMIN_USER}..."
    superset fab create-admin \
        --username "$ADMIN_USER" \
        --firstname Superset \
        --lastname Admin \
        --email "$ADMIN_EMAIL" \
        --password "$ADMIN_PASSWORD"
else
    echo "Admin user ${ADMIN_USER} already exists."
fi

superset init

echo "Superset initialization finished."

echo "Starting Gunicorn..."
gunicorn \
  --bind "0.0.0.0:8088" \
  --workers 1 \
  --worker-class gthread \
  --threads 20 \
  --timeout 60 \
  --limit-request-line 4094 \
  --limit-request-field_size 8190 \
  "superset.app:create_app()"
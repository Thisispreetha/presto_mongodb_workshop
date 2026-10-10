#!/usr/bin/env bash
set -e

echo "=== Initializing Apache Superset ==="

# Wait for PostgreSQL
until python3 -c "import socket; s = socket.socket(); s.connect(('postgres', 5432))" >/dev/null 2>&1; do
  echo "Waiting for PostgreSQL on postgres:5432..."
  sleep 2
done

# Install Presto driver (PyHive) and PostgreSQL adapter
echo "Installing Presto and PostgreSQL drivers into Superset environment..."
pip install --target /app/.venv/lib/python3.10/site-packages --no-cache-dir "pyhive[presto]" psycopg2-binary

# Database upgrade
echo "Upgrading Superset metadata DB..."
superset db upgrade

# Create Admin User if not exists
echo "Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname Workshop \
  --lastname Admin \
  --email admin@gdeltlakehouse.local \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

# Initialize Superset roles and permissions
echo "Initializing roles..."
superset init

echo "=== Starting Superset Web Server ==="
/usr/bin/run-server.sh


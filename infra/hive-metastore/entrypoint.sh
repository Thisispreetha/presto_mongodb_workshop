#!/usr/bin/env bash
set -e

echo "=== Initializing Hive Metastore ==="

export HIVE_CONF_DIR=/opt/hive/conf

# Wait for PostgreSQL using pure bash socket check
until (echo > /dev/tcp/postgres/5432) 2>/dev/null; do
  echo "Waiting for PostgreSQL on postgres:5432..."
  sleep 2
done

# Initialize schema if not present
echo "Checking Hive Metastore schema in PostgreSQL..."
/opt/hive/bin/schematool -dbType postgres -info || {
  echo "Metastore schema not found. Initializing with schematool..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
}

echo "=== Starting Hive Metastore on port 9083 ==="
exec /opt/hive/bin/hive --service metastore

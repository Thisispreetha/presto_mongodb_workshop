#!/usr/bin/env bash
set -e

if ! command -v java &>/dev/null; then
  echo "=== Installing OpenJDK 17 Runtime & Tools in Airflow ==="
  apt-get update -qq && apt-get install -y -qq --no-install-recommends openjdk-17-jre-headless netcat-openbsd procps
  mkdir -p /usr/lib/jvm
  ln -sfn /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/default-java 2>/dev/null || true
fi

# Dynamically generate Airflow 3 simple auth credentials from .env variables
ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
ADMIN_PASS="${AIRFLOW_ADMIN_PASSWORD:-admin}"
echo "{\"${ADMIN_USER}\": \"${ADMIN_PASS}\"}" > /opt/airflow/simple_auth_manager_passwords.json

exec /entrypoint "$@"

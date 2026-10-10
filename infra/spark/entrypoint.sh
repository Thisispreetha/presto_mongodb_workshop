#!/usr/bin/env bash
set -e

if ! python3 -c "import pymongo" &>/dev/null; then
  echo "=== Installing Python dependencies in Spark ==="
  pip3 install --no-cache-dir pymongo requests
fi

# Auto-download AWS SDK bundle if missing (omitted from git due to GitHub 100MB limit)
JAR_PATH="/opt/spark-extra-jars/aws-java-sdk-bundle-1.12.262.jar"
if [ ! -s "$JAR_PATH" ]; then
  echo "=== Downloading aws-java-sdk-bundle-1.12.262.jar (~268MB) from Maven Central... ==="
  mkdir -p /opt/spark-extra-jars
  if curl -fSL --connect-timeout 30 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o "${JAR_PATH}.tmp"; then
    mv "${JAR_PATH}.tmp" "$JAR_PATH"
    echo "=== AWS SDK bundle downloaded successfully ==="
  else
    echo "WARNING: Failed to download AWS SDK bundle. S3A operations may fail if not present."
    rm -f "${JAR_PATH}.tmp"
  fi
fi

SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master}"

echo "=== Starting Spark Master on port 7077 (Web UI: 8082, Host: ${SPARK_MASTER_HOST}) ==="
/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host "${SPARK_MASTER_HOST}" --port 7077 --webui-port 8082 &
MASTER_PID=$!

# Wait for Spark Master to open port 7077
echo "Waiting for Spark Master RPC to bind on port 7077..."
until (echo > /dev/tcp/${SPARK_MASTER_HOST}/7077) 2>/dev/null || (echo > /dev/tcp/127.0.0.1/7077) 2>/dev/null; do
  sleep 1
done
echo "Spark Master is ready on ${SPARK_MASTER_HOST}:7077."

echo "=== Starting Spark Worker connected to spark://${SPARK_MASTER_HOST}:7077 (Web UI: 8084) ==="
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  spark://${SPARK_MASTER_HOST}:7077 --webui-port 8084 --cores ${SPARK_WORKER_CORES:-4} &
WORKER_PID=$!

trap "kill -TERM $MASTER_PID $WORKER_PID 2>/dev/null" TERM INT

wait -n $MASTER_PID $WORKER_PID

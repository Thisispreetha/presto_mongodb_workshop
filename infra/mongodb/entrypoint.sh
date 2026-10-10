#!/usr/bin/env bash
set -e

# Run background auto-initializer for replica set rs0 and schema
(
  echo "=== [Auto-Init] Waiting for MongoDB on localhost:27017 ==="
  until mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
    sleep 2
  done

  # Initialize replica set rs0 if not already initialized
  mongosh --quiet <<EOF > /dev/null 2>&1
  try {
    var status = rs.status();
  } catch (e) {
    rs.initiate({
      _id: "rs0",
      members: [{ _id: 0, host: "mongodb:27017" }]
    });
  }
EOF

  # Wait until node is PRIMARY
  mongosh --quiet --eval 'while (!rs.isMaster().ismaster) { sleep(1000); }' > /dev/null 2>&1

  # Seed collections and Presto schema if not already present
  if [ -f /docker-entrypoint-initdb.d/init-schema.js ]; then
    mongosh --quiet "mongodb://127.0.0.1:27017/market_data" /docker-entrypoint-initdb.d/init-schema.js > /dev/null 2>&1 || true
  fi

  echo "=== [Auto-Init] MongoDB Replica Set rs0 and Schema Ready ==="
) &

# Run mongod in foreground as PID 1
exec docker-entrypoint.sh mongod --replSet rs0 --bind_ip_all

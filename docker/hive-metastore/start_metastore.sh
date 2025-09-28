#!/bin/bash

set -euo pipefail

export PGPASSWORD=hive
HIVE_DB_NAME="metastore"
HIVE_DB_USER="hive"
HIVE_DB_HOST="hive-metastore-db"

echo "⏳ Waiting for PostgreSQL to be ready..."
until nc -z -v -w30 "$HIVE_DB_HOST" 5432; do
  echo "⏳ PostgreSQL not ready yet..."
  sleep 1
done

echo "✅ PostgreSQL is ready."

# 🧹 Drop và recreate lại database
echo "🔄 Dropping and recreating the metastore database..."
psql -h "$HIVE_DB_HOST" -U "$HIVE_DB_USER" -d postgres -c "DROP DATABASE IF EXISTS $HIVE_DB_NAME;"
psql -h "$HIVE_DB_HOST" -U "$HIVE_DB_USER" -d postgres -c "CREATE DATABASE $HIVE_DB_NAME OWNER $HIVE_DB_USER;"

# 🛠 Init schema
echo "⚙️ Initializing Hive Metastore schema..."
/opt/hive/bin/schematool -dbType postgres -initSchema --verbose

# 🚀 Start metastore
echo "🚀 Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore -p 9083

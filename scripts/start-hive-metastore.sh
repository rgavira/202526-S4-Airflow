#!/bin/bash

set -e

echo "==============================================="
echo "Starting Hive Metastore"
echo "==============================================="

# Wait for MySQL
echo "Waiting for MySQL to be ready..."
until nc -z hive-metastore-db 3306; do
    echo "MySQL is not ready yet..."
    sleep 2
done

echo "MySQL is ready!"

# Initialize schema if needed
echo "Initializing Hive Metastore schema..."
/opt/hive/bin/schematool -dbType mysql -initSchema 2>/dev/null || echo "Schema already exists"

# Start metastore
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore

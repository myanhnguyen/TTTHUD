#!/bin/bash

# Start PostgreSQL service
service postgresql start

# Create the database
su - postgres -c "psql -c \"CREATE DATABASE sparkdb;\""

# Create the user and grant privileges
su - postgres -c "psql -c \"CREATE USER sparkuser WITH PASSWORD 'sparkpassword';\""
su - postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE sparkdb TO sparkuser;\""

# Create the table and import data
su - postgres -c "psql -d sparkdb -c \"CREATE TABLE sample_data (region VARCHAR(255), country VARCHAR(255), item VARCHAR(255));\""
su - postgres -c "psql -d sparkdb -c \"COPY sample_data FROM '/data/data.csv' DELIMITER ',' CSV HEADER;\""

# Grant ownership of the table to sparkuser
su - postgres -c "psql -d sparkdb -c \"ALTER TABLE sample_data OWNER TO sparkuser;\""

# Start Spark master and worker
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077

# Keep the container running
tail -f /dev/null

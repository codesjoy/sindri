#!/bin/sh
set -eu

# The bootstrap superuser exists only for initial database/role provisioning.
# Every workload receives a distinct login and can CONNECT only to its own DB.
for owner in sequence; do
  database="skuld_${owner}"
  role="$database"
  password="${SKULD_SEQUENCE_DB_PASSWORD:-skuld-local}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v role="$role" -v database="$database" -v password="$password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role', :'password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role')\gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'database', :'role')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'database')\gexec
SQL
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v database="$database" <<'SQL'
REVOKE CONNECT ON DATABASE postgres FROM PUBLIC;
REVOKE CONNECT ON DATABASE :"database" FROM PUBLIC;
SQL
done

for owner in sequence; do
  database="skuld_${owner}"
  role="$database"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v role="$role" -v database="$database" <<'SQL'
GRANT CONNECT ON DATABASE :"database" TO :"role";
SQL
done

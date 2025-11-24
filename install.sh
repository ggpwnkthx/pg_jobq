#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# install.sh
#
# Installs or upgrades the jobq schema into a target PostgreSQL database by
# running the SQL files via either:
#   - local psql, if available, or
#   - a postgres client Docker image, if docker is available.
#
# Behavior:
#   - Installs jobq into the database specified by --db-name.
#   - You can run this script multiple times against different databases to
#     give each of them their own jobq schema and queue.
#   - If the server parameter cron.database_name is set, the installer will
#     first attempt to install/upgrade jobq in that pg_cron "home" database
#     (if reachable) before installing into the requested --db-name. This
#     helps ensure that jobq.install_cron_jobs() can be created in the cron
#     home as soon as pg_cron is available there.
#   - pg_cron is OPTIONAL for this script:
#       * jobq core (jobs, functions, views, security) does not require
#         pg_cron to be installed in the target database.
#       * In whatever database pg_cron IS installed, running
#         20_security_and_cron.sql will create jobq.install_cron_jobs(),
#         which can then be used to schedule workers into any jobq-enabled DB.
#
#   - During 00_admin_bootstrap.sql, the azure_storage extension is also
#     created in the target database (IF NOT EXISTS). For that to succeed
#     on Azure Flexible Server, azure_storage must already be allowlisted
#     and its library loaded via:
#       * shared_preload_libraries (module load)
#       * azure.extensions (extension allowlist)
#
# Requirements:
#   - Either:
#       - local psql client, or
#       - docker CLI + postgres image
#   - SQL files are in the same directory as this script
#   - Target user has sufficient privileges:
#       * On Azure Flexible Server, this should normally be the server
#         admin login (member of azure_pg_admin) so it can:
#           - create the jobq_* roles
#           - optionally GRANT pg_signal_backend to jobq_worker
#           - manage pg_cron jobs via cron.schedule_in_database
#
# Usage:
#   ./install.sh \
#     --db-host myserver.postgres.database.azure.com \
#     --db-name mydb \
#     --db-user myadmin@myserver \
#     --db-password 'supersecret' \
#     [--db-port 5432] \
#     [--sslmode require] \
#     [--docker-image postgres:18]
#
# Behavior:
#   - The jobq schema, objects, and security will be installed into the
#     database passed via --db-name.
#   - To enable pg_cron-based scheduling:
#       * Ensure pg_cron is installed in exactly one database on the server.
#       * Run this installer (and all SQL) in that database as well so that
#         jobq.install_cron_jobs() is available there.
#       * Use jobq.install_cron_jobs('<db>') from that pg_cron home DB to
#         schedule workers into any jobq-enabled database.
###############################################################################

DB_HOST=""
DB_NAME=""      # Target DB from CLI (primary jobq target)
DB_USER=""
DB_PASSWORD=""
DB_PORT=5432
SSL_MODE="require"          # Azure PG typically needs this
DOCKER_IMAGE="postgres:18"  # Used only if we fall back to Docker

# This will be the actual database we run the installer against at any moment.
TARGET_DB=""

SCRIPTS=(
  "00_admin_bootstrap.sql"
  "10_jobq_types_and_table.sql"
  "11_jobq_enqueue_and_cancel.sql"
  "12_jobq_worker_core.sql"
  "13_jobq_monitoring.sql"
  "14_jobq_maintenance.sql"
  "20_security_and_cron.sql"
  "30_production_hardening_and_version.sql"
)

usage() {
  cat <<EOF
Usage: $0 --db-host HOST --db-name DB --db-user USER --db-password PASS [options]

Required:
  --db-host HOST         PostgreSQL host (e.g. myserver.postgres.database.azure.com)
  --db-name DB           Database name to install/upgrade jobq
  --db-user USER         Database user (ideally server admin / azure_pg_admin member)
  --db-password PASS     Database password

Optional:
  --db-port PORT         Port (default: 5432)
  --sslmode MODE         PGSSLMODE value (default: require)
  --docker-image IMAGE   Postgres client Docker image (default: postgres:18)
  -h, --help             Show this help

Behavior:
  - Installs jobq into the database specified by --db-name.
  - Run this script once per database that should have its own jobq schema.
  - If cron.database_name is set, the installer will first attempt to
    install/upgrade jobq in that pg_cron "home" database (if reachable)
    before installing into the requested --db-name.
  - On Azure Flexible Server, run this as the server admin login (member
    of azure_pg_admin). That login can create roles and, where supported,
    GRANT pg_signal_backend to jobq_worker and manage pg_cron jobs.
  - pg_cron is NOT required for jobq core; it is only required in the
    database where you want to use jobq.install_cron_jobs() to schedule
    workers via cron.schedule_in_database.
EOF
}

# Basic CLI parsing
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db-host)
      DB_HOST="$2"
      shift 2
      ;;
    --db-name)
      DB_NAME="$2"
      shift 2
      ;;
    --db-user)
      DB_USER="$2"
      shift 2
      ;;
    --db-password)
      DB_PASSWORD="$2"
      shift 2
      ;;
    --db-port)
      DB_PORT="$2"
      shift 2
      ;;
    --sslmode)
      SSL_MODE="$2"
      shift 2
      ;;
    --docker-image)
      DOCKER_IMAGE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

# Validate required params
if [[ -z "$DB_HOST" || -z "$DB_NAME" || -z "$DB_USER" || -z "$DB_PASSWORD" ]]; then
  echo "ERROR: --db-host, --db-name, --db-user, and --db-password are required." >&2
  usage
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Decide execution mode: local psql vs docker
RUN_MODE=""
if command -v psql >/dev/null 2>&1; then
  RUN_MODE="local"
elif command -v docker >/dev/null 2>&1; then
  RUN_MODE="docker"
else
  echo "ERROR: Neither 'psql' nor 'docker' is available on this system." >&2
  echo "       Install the PostgreSQL client or Docker and retry." >&2
  exit 1
fi

# Sanity-check that all expected SQL files exist
for sql_file in "${SCRIPTS[@]}"; do
  if [[ ! -f "$SCRIPT_DIR/$sql_file" ]]; then
    echo "ERROR: Expected SQL file not found: $SCRIPT_DIR/$sql_file" >&2
    exit 1
  fi
done

echo "=== Jobq installer kick-off ==="
echo "Host             : $DB_HOST"
echo "Primary target DB: $DB_NAME"
echo "User             : $DB_USER"
echo "Port             : $DB_PORT"
echo "SSL mode         : $SSL_MODE"
echo "SQL dir          : $SCRIPT_DIR"
if [[ "$RUN_MODE" == "local" ]]; then
  echo "Client           : local psql"
else
  echo "Client           : docker"
  echo "Docker image     : $DOCKER_IMAGE"
fi
echo
echo "NOTE: On Azure Flexible Server, this script is intended to run as"
echo "      the server admin login (member of azure_pg_admin) so it can"
echo "      create roles, manage pg_cron, and (where supported) grant"
echo "      pg_signal_backend to jobq_worker."
echo

TARGET_DB="$DB_NAME"

echo "=== Installation target resolved ==="
echo "Primary jobq target database: $TARGET_DB"
echo

###############################################################################
# Helper: run psql (local or docker) with common options
###############################################################################

run_psql() {
  # Args after this function name are passed directly to psql
  if [[ "$RUN_MODE" == "local" ]]; then
    PGPASSWORD="$DB_PASSWORD" PGSSLMODE="$SSL_MODE" \
      psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        "$@"
  else
    docker run --rm \
      -e PGPASSWORD="$DB_PASSWORD" \
      -e PGSSLMODE="$SSL_MODE" \
      -v "$SCRIPT_DIR":"$SCRIPT_DIR" \
      "$DOCKER_IMAGE" \
      psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        "$@"
  fi
}

###############################################################################
# Helper: run a SQL file against TARGET_DB
###############################################################################

run_sql() {
  local sql_file="$1"

  echo ">>> Applying $sql_file (database: $TARGET_DB)"

  run_psql \
    -d "$TARGET_DB" \
    -v ON_ERROR_STOP=1 \
    -f "$SCRIPT_DIR/$sql_file"

  echo "<<< Completed $sql_file"
  echo
}

###############################################################################
# Detect pg_cron home database (if any) and ensure jobq is installed there
# before the user-specified target database.
###############################################################################

CRON_DB_NAME=""
CRON_DB_NAME="$(run_psql -d "$DB_NAME" -Atqc "SELECT current_setting('cron.database_name', true)" 2>/dev/null || true)"
CRON_DB_NAME="${CRON_DB_NAME%%[[:space:]]*}"

if [[ -n "$CRON_DB_NAME" && "$CRON_DB_NAME" != "$DB_NAME" ]]; then
  echo "=== Detected pg_cron home database from cron.database_name: $CRON_DB_NAME ==="
  # Verify we can connect before trying to install there
  if run_psql -d "$CRON_DB_NAME" -Atqc "SELECT 1" >/dev/null 2>&1; then
    TARGET_DB="$CRON_DB_NAME"
    echo "Ensuring jobq is installed into pg_cron home database first: $TARGET_DB"
    echo
    for sql_file in "${SCRIPTS[@]}"; do
      run_sql "$sql_file"
    done
    echo "=== Jobq install completed successfully against pg_cron home database: $TARGET_DB ==="
    echo
  else
    echo "WARNING: cron.database_name is '$CRON_DB_NAME' but connection to that database failed."
    echo "         Skipping auto-install into cron home; continuing with primary target database only."
    echo
  fi
fi

###############################################################################
# Execute scripts in order against user-specified target database
###############################################################################

TARGET_DB="$DB_NAME"
echo "=== Installing jobq into requested target database: $TARGET_DB ==="
echo

for sql_file in "${SCRIPTS[@]}"; do
  run_sql "$sql_file"
done

echo "=== Jobq install completed successfully against database: $TARGET_DB ==="

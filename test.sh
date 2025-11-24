#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# test.sh
#
# Connectivity & sanity tests for:
#   - jobq core schema
#   - azure_storage extension
#   - Azure Storage account + key + container
#
# Behavior:
#   - Detects local psql vs Docker (postgres:18), similar to install.sh.
#   - Runs a series of SQL checks against the target database:
#       * Verifies azure_storage extension and jobq schema/objects exist.
#       * Registers the given storage account + key (azure_storage.account_add).
#       * Grants jobq_worker access to that account (account_user_add).
#       * Lists blobs in the given container (blob_list).
#       * Writes a tiny test blob (blob_put) and re-lists blobs.
#       * Enqueues a trivial job via jobq.enqueue(), runs the worker once
#         via jobq.run_next_job(), and asserts:
#           - the job finishes with status=succeeded
#           - result_blob_path is populated
#           - the corresponding blob exists in Azure Storage.
#       * Optionally checks jobq metrics views/functions.
#
# Requirements:
#   - Either:
#       * local psql client, or
#       * docker CLI + postgres image
#   - Target user should normally be the server admin / azure_pg_admin
#     login on Azure Flexible Server, so it can:
#       * use azure_storage_admin capabilities (account_add/account_user_add)
#       * see extensions and schemas
#
# NOTE:
#   - The script never prints the storage account key.
#   - The test blob is created with a "jobq_test_YYYYMMDDHH24MISS.csv" name
#     under the specified container.
#   - The enqueue/export test assumes the caller can SET ROLE jobq_client
#     and jobq_worker (which is true for the server admin login that created
#     the jobq roles on Azure Flexible Server).
###############################################################################

DB_HOST=""
DB_NAME=""
DB_USER=""
DB_PASSWORD=""
DB_PORT=5432
SSL_MODE="require"          # Azure PG typically needs this
DOCKER_IMAGE="postgres:18"  # Used only if we fall back to Docker

STORAGE_ACCOUNT=""
STORAGE_KEY=""
STORAGE_CONTAINER=""

RUN_MODE=""

usage() {
  cat <<EOF
Usage: $0 --db-host HOST --db-name DB --db-user USER --db-password PASS \\
          --storage-account NAME --storage-key KEY --storage-container CONTAINER [options]

Required:
  --db-host HOST              PostgreSQL host (e.g. myserver.postgres.database.azure.com)
  --db-name DB                Database name to test
  --db-user USER              Database user (ideally server admin / azure_pg_admin member)
  --db-password PASS          Database password

  --storage-account NAME      Azure Storage account name
  --storage-key KEY           Azure Storage account access key
  --storage-container NAME    Azure Blob container name (must exist)

Optional:
  --db-port PORT              Port (default: 5432)
  --sslmode MODE              PGSSLMODE value (default: require)
  --docker-image IMAGE        Postgres client Docker image (default: postgres:18)
  -h, --help                  Show this help

What this script tests:
  - jobq core is installed in the target DB:
      * jobq schema
      * jobq.jobs table
      * jobq.job_status type
      * jobq.enqueue() function (existence + basic happy-path behaviour)
      * jobq.run_next_job() (existence + basic happy-path behaviour)
  - azure_storage extension is installed in the target DB.
  - The provided storage account & key can be registered via azure_storage.account_add().
  - jobq_worker is granted access to that account via azure_storage.account_user_add().
  - The container is reachable via azure_storage.blob_list().
  - A small test blob can be written via azure_storage.blob_put().
  - A simple job can be enqueued into jobq.jobs, executed end-to-end via
    jobq.run_next_job(), and its Parquet export blob verified to exist.
EOF
}

###############################################################################
# Basic CLI parsing
###############################################################################
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
    --storage-account)
      STORAGE_ACCOUNT="$2"
      shift 2
      ;;
    --storage-key)
      STORAGE_KEY="$2"
      shift 2
      ;;
    --storage-container)
      STORAGE_CONTAINER="$2"
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

if [[ -z "$STORAGE_ACCOUNT" || -z "$STORAGE_KEY" || -z "$STORAGE_CONTAINER" ]]; then
  echo "ERROR: --storage-account, --storage-key, and --storage-container are required." >&2
  usage
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SQL_TEST_FILES=(
  "40_preflight.sql"
  "41_register_storage.sql"
  "42_blob_list.sql"
  "43_blob_put.sql"
  "44_enqueue_job.sql"
  "45_metrics.sql"
)

# Sanity-check that all expected SQL files exist
for sql_file in "${SQL_TEST_FILES[@]}"; do
  if [[ ! -f "$SCRIPT_DIR/$sql_file" ]]; then
    echo "ERROR: Expected SQL file not found: $SCRIPT_DIR/$sql_file" >&2
    exit 1
  fi
done

###############################################################################
# Decide execution mode: local psql vs docker
###############################################################################
if command -v psql >/dev/null 2>&1; then
  RUN_MODE="local"
elif command -v docker >/dev/null 2>&1; then
  RUN_MODE="docker"
else
  echo "ERROR: Neither 'psql' nor 'docker' is available on this system." >&2
  echo "       Install the PostgreSQL client or Docker and retry." >&2
  exit 1
fi

echo "=== jobq / azure_storage test runner ==="
echo "Host               : $DB_HOST"
echo "Database           : $DB_NAME"
echo "User               : $DB_USER"
echo "Port               : $DB_PORT"
echo "SSL mode           : $SSL_MODE"
echo "Client             : $RUN_MODE"
if [[ "$RUN_MODE" == "docker" ]]; then
  echo "Docker image       : $DOCKER_IMAGE"
fi
echo
echo "Azure Storage acct : $STORAGE_ACCOUNT"
echo "Container          : $STORAGE_CONTAINER"
echo "(Key is provided but will not be printed.)"
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
# Helper: run a SQL test file against DB_NAME
###############################################################################
run_sql_file() {
  local sql_file="$1"
  shift
  run_psql \
    -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 \
    "$@" \
    -f "$SCRIPT_DIR/$sql_file"
}

###############################################################################
# 1. Preflight: check jobq + azure_storage objects exist
###############################################################################
echo ">>> [1/6] Preflight: checking azure_storage extension and jobq core objects..."
run_sql_file "40_preflight.sql"
echo "<<< Preflight OK."
echo

###############################################################################
# 2. Register storage account & grant jobq_worker access
###############################################################################
echo ">>> [2/6] azure_storage.account_add() and account_user_add(jobq_worker)..."
run_sql_file "41_register_storage.sql" \
  -v storage_account="$STORAGE_ACCOUNT" \
  -v storage_key="$STORAGE_KEY"
echo "<<< Storage account registered and jobq_worker granted access."
echo

###############################################################################
# 3. Connectivity test: list blobs in the container
###############################################################################
echo ">>> [3/6] Testing connectivity with azure_storage.blob_list()..."
run_sql_file "42_blob_list.sql" \
  -v storage_account="$STORAGE_ACCOUNT" \
  -v storage_container="$STORAGE_CONTAINER"
echo "<<< blob_list() call succeeded (container reachable)."
echo

###############################################################################
# 4. Write a small test blob via azure_storage.blob_put()
###############################################################################
echo ">>> [4/6] Writing a small test blob via azure_storage.blob_put()..."
run_sql_file "43_blob_put.sql" \
  -v storage_account="$STORAGE_ACCOUNT" \
  -v storage_container="$STORAGE_CONTAINER"
echo "<<< blob_put() test completed (test blob created & listed)."
echo

###############################################################################
# 5. jobq enqueue + run_next_job() + blob existence sanity check
###############################################################################
echo ">>> [5/6] jobq.enqueue()/run_next_job() end-to-end export test..."
run_sql_file "44_enqueue_job.sql" \
  -v storage_account="$STORAGE_ACCOUNT" \
  -v storage_container="$STORAGE_CONTAINER"
echo "<<< jobq.enqueue()/run_next_job() test completed (job row created, succeeded, and blob verified)."
echo

###############################################################################
# 6. Final jobq sanity check: simple metrics query (if permitted)
###############################################################################
echo ">>> [6/6] Optional: jobq metrics surface check (if permissions allow)..."

# This may fail if the current user does not have SELECT/EXECUTE on the
# metrics views/functions. That's OK; we treat it as a soft warning.
if run_psql \
     -d "$DB_NAME" \
     -v ON_ERROR_STOP=1 \
     -f "$SCRIPT_DIR/45_metrics.sql" \
     >/dev/null 2>&1; then
  echo "<<< jobq metrics queries succeeded."
else
  echo "!!! WARNING: jobq metrics test failed."
  echo "    This may simply mean the current user lacks SELECT/EXECUTE on"
  echo "    jobq.v_queue_overview or jobq.get_queue_metrics()."
fi

echo
echo "=== All core tests completed. ==="
echo "If no errors were reported above, jobq + azure_storage + the provided"
echo "storage account/container are wired up and reachable from this database,"
echo "and end-to-end exports via jobq.run_next_job() -> azure_storage.blob_put()"
echo "are working (including verification that the output blob exists)."
echo

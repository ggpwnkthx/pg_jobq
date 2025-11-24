# jobq – Read‑only Job Queue for Azure Database for PostgreSQL Flexible

`jobq` is a per‑database, read‑only job queue for **Azure Database for PostgreSQL – Flexible Server (v18)** that runs ad‑hoc or scheduled SQL exports to **Azure Blob Storage** (Parquet) using the `azure_storage` extension.

It is designed for **long‑running analytics / ETL exports** that you **do not** want to run directly in your OLTP app sessions, and that you want to control centrally from Postgres (with monitoring, retries, and ops controls).

---

## Features

- **Per‑database queue**  
  Each database that runs the installer gets its own `jobq.jobs` table and worker APIs. Jobs always execute in the context of the database where they are enqueued.

- **Read‑only execution**  
  `jobq.enqueue()` enforces **SELECT/WITH only** (no comments, semicolons, `INTO`, or obvious DML/DDL keywords) before the query is executed under the `jobq_worker` role.

- **Azure Blob exports (Parquet)**  
  Jobs stream results to Azure Storage via `azure_storage.blob_put`, storing the blob path back into `jobq.jobs.result_blob_path`.

- **Scheduling with `pg_cron`**  
  With `pg_cron` installed in one database (the *cron home*), `jobq` exposes `jobq.install_cron_jobs()` to wire:
  - a worker loop (`jobq-runner`)
  - an orphan requeueer (`jobq-requeue-orphans`)
  - a retention cleaner (`jobq-purge-old`)

- **Backoff, retries, and timeouts**
  - Per‑job `max_runtime` → `statement_timeout`
  - `max_attempts` + exponential-ish backoff (1–10 minutes)
  - Jobs are retried until `max_attempts` is reached, then marked `failed`.

- **Monitoring & ops**
  - Views: `jobq.v_queue_overview`, `jobq.v_running_jobs`, `jobq.v_recent_jobs`, `jobq.v_stalled_jobs`
  - Metrics function: `jobq.get_queue_metrics()`
  - Ops APIs: `jobq.kill`, `jobq.requeue_orphaned_running_jobs`, `jobq.purge_old_jobs`

---

## Requirements

### Postgres / Azure

- **Azure Database for PostgreSQL – Flexible Server v18**
- `azure_storage` extension allow‑listed and loadable:
  - `shared_preload_libraries` includes `azure_storage`
  - `azure.extensions` includes `azure_storage`
- `pg_cron` installed in **exactly one** database on the server:
  - `shared_preload_libraries` includes `pg_cron`
  - `azure.extensions` includes `pg_cron`
  - `cron.database_name` points at the *pg_cron home database*

### Client tools

On the machine where you run the scripts, you need either:

- Local `psql` client, **or**
- `docker` plus a Postgres client image (default: `postgres:18`)

---

## Security Model

### Roles

The bootstrap script creates cluster‑level roles:

- `jobq_worker`  
  Owner of the `jobq` schema and SECURITY DEFINER functions. **NOLOGIN** / `NOINHERIT`. Used only as an execution context.

- `jobq_reader`  
  Marker role for read‑only access to data your jobs will query. You grant SELECT privileges on your app tables separately.

- `jobq_client`  
  Intended for **trusted backend services** that enqueue and cancel jobs via `jobq.enqueue` and `jobq.cancel`.

- `jobq_reporting`  
  Intended for BI/reporting users. Gets SELECT on `jobq` views & metrics.

- `jobq_ops`  
  Ops/SRE roles for kill/requeue/purge and viewing monitoring surfaces.

> **Important:** `jobq.enqueue` is *not* a sandbox. Any caller can run arbitrary **read‑only SQL** under `jobq_worker`. Only trusted backend roles should ever get EXECUTE on it, and `jobq_worker` should only have the minimal SELECT it needs.

### Ownership & privileges

`20_security_and_cron.sql`:

- Sets `jobq_worker` as owner of:
  - `jobq` schema
  - `jobq.jobs` table and job_id sequence
  - `jobq` views
- Revokes PUBLIC usage on the schema and table, then grants:
  - `jobq_worker`: full on `jobq.jobs`
  - `jobq_reporting` / `jobq_ops`: SELECT on views (and optionally on `jobq.jobs`)
  - `jobq_client`: EXECUTE on `jobq.enqueue` and `jobq.cancel`
  - `jobq_ops`: EXECUTE on `kill`, `requeue_orphaned_running_jobs`, `purge_old_jobs`
  - `azure_pg_admin`: EXECUTE on maintenance/ops functions for admin logins

---

## Installation

### 1. Server‑level prerequisites (Azure Portal / CLI)

On your PostgreSQL Flexible Server:

1. Allow‑list the extensions and preload libraries:

   - Add `azure_storage` and `pg_cron` to:
     - `shared_preload_libraries`
     - `azure.extensions`

2. Set:

   - `cron.database_name = '<cron_home_db>'`

   where `<cron_home_db>` is the database where you will install pg_cron and from which you want to manage cron jobs.

> These are **server parameters**, not SQL statements; set them in the Azure Portal or via CLI and then restart the server if required.

### 2. Run the installer

From your local shell:

```bash
chmod +x install.sh

./install.sh \
  --db-host myserver.postgres.database.azure.com \
  --db-name app_db \
  --db-user myadmin@myserver \
  --db-password 'supersecret' \
  --db-port 5432 \
  --sslmode require
```

What this does:

- Installs/updates `jobq` into the `--db-name` database.
- If `cron.database_name` is set and reachable, it also installs `jobq`
  into that **cron home** database first so `jobq.install_cron_jobs()` will exist there.

You can run the installer multiple times against different databases:

```bash
./install.sh ... --db-name reporting_db
./install.sh ... --db-name analytics_db
```

Each database gets its own `jobq.jobs` queue.

> On Azure Flexible Server, run this as the server admin login (member of `azure_pg_admin`) so the script can:
> - Create the `jobq_*` roles.
> - Optionally grant `pg_signal_backend` to `jobq_worker`.
> - Manage `pg_cron` jobs via `cron.schedule_in_database`.

---

## Testing the Installation

Use `test.sh` to verify `jobq` + `azure_storage` + your Storage account:

```bash
chmod +x test.sh

./test.sh \
  --db-host myserver.postgres.database.azure.com \
  --db-name app_db \
  --db-user myadmin@myserver \
  --db-password 'supersecret' \
  --storage-account mystorageacct \
  --storage-key 'AZURE_STORAGE_ACCOUNT_KEY' \
  --storage-container jobq-exports
```

The test runner will:

1. Run `40_preflight.sql`:
   - Check `azure_storage` extension exists.
   - Check `jobq` schema, `jobq.jobs` table, `jobq.job_status` type, and key functions.
2. Run `41_storage_and_jobq_integration.sql`:
   - Register the storage account and key:
     - `azure_storage.account_add(<name>, <key>)`
   - Grant `jobq_worker` access to that account:
     - `azure_storage.account_user_add(<name>, 'jobq_worker')`
   - Verify container connectivity with `azure_storage.blob_list()`.
   - Write a small test blob with `azure_storage.blob_put()` and re-list blobs.
   - Enqueue a trivial job via `jobq.enqueue('SELECT 1 AS jobq_test_value', ...)`.
   - Run the worker once via `CALL jobq.run_next_job()`.
   - Wait for that specific job to reach a terminal status.
   - Assert:
     - `status = 'succeeded'`
     - `result_blob_path` is populated
     - the referenced blob exists in Azure Storage.
3. Run `42_metrics.sql` (best-effort):
   - Attempt to query `jobq.v_queue_overview` and `jobq.get_queue_metrics()`.
   - If the current user lacks permissions, this logs a warning but does not fail the test run.

If all steps succeed, you have a working end-to-end path from `jobq` to Azure Blob.

---

## Usage

### Roles and access

Typical mapping:

- **App / service** → member of `jobq_client`
- **BI / reporting users** → member of `jobq_reporting`
- **Ops / SRE** → member of `jobq_ops`
- **Admin** → server admin login (member of `azure_pg_admin`), can also assume `jobq_worker` for maintenance.

Granting access (run as admin):

```sql
-- Grant your application role the ability to enqueue/cancel jobs
GRANT jobq_client TO app_role;

-- Grant reporting group access to metrics and views
GRANT jobq_reporting TO reporting_role;

-- Grant ops/SRE group operational controls
GRANT jobq_ops TO sre_role;
```

### Enqueueing a job (application)

As a `jobq_client` role, enqueue a read‑only query:

```sql
SELECT jobq.enqueue(
  p_query_sql         => $q$
    SELECT
      id,
      created_at,
      amount
    FROM app_schema.orders
    WHERE created_at >= now() - interval '1 day'
  $q$,
  p_storage_account   => 'mystorageacct',      -- name from azure_storage.account_add
  p_storage_container => 'jobq-exports',       -- existing container
  p_scheduled_at      => now(),                -- run as soon as a worker can pick it up
  p_priority          => 0,                    -- higher number = higher priority
  p_correlation_id    => 'daily_orders_export',
  p_max_runtime       => interval '30 minutes' -- per-job timeout [1s, 24h]
) AS job_id;
```

Constraints enforced by `jobq.enqueue`:

- Query must start with `SELECT` or `WITH`
- No semicolons (`;`)
- No SQL comments (`--` or `/* */`)
- No `INTO`
- No obvious DML/DDL keywords (INSERT/UPDATE/DELETE/MERGE/CREATE/ALTER/DROP/…)
- `p_priority` must be between -1000 and 1000
- `p_max_runtime` in (0, 24h]

### Running jobs

#### With pg_cron (recommended)

In the database where **pg_cron** and **jobq** are installed (the cron home DB), as an `azure_pg_admin` login:

```sql
-- Target the current database:
SELECT jobq.install_cron_jobs();

-- Or target another jobq-enabled database:
SELECT jobq.install_cron_jobs('app_db');
```

This creates three cron jobs:

- `jobq-runner` – `CALL jobq.run_next_job();` every minute
- `jobq-requeue-orphans` – `SELECT jobq.requeue_orphaned_running_jobs(100);` every 5 minutes
- `jobq-purge-old` – `SELECT jobq.purge_old_jobs('30 days', 50000);` daily at 03:00

> You can adjust schedules afterwards using the `cron.job` table and `cron.unschedule`.

#### Manually (no pg_cron)

In a jobq-enabled database, as `jobq_worker` (or admin that can SET ROLE jobq_worker):

```sql
SET ROLE jobq_worker;

-- IMPORTANT: jobq.run_next_job() does its own COMMITs.
-- It must be called as a top-level CALL, not inside an explicit BEGIN/COMMIT.
CALL jobq.run_next_job();
```

You can call this in a loop from your application or from an external scheduler if you don’t want to use pg_cron.

### Monitoring

In a jobq-enabled database:

```sql
-- High-level overview
SELECT * FROM jobq.v_queue_overview;

-- Typed metrics row
SELECT * FROM jobq.get_queue_metrics();

-- Running jobs with pg_stat_activity details
SELECT * FROM jobq.v_running_jobs;

-- Stalled jobs (elapsed > max_runtime)
SELECT * FROM jobq.v_stalled_jobs;

-- Recent jobs (7-day history)
SELECT * FROM jobq.v_recent_jobs
ORDER BY job_id DESC
LIMIT 100;
```

### Operations & maintenance

As `jobq_ops` (or admin):

```sql
-- Soft-cancel a pending job
SELECT jobq.cancel(12345);  -- TRUE if a pending job was cancelled

-- Best-effort kill a running job/backend (see caveats below)
SELECT jobq.kill(12345);

-- Requeue or fail orphaned running jobs (status='running' but no backend)
SELECT jobq.requeue_orphaned_running_jobs(100);

-- Purge finished jobs older than 30 days (up to 50k rows)
SELECT jobq.purge_old_jobs('30 days', 50000);
```

---

## Runtime Behavior & Tuning

### Global parallelism and connection headroom

`jobq.claim_next_job()` uses two custom GUCs to control concurrency:

- `jobq.max_parallel_jobs` (global cap across the cluster)
- `jobq.min_free_connections` (minimum free connections before picking up a new job)

Configure per database (or globally) as needed:

```sql
ALTER DATABASE app_db
  SET jobq.max_parallel_jobs = '8';

ALTER DATABASE app_db
  SET jobq.min_free_connections = '5';
```

At runtime:

- `max_connections` and `pg_stat_activity` are used to compute free connections.
- If `free_connections <= jobq.min_free_connections`, no job is claimed.
- If all advisory lock “slots” are busy, no job is claimed.

### Per-job timeout

`jobq.runner`:

- Reads `max_runtime` from the `jobq.jobs` row and clamps it into `[1 second, 24 hours]`.
- Sets `statement_timeout` for the duration of the job.
- Restores the previous `statement_timeout` afterward.

If the query exceeds the timeout (statement timeout), it is treated like any other error: the job is retried until `max_attempts` is reached, then marked `failed`.

### Retention

`jobq.purge_old_jobs(p_older_than, p_limit)` deletes finished jobs (statuses `succeeded`, `failed`, `cancelled`) whose `finished_at` is older than `p_older_than`.

The default cron schedule (via `jobq.install_cron_jobs`) runs:

```sql
SELECT jobq.purge_old_jobs('30 days', 50000);
```

daily at 03:00.

You can override this by editing the cron job, or run purges manually if you prefer to control retention yourself.

---

## Caveats & Design Notes

- **Not a sandbox**: `jobq.enqueue` only does string‑level checks. It does not analyze function volatility or side effects. Treat caller roles as trusted backend principals and scope `jobq_worker`’s SELECT privileges carefully.
- **Kill semantics**: `jobq.kill` is best‑effort and conservative. It performs sanity checks on the backend query text and role membership before calling `pg_terminate_backend`. Depending on timing, it may behave more like “mark this as cancelled if still running” than a hard preemptive kill.
- **Empty result sets**: The underlying `azure_storage.blob_put` pattern is a `SELECT ... FROM (p_query_sql) AS q`. If the query returns zero rows, `blob_put` is not called and no blob is created. The job can still be marked `succeeded`. Decide whether this is acceptable for your downstream consumers.

---

## Diagrams

- `architecture.mmd` – high‑level topology: app / jobq / pg_cron / Azure Storage
- `lifecycle.mmd` – worker & job lifecycle (enqueue → claim → run → succeed/fail/requeue/kill)

---

## Upgrades / Migrations

The SQL scripts are intended to be **re-runnable**:

- Types and schema objects are created with `IF NOT EXISTS`.
- New columns and constraints are backfilled with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and explicit constraint checks.
- Ownership and privileges can be applied repeatedly without breaking existing setups.

Recommended pattern:

- Check in the SQL scripts alongside your application code.
- For each target database, re-run `install.sh` as part of your deployment pipeline.

---

## Troubleshooting

If something fails during install or test:

- Run `40_preflight.sql` manually to see which objects are missing.
- Check that:
  - `azure_storage` is allow‑listed and the extension exists in the target DB.
  - `pg_cron` is correctly installed in the cron home DB.
  - You are running as the server admin (member of `azure_pg_admin`) on Azure Flexible.
- Look at `jobq.v_running_jobs` and `jobq.v_recent_jobs` for error messages and `last_error` details.

# Lakebase Provisioned for AI Agent Development

**Official Documentation**:
- https://docs.databricks.com/aws/en/oltp/instances/about (What is Lakebase Provisioned?)
- https://docs.databricks.com/aws/en/oltp/instances/authentication (Authenticate to a database instance)
- https://databricks-sdk-py.readthedocs.io/en/latest/workspace/database/database.html (Python SDK Reference)

Lakebase Provisioned is a fully managed PostgreSQL 16 OLTP database engine embedded in the Databricks Data Intelligence Platform. It provides dedicated compute and Databricks-managed storage for serving golden tables to applications, storing application state, and serving features to ML models — all governed by Unity Catalog and natively synced with Delta Lake.

## Installation

Already included in this template:
```toml
# pyproject.toml
databricks-sdk>=0.61.0
psycopg2-binary
```

## Setup

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    DatabaseInstance, DatabaseCatalog,
    SyncedDatabaseTable, SyncedTableSpec,
    NewPipelineSpec, SyncedTableSchedulingPolicy
)
import uuid

# Initialize client (uses your configured authentication)
w = WorkspaceClient()
```

## 1. Instance Lifecycle Management

### Create a Database Instance

Create a new Lakebase Provisioned instance with dedicated compute and storage. Each Capacity Unit (CU) provides approximately 16 GB RAM with proportional CPU and local SSD.

```python
# Create a basic instance
instance = w.database.create_database_instance(
    DatabaseInstance(
        name="my-instance",          # 1-63 chars, letter-first, alphanumeric + hyphens
        capacity="CU_1",             # CU_1 | CU_2 | CU_4 | CU_8
        retention_window_in_days=7,  # PITR window: 2-35 days, default 7
        node_count=1,                # 1=no HA, 2-3=HA nodes
        enable_readable_secondaries=False,
        enable_pg_native_login=False
    )
)

print(f"Created: {instance.name}")
print(f"Endpoint: {instance.read_write_dns}")

# Blocking variant (waits until instance is AVAILABLE, ~20 min timeout)
instance = w.database.create_database_instance_and_wait(
    DatabaseInstance(name="my-instance", capacity="CU_1")
)
```

**Available Capacity Tiers:**
- `CU_1` — ~16 GB RAM
- `CU_2` — ~32 GB RAM (default)
- `CU_4` — ~64 GB RAM
- `CU_8` — ~128 GB RAM

**Instance Limits:**
- Max 10 instances per workspace
- Max 2 TB storage per instance
- Max 1,000 concurrent connections per instance

### List Database Instances

```python
# List all instances (auto-paginating iterator)
for inst in w.database.list_database_instances():
    print(f"Name: {inst.name}")
    print(f"State: {inst.state}")
    print(f"Capacity: {inst.capacity}")
    print(f"Endpoint: {inst.read_write_dns}")
    print("---")
```

### Get Instance Details

```python
# Get a single instance by name
instance = w.database.get_database_instance(name="my-instance")
print(f"Name: {instance.name}")
print(f"State: {instance.state}")           # STARTING, AVAILABLE, UPDATING, STOPPED, DELETING
print(f"Capacity: {instance.capacity}")
print(f"Host: {instance.read_write_dns}")    # Connection endpoint
print(f"Stopped: {instance.effective_stopped}")
```

### Update an Instance

Resize compute, stop/start, or enable high availability on an existing instance:

```python
# Resize to CU_4
w.database.update_database_instance(
    name="my-instance",
    database_instance=DatabaseInstance(name="my-instance", capacity="CU_4"),
    update_mask="*"
)

# Stop the instance (data preserved, no compute billing)
w.database.update_database_instance(
    name="my-instance",
    database_instance=DatabaseInstance(name="my-instance", stopped=True),
    update_mask="*"
)

# Start the instance
w.database.update_database_instance(
    name="my-instance",
    database_instance=DatabaseInstance(name="my-instance", stopped=False),
    update_mask="*"
)

# Enable HA with 3 nodes and readable secondaries
w.database.update_database_instance(
    name="my-instance",
    database_instance=DatabaseInstance(
        name="my-instance",
        node_count=3,
        enable_readable_secondaries=True
    ),
    update_mask="*"
)
```

### Delete an Instance

```python
# Simple delete (purge=True is required)
w.database.delete_database_instance(name="my-instance", purge=True)

# Force delete (also deletes child instances created via PITR)
w.database.delete_database_instance(name="my-instance", force=True, purge=True)
```

### Point-in-Time Recovery (Child Instances)

Create copy-on-write clones from a parent instance for data recovery or testing:

```python
# Clone from current point in time
child = w.database.create_database_instance(
    DatabaseInstance(
        name="recovery-clone",
        capacity="CU_1",
        parent_instance_ref={"name": "my-instance"}
    )
)

# Clone from a specific timestamp (UTC)
child = w.database.create_database_instance(
    DatabaseInstance(
        name="recovery-clone",
        capacity="CU_1",
        parent_instance_ref={
            "name": "my-instance",
            "branch_timestamp": "2026-01-15T10:00:00Z"
        }
    )
)

# Clone from a specific WAL LSN
child = w.database.create_database_instance(
    DatabaseInstance(
        name="recovery-clone",
        capacity="CU_1",
        parent_instance_ref={
            "name": "my-instance",
            "lsn": "0/2A156E"
        }
    )
)
```

**Documentation:**
- [Create and Manage Instances](https://docs.databricks.com/aws/en/oltp/instances/create/)
- [Manage Instance Capacity](https://docs.databricks.com/aws/en/oltp/instances/create/capacity)
- [Restore Data and Time Travel](https://docs.databricks.com/aws/en/oltp/instances/create/child-instance)

## 2. Authentication and Credentials

### Generate OAuth Credential

Lakebase uses OAuth tokens tied to Databricks identities. Tokens expire after approximately 1 hour and must be refreshed for long-lived connections.

```python
# Generate credential for a user
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=["my-instance"]
)

token = cred.token              # The OAuth password for Postgres
expiry = cred.expiration_time   # ISO timestamp (~1 hour from now)
```

### Service Principal Authentication (M2M OAuth)

```python
# Initialize client with service principal credentials
w = WorkspaceClient(
    host="https://<WORKSPACE_URL>/",
    client_id="<SERVICE_PRINCIPAL_ID>",
    client_secret="<SECRET>"
)

cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=["my-instance"]
)
```

**Identity Mapping:**
- Databricks user email → Postgres role name (e.g., `alice@company.com`)
- Service principal application ID → Postgres role name (e.g., `<client_id>`)

**Documentation:**
- [Authenticate to a Database Instance](https://docs.databricks.com/aws/en/oltp/instances/authentication)

## 3. Connecting with PostgreSQL Clients

### psycopg2 — Single Connection

The simplest way to connect from Python. Use the OAuth token as the Postgres password:

```python
import psycopg2

instance = w.database.get_database_instance(name="my-instance")
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=["my-instance"]
)

conn = psycopg2.connect(
    host=instance.read_write_dns,
    dbname="databricks_postgres",
    user="alice@company.com",
    password=cred.token,
    sslmode="require"
)

with conn.cursor() as cur:
    cur.execute("SELECT version()")
    print(cur.fetchone()[0])
conn.close()
```

### psycopg2 — Connection Pool

```python
from psycopg2 import pool

connection_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    user="alice@company.com",
    password=cred.token,
    host=instance.read_write_dns,
    port="5432",
    database="databricks_postgres"
)

conn = connection_pool.getconn()
# ... use connection ...
connection_pool.putconn(conn)
```

### psycopg3 — Auto-Rotating Token Pool

For long-lived applications that need automatic credential refresh:

```python
import psycopg
from psycopg_pool import ConnectionPool

class AutoAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo='', **kwargs):
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=["my-instance"]
        )
        kwargs['password'] = cred.token
        return super().connect(conninfo, **kwargs)

pool = ConnectionPool(
    conninfo=f"dbname=databricks_postgres user=alice@company.com host={instance.read_write_dns}",
    connection_class=AutoAuthConnection,
    min_size=1,
    max_size=10,
    open=True
)

with pool.connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(cur.fetchone())
```

### SQLAlchemy with Token Refresh

```python
from sqlalchemy import create_engine, text, event
import time

engine = create_engine(
    f"postgresql+psycopg2://{user}:@{host}:5432/databricks_postgres"
)

_token, _last_refresh = None, 0

@event.listens_for(engine, "do_connect")
def provide_token(dialect, conn_rec, cargs, cparams):
    global _token, _last_refresh
    if _token is None or time.time() - _last_refresh > 900:  # 15-min refresh
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=["my-instance"]
        )
        _token = cred.token
        _last_refresh = time.time()
    cparams["password"] = _token

with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    print(result.fetchone())
```

### psql (Command Line)

```bash
TOKEN=$(databricks database generate-database-credential \
  --request-id $(uuidgen) \
  --json '{"instance_names":["my-instance"]}' | jq -r .token)

psql "postgresql://alice%40company.com:${TOKEN}@<read_write_dns>/databricks_postgres?sslmode=require"
```

### Databricks Notebook (Built-in Helper)

```python
# No manual credential needed inside a Databricks notebook
conn = w.database.connect("my-instance", database="databricks_postgres")
with conn.cursor() as cur:
    cur.execute("SELECT version()")
    print(cur.fetchone())
conn.close()
```

**Documentation:**
- [Access from SQL Clients](https://docs.databricks.com/aws/en/oltp/instances/query/psql)
- [Access from Notebooks](https://docs.databricks.com/aws/en/oltp/instances/query/notebook)

## 4. SQL Operations

Lakebase is standard Postgres 16. All DDL and DML use standard PostgreSQL syntax over the wire protocol.

### Schema and Table Creation

```sql
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.customers (
    id          SERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    metadata    JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_customers_email ON app.customers(email);
```

### CRUD Operations

```sql
-- Insert
INSERT INTO app.customers (email, name, metadata)
VALUES ('alice@example.com', 'Alice', '{"tier": "premium"}')
RETURNING id;

-- Read
SELECT * FROM app.customers WHERE metadata->>'tier' = 'premium';

-- Update
UPDATE app.customers SET metadata = metadata || '{"verified": true}' WHERE id = 1;

-- Delete
DELETE FROM app.customers WHERE created_at < NOW() - INTERVAL '1 year';

-- Transaction
BEGIN;
  UPDATE app.customers SET name = 'Alice Smith' WHERE id = 1;
  INSERT INTO app.audit_log (action, customer_id) VALUES ('rename', 1);
COMMIT;
```

### CRUD from Python

```python
with conn.cursor() as cur:
    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app.products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10,2),
            tags TEXT[]
        )
    """)
    conn.commit()

    # Insert
    cur.execute(
        "INSERT INTO app.products (name, price, tags) VALUES (%s, %s, %s) RETURNING id",
        ("Widget", 29.99, ["electronics", "sale"])
    )
    product_id = cur.fetchone()[0]
    conn.commit()

    # Read
    cur.execute("SELECT * FROM app.products WHERE id = %s", (product_id,))
    row = cur.fetchone()

    # Update
    cur.execute("UPDATE app.products SET price = %s WHERE id = %s", (24.99, product_id))
    conn.commit()

    # Delete
    cur.execute("DELETE FROM app.products WHERE id = %s", (product_id,))
    conn.commit()
```

### Extensions

Lakebase ships with 47 extensions. Enable them with standard `CREATE EXTENSION`:

```sql
CREATE EXTENSION IF NOT EXISTS vector;             -- pgvector 0.8.0 (AI/ML embeddings)
CREATE EXTENSION IF NOT EXISTS postgis;            -- PostGIS 3.3.3 (geospatial)
CREATE EXTENSION IF NOT EXISTS pg_graphql;         -- GraphQL queries
CREATE EXTENSION IF NOT EXISTS pg_stat_statements; -- Query analytics
CREATE EXTENSION IF NOT EXISTS pgcrypto;           -- Encryption
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";        -- UUID generation
CREATE EXTENSION IF NOT EXISTS hstore;             -- Key-value pairs
CREATE EXTENSION IF NOT EXISTS citext;             -- Case-insensitive text
```

### User and Role Management

```sql
CREATE EXTENSION IF NOT EXISTS databricks_auth;

-- Create role for a Databricks user
SELECT databricks_create_role('bob@company.com', 'USER');

-- Create role for a service principal
SELECT databricks_create_role('<service_principal_client_id>', 'SERVICE_PRINCIPAL');

-- Grant permissions
GRANT USAGE ON SCHEMA app TO "bob@company.com";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app TO "bob@company.com";
```

### Database Parameter Configuration

```sql
-- View configurable parameters
SELECT name FROM pg_settings WHERE context = 'user';

-- Set for current session
SET maintenance_work_mem = '1 GB';

-- Set for all sessions on a database
ALTER DATABASE databricks_postgres SET maintenance_work_mem = '1 GB';

-- Set per-role
ALTER USER "alice@company.com" SET maintenance_work_mem = '1 GB';
```

**Documentation:**
- [PostgreSQL Compatibility](https://docs.databricks.com/aws/en/oltp/instances/query/postgres-compatibility)
- [Manage Permissions](https://docs.databricks.com/aws/en/oltp/instances/manage-privileges)

## 5. Unity Catalog Integration

### Register a Database as a UC Catalog

Registering creates a read-only Unity Catalog catalog that makes Postgres tables discoverable from SQL warehouses, notebooks, and Catalog Explorer:

```python
# Register an existing Postgres database
catalog = w.database.create_database_catalog(
    DatabaseCatalog(
        name="my_catalog",
        database_instance_name="my-instance",
        database_name="databricks_postgres"
    )
)

# Create a new Postgres database AND register it
catalog = w.database.create_database_catalog(
    DatabaseCatalog(
        name="new_catalog",
        database_instance_name="my-instance",
        database_name="new_database",
        create_database_if_not_exists=True
    )
)
```

### List, Get, and Delete Catalogs

```python
# List catalogs for an instance
for cat in w.database.list_database_catalogs(instance_name="my-instance"):
    print(f"Catalog: {cat.name}")

# Get a specific catalog
cat = w.database.get_database_catalog(name="my_catalog")

# Delete
w.database.delete_database_catalog(name="my_catalog")
```

**Documentation:**
- [Register Database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/instances/register-uc)

## 6. Synced Tables (Delta Lake → Postgres)

Synced tables replicate data from Unity Catalog Delta tables into Postgres via managed LakeFlow Declarative Pipelines. This is the primary mechanism for reverse ETL into Lakebase.

### Sync Modes

| Mode | Behavior | Requires CDF | Latency |
|------|----------|-------------|---------|
| `SNAPSHOT` | Full table replacement each run | No | Per-run |
| `TRIGGERED` | Initial snapshot + incremental on demand | Yes | Per-trigger |
| `CONTINUOUS` | Initial snapshot + streaming incremental | Yes | ~15 seconds |

### Create a Synced Table

```python
synced = w.database.create_synced_database_table(
    SyncedDatabaseTable(
        name="my_catalog.public.customer_features",   # catalog.schema.table
        spec=SyncedTableSpec(
            source_table_full_name="analytics.gold.customer_features",
            primary_key_columns=["customer_id"],
            scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
            new_pipeline_spec=NewPipelineSpec(
                storage_catalog="analytics",
                storage_schema="gold"
            )
        )
    )
)
```

### Check Sync Status

```python
status = w.database.get_synced_database_table(
    name="my_catalog.public.customer_features"
)
print(f"Status: {status.data_synchronization_status}")
```

### Delete a Synced Table

```python
# purge_data=True also drops the Postgres table
w.database.delete_synced_database_table(
    name="my_catalog.public.customer_features",
    purge_data=True
)
```

**Sync Performance:**
- ~1,200 rows/sec/CU for continuous sync
- ~15,000 rows/sec/CU for bulk/snapshot loads
- Max 20 synced tables per source table
- Each sync pipeline uses up to 16 connections

**Documentation:**
- [Reverse ETL with Synced Tables](https://docs.databricks.com/aws/en/oltp/instances/sync-data/sync-table)

## 7. High Availability

### Configure HA

HA nodes sit in different availability zones. Readable secondaries serve read traffic through a separate DNS endpoint (`instance-ro-{uuid}`). Databricks recommends at least 3 nodes when using readable secondaries.

```python
# Enable HA at creation
instance = w.database.create_database_instance(
    DatabaseInstance(
        name="ha-instance",
        capacity="CU_2",
        node_count=3,
        enable_readable_secondaries=True
    )
)

# Enable HA on an existing instance
w.database.update_database_instance(
    name="my-instance",
    database_instance=DatabaseInstance(
        name="my-instance",
        node_count=3,
        enable_readable_secondaries=True
    ),
    update_mask="*"
)
```

**Documentation:**
- [Configure for High Availability](https://docs.databricks.com/aws/en/oltp/instances/create/high-availability)

## 8. Databricks Asset Bundles (DABs)

Lakebase Provisioned is fully supported in DABs via three resource types. Requires CLI >= 0.265.0. **Instances start billing immediately on `databricks bundle deploy`.**

### Minimal Bundle — Instance + Catalog

```yaml
bundle:
  name: lakebase-stack

resources:
  database_instances:
    my_db:
      name: my-instance
      capacity: CU_1

  database_catalogs:
    my_cat:
      database_instance_name: ${resources.database_instances.my_db.name}
      name: my_catalog
      database_name: my_database
      create_database_if_not_exists: true
```

### Full Production Bundle

```yaml
bundle:
  name: lakebase-app

variables:
  source_catalog:
    description: "Source UC catalog"
  source_schema:
    description: "Source UC schema"

resources:
  database_instances:
    my_db:
      name: my-instance
      capacity: CU_1
      retention_window_in_days: 14

  database_catalogs:
    my_cat:
      database_instance_name: ${resources.database_instances.my_db.name}
      name: my_catalog
      database_name: my_database
      create_database_if_not_exists: true

  synced_database_tables:
    features_sync:
      name: ${resources.database_catalogs.my_cat.name}.${resources.database_catalogs.my_cat.database_name}.customer_features
      database_instance_name: ${resources.database_instances.my_db.name}
      logical_database_name: ${resources.database_catalogs.my_cat.database_name}
      spec:
        source_table_full_name: '${var.source_catalog}.${var.source_schema}.customer_features'
        scheduling_policy: SNAPSHOT
        primary_key_columns:
          - customer_id
        new_pipeline_spec:
          storage_catalog: '${var.source_catalog}'
          storage_schema: '${var.source_schema}'

  jobs:
    sync_schedule:
      name: sync-pipeline-schedule
      tasks:
        - task_key: run-sync
          pipeline_task:
            pipeline_id: ${resources.synced_database_tables.features_sync.data_synchronization_status.pipeline_id}
      schedule:
        quartz_cron_expression: '0 0 * * * ?'
        timezone_id: UTC

  apps:
    my_app:
      name: my-lakebase-app
      source_code_path: ./app
      resources:
        - name: my-db
          database:
            instance_name: ${resources.database_instances.my_db.name}
            database_name: my_database
            permission: CAN_CONNECT_AND_CREATE

targets:
  dev:
    default: true
    mode: development
    variables:
      source_catalog: dev_catalog
      source_schema: dev_schema
  prod:
    variables:
      source_catalog: prod_catalog
      source_schema: prod_schema
```

### database_instances — All Fields

```yaml
resources:
  database_instances:
    <key>:
      name: <string>                         # Required
      capacity: CU_1 | CU_2 | CU_4 | CU_8   # Required
      stopped: false                          # Optional
      retention_window_in_days: 7             # Optional (2-35)
      node_count: 1                           # Optional (1-3)
      enable_readable_secondaries: false      # Optional
      enable_pg_native_login: false           # Optional
      usage_policy_id: "<policy-id>"          # Optional
      custom_tags:                            # Optional
        key: value
      purge_on_delete: true                   # Optional
      parent_instance_ref:                    # Optional (PITR clone)
        name: <parent-instance>
        branch_timestamp: "2026-01-15T10:00:00Z"
      lifecycle:                              # Optional
        deny_user_destroy: true
```

### synced_database_tables — All Fields

```yaml
resources:
  synced_database_tables:
    <key>:
      name: <catalog.schema.table>                    # Required (three-part)
      database_instance_name: <string>                # Required for standard catalogs
      logical_database_name: <string>                 # Required for standard catalogs
      spec:
        source_table_full_name: '<three.part.name>'   # Required
        scheduling_policy: SNAPSHOT | TRIGGERED | CONTINUOUS  # Required
        primary_key_columns: [col1, col2]             # Required
        timeseries_key: <column>                      # Optional (dedup column)
        create_database_objects_if_missing: true       # Optional
        new_pipeline_spec:                            # Required (or existing_pipeline_id)
          storage_catalog: '<catalog>'
          storage_schema: '<schema>'
          budget_policy_id: '<id>'                    # Optional
        existing_pipeline_id: '<id>'                  # Alternative to new_pipeline_spec
```

### Deploy Commands

```bash
databricks bundle validate            # Syntax check
databricks bundle deploy              # Create/update all resources
databricks bundle deploy -t prod      # Target-specific deploy
databricks bundle summary             # View deployed resource state
databricks bundle run sync_schedule   # Trigger a job
databricks bundle destroy             # Delete ALL bundle resources
```

**Documentation:**
- [DABs Resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
- [Bundle Examples (GitHub)](https://github.com/databricks/bundle-examples)

## 9. REST API Reference

All endpoints use base path `/api/2.0/database/` and require `Authorization: Bearer <token>`.

| Operation | Method | Endpoint |
|-----------|--------|----------|
| Create instance | POST | `/instances` |
| Get instance | GET | `/instances/{name}` |
| List instances | GET | `/instances` |
| Update instance | PATCH | `/instances/{name}` |
| Delete instance | DELETE | `/instances/{name}?purge=true` |
| Generate credential | POST | `/credentials` |
| Create catalog | POST | `/catalogs` |
| Delete catalog | DELETE | `/catalogs/{name}` |
| Create synced table | POST | `/synced_tables` |
| Get synced table | GET | `/synced_tables/{name}` |
| Delete synced table | DELETE | `/synced_tables/{name}` |

### curl Examples

```bash
# Create instance
curl -X POST -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  https://${WORKSPACE}/api/2.0/database/instances \
  -d '{"name":"my-instance","capacity":"CU_2"}'

# Stop instance
curl -X PATCH -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  https://${WORKSPACE}/api/2.0/database/instances/my-instance \
  -d '{"stopped":true}'

# Delete instance
curl -X DELETE -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  "https://${WORKSPACE}/api/2.0/database/instances/my-instance?purge=true"
```

## 10. Integration with FastAPI

### Complete Lakebase API Example

```python
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
import psycopg2
import uuid

router = APIRouter()
w = WorkspaceClient()

INSTANCE_NAME = "my-instance"

def get_connection():
    """Get a Postgres connection with fresh OAuth credentials"""
    instance = w.database.get_database_instance(name=INSTANCE_NAME)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[INSTANCE_NAME]
    )
    return psycopg2.connect(
        host=instance.read_write_dns,
        dbname="databricks_postgres",
        user="service-principal@company.com",
        password=cred.token,
        sslmode="require"
    )

class Customer(BaseModel):
    email: str
    name: str
    metadata: dict = {}

class CustomerResponse(BaseModel):
    id: int
    email: str
    name: str
    metadata: dict

@router.post("/customers", response_model=CustomerResponse)
async def create_customer(customer: Customer):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO app.customers (email, name, metadata)
                   VALUES (%s, %s, %s::jsonb) RETURNING id, email, name, metadata""",
                (customer.email, customer.name, json.dumps(customer.metadata))
            )
            row = cur.fetchone()
            conn.commit()
        conn.close()
        return CustomerResponse(id=row[0], email=row[1], name=row[2], metadata=row[3])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/customers/{customer_id}", response_model=CustomerResponse)
async def get_customer(customer_id: int):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, email, name, metadata FROM app.customers WHERE id = %s", (customer_id,))
            row = cur.fetchone()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Customer not found")
        return CustomerResponse(id=row[0], email=row[1], name=row[2], metadata=row[3])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/instance-status")
async def instance_status():
    """Check Lakebase instance health"""
    try:
        instance = w.database.get_database_instance(name=INSTANCE_NAME)
        return {
            "name": instance.name,
            "state": str(instance.state),
            "capacity": instance.capacity,
            "endpoint": instance.read_write_dns
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

## Best Practices

1. **Refresh OAuth tokens**: Tokens expire in ~1 hour — implement automatic refresh for long-lived connections with sqlalchemy
2. **Use connection pools**: Avoid creating new connections per request; use psycopg2 `ThreadedConnectionPool` or psycopg3 `ConnectionPool`
3. **Check instance state**: Verify state is `AVAILABLE` before querying
4. **Enable HA for production**: Use `node_count=3` with `enable_readable_secondaries=True`
5. **Choose the right sync mode**: Use `SNAPSHOT` when >10% of source changes; `CONTINUOUS` for real-time; `TRIGGERED` for cost-conscious incremental
6. **Use Unity Catalog registration**: Makes Postgres tables discoverable across the platform
7. **Manage permissions separately**: Workspace ACLs, UC grants, and Postgres GRANT/REVOKE are three independent layers
8. **Size capacity appropriately**: Target >99% buffer cache hit rate; resize requires restart
9. **Use DABs for production deployments**: Declarative infrastructure-as-code with target-based environments

## Resource Links

- **What is Lakebase Provisioned?**: https://docs.databricks.com/aws/en/oltp/instances/about
- **Python SDK Reference**: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/database/database.html
- **REST API Reference**: https://docs.databricks.com/api/workspace/database
- **Authentication Guide**: https://docs.databricks.com/aws/en/oltp/instances/authentication
- **PostgreSQL Compatibility**: https://docs.databricks.com/aws/en/oltp/instances/query/postgres-compatibility
- **Synced Tables**: https://docs.databricks.com/aws/en/oltp/instances/sync-data/sync-table
- **Unity Catalog Registration**: https://docs.databricks.com/aws/en/oltp/instances/register-uc
- **High Availability**: https://docs.databricks.com/aws/en/oltp/instances/create/high-availability
- **Monitoring**: https://docs.databricks.com/aws/en/oltp/instances/create/monitor
- **DABs Resources**: https://docs.databricks.com/aws/en/dev-tools/bundles/resources
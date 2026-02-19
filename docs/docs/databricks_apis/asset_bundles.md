# Databricks Asset Bundles (DABs) for CI/CD

**Official Documentation**: https://docs.databricks.com/aws/en/dev-tools/bundles  
**Bundle Configuration Reference**: https://docs.databricks.com/aws/en/dev-tools/bundles/reference  
**Bundle Resources Reference**: https://docs.databricks.com/aws/en/dev-tools/bundles/resources  
**Bundle Examples Repository**: https://github.com/databricks/bundle-examples

Databricks Asset Bundles (DABs) are an infrastructure-as-code (IaC) approach to managing Databricks projects. They allow you to define, deploy, and manage Databricks resources — jobs, pipelines, model serving endpoints, clusters, dashboards, and more — as YAML (or Python) configuration files alongside your source code. This guide covers how to set up, configure, deploy, and integrate DABs into CI/CD workflows.

## Prerequisites

```bash
# Install the Databricks CLI (v0.218.0+ required, v0.275.0+ for Python bundle support)
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify installation
databricks --version

# Configure authentication (OAuth U2M recommended)
databricks configure
```

For Python SDK interactions with deployed resources:
```toml
# pyproject.toml
databricks-sdk==0.59.0
```

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()
```

## Core Concepts

A bundle is an end-to-end definition of a Databricks project. It includes:

- **`databricks.yml`** — the root configuration file (required, exactly one per project)
- **Source files** — notebooks, Python files, SQL files containing business logic
- **Resource definitions** — YAML or Python files describing Databricks objects (jobs, pipelines, etc.)
- **Targets** — environment-specific configurations (dev, staging, prod)
- **Artifacts** — build outputs like Python wheel files
- **Variables** — parameterized values that change across targets

The lifecycle is: **init → develop → validate → deploy → run**.

## Project Structure

A typical bundle project looks like this:

```
my_project/
├── databricks.yml              # Root bundle configuration
├── resources/
│   ├── my_job.yml              # Job resource definition
│   ├── my_pipeline.yml         # Pipeline resource definition
│   └── my_serving_endpoint.yml # Model serving endpoint definition
├── src/
│   ├── notebooks/
│   │   ├── ingest.py
│   │   └── transform.py
│   ├── pipelines/
│   │   └── dlt_pipeline.py
│   └── wheel/
│       ├── setup.py
│       └── my_package/
│           └── __init__.py
├── tests/
│   ├── unit/
│   │   └── test_transform.py
│   └── integration/
│       └── test_pipeline.py
└── .github/
    └── workflows/
        ├── ci.yml              # PR validation
        └── cd.yml              # Production deployment
```

## Initialize a Bundle

```bash
# Interactive initialization from a default template
databricks bundle init

# Initialize from a specific template
databricks bundle init default-python

# The CLI will prompt you for:
#   - Project name
#   - Whether to include sample notebooks
#   - Whether to use serverless compute
#   - Default Unity Catalog catalog
#   - Whether to use personal schemas per developer
```

This generates a `databricks.yml` and a scaffolded project. You can also create `databricks.yml` manually.

## Bundle Configuration (databricks.yml)

### Minimal Configuration

```yaml
# databricks.yml
bundle:
  name: my-data-pipeline

resources:
  jobs:
    ingest-job:
      name: ingest-job
      tasks:
        - task_key: ingest-task
          existing_cluster_id: 1234-567890-abcde123
          notebook_task:
            notebook_path: ./src/notebooks/ingest.py

targets:
  dev:
    default: true
  prod:
    workspace:
      host: https://<production-workspace-url>
```

### Full Configuration with Multi-Target, Variables, and Permissions

```yaml
# databricks.yml
bundle:
  name: retail-analytics-pipeline
  databricks_cli_version: ">=0.218.0"

# Include modular resource files
include:
  - "resources/*.yml"

# Custom variables parameterized per target
variables:
  catalog:
    description: "Unity Catalog catalog name"
    default: "dev_catalog"
  schema:
    description: "Schema for pipeline output"
    default: "default"
  warehouse_id:
    description: "SQL Warehouse ID for queries"
  notification_email:
    description: "Email for job failure alerts"
    default: "data-team@company.com"

# Workspace settings (defaults, overridden per target)
workspace:
  host: https://my-dev-workspace.cloud.databricks.com

# Permissions applied to ALL resources in the bundle
permissions:
  - level: CAN_VIEW
    group_name: data-readers
  - level: CAN_MANAGE
    user_name: admin@company.com
  - level: CAN_RUN
    service_principal_name: cicd-service-principal

# Artifacts (e.g., Python wheel builds)
artifacts:
  my-python-package:
    type: whl
    build: |
      python -m pytest tests/unit/ -v
      python setup.py bdist_wheel
    path: ./src/wheel

# Target definitions
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: dev_catalog
      schema: ${workspace.current_user.short_name}_dev

  staging:
    workspace:
      host: https://my-staging-workspace.cloud.databricks.com
    variables:
      catalog: staging_catalog
      schema: staging
      warehouse_id: "abc123def456"

  prod:
    mode: production
    workspace:
      host: https://my-prod-workspace.cloud.databricks.com
      root_path: /Shared/.bundle/prod/${bundle.name}
    run_as:
      service_principal_name: prod-service-principal
    variables:
      catalog: prod_catalog
      schema: production
      warehouse_id: "xyz789ghi012"
      notification_email: prod-alerts@company.com
    presets:
      trigger_pause_status: UNPAUSED
```

## Deploying Resources

### Jobs

Jobs are the most common resource. They define Databricks Workflows with one or more tasks.

**resources/etl_job.yml:**
```yaml
resources:
  jobs:
    daily-etl:
      name: "[${bundle.target}] Daily ETL Pipeline"
      
      # Cluster definition shared across tasks
      job_clusters:
        - job_cluster_key: etl-cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "m5d.xlarge"
            num_workers: 4
            aws_attributes:
              first_on_demand: 1
              availability: SPOT_WITH_FALLBACK
              zone_id: auto
            spark_conf:
              spark.sql.shuffle.partitions: "200"
              spark.databricks.delta.optimizeWrite.enabled: "true"

      # Email notifications
      email_notifications:
        on_failure:
          - ${var.notification_email}
        no_alert_for_skipped_runs: true

      # Schedule (cron)
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "America/New_York"
        pause_status: PAUSED   # Overridden to UNPAUSED in prod via presets

      # Tasks
      tasks:
        - task_key: ingest-raw-data
          job_cluster_key: etl-cluster
          notebook_task:
            notebook_path: ./src/notebooks/ingest.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
              source: "s3://my-bucket/raw/"
          
        - task_key: transform-data
          depends_on:
            - task_key: ingest-raw-data
          job_cluster_key: etl-cluster
          notebook_task:
            notebook_path: ./src/notebooks/transform.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}

        - task_key: run-quality-checks
          depends_on:
            - task_key: transform-data
          job_cluster_key: etl-cluster
          notebook_task:
            notebook_path: ./src/notebooks/quality_checks.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}

      # Health monitoring
      health:
        rules:
          - metric: RUN_DURATION_SECONDS
            op: GREATER_THAN
            value: 7200
```

### Jobs with Python Wheel Tasks

```yaml
resources:
  jobs:
    wheel-job:
      name: "[${bundle.target}] ML Feature Engineering"
      tasks:
        - task_key: feature-engineering
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "m5d.xlarge"
            num_workers: 2
          python_wheel_task:
            package_name: my_package
            entry_point: run_features
            parameters:
              - "--catalog"
              - ${var.catalog}
              - "--env"
              - ${bundle.target}
          libraries:
            - whl: ./dist/*.whl
```

### Jobs with Serverless Compute

```yaml
resources:
  jobs:
    serverless-job:
      name: "[${bundle.target}] Serverless Processing"
      tasks:
        - task_key: process
          notebook_task:
            notebook_path: ./src/notebooks/process.py
          # Use environment_key for serverless
          environment_key: default
      
      # Define the serverless environment
      environments:
        - environment_key: default
          spec:
            client: "1"
            dependencies:
              - pandas>=2.0.0
              - scikit-learn>=1.3.0
```

### Lakeflow Declarative Pipelines (Delta Live Tables)

**resources/dlt_pipeline.yml:**
```yaml
resources:
  pipelines:
    customer-analytics-pipeline:
      name: "[${bundle.target}] Customer Analytics DLT"
      catalog: ${var.catalog}
      target: ${var.schema}
      
      # Pipeline-specific cluster config
      clusters:
        - label: default
          num_workers: 2
          node_type_id: "m5d.large"
          autoscale:
            min_workers: 1
            max_workers: 8

      # Source code for the pipeline
      libraries:
        - notebook:
            path: ./src/pipelines/dlt_pipeline.py

      # Continuous vs triggered
      continuous: false

      # Development mode (auto-set by bundle mode)
      development: true

      # Pipeline-specific configuration
      configuration:
        catalog: ${var.catalog}
        schema: ${var.schema}
        bundle.sourcePath: ${workspace.file_path}/src

      # Photon acceleration
      photon: true

      # Notifications
      notifications:
        - email_recipients:
            - ${var.notification_email}
          alerts:
            - on-update-failure
            - on-flow-failure
```

### Model Serving Endpoints

**resources/serving_endpoint.yml:**
```yaml
resources:
  model_serving_endpoints:
    fraud-detection-endpoint:
      name: "fraud-detection-${bundle.target}"
      config:
        served_entities:
          - entity_name: "${var.catalog}.ml_models.fraud_model"
            entity_version: "3"
            workload_size: "Small"
            scale_to_zero_enabled: true
        
        traffic_config:
          routes:
            - served_model_name: "fraud_model-3"
              traffic_percentage: 100

      tags:
        - key: "team"
          value: "ml-engineering"
        - key: "environment"
          value: ${bundle.target}
```

### Clusters

**resources/clusters.yml:**
```yaml
resources:
  clusters:
    shared-analytics-cluster:
      cluster_name: "[${bundle.target}] Analytics Cluster"
      spark_version: "15.4.x-scala2.12"
      node_type_id: "m5d.xlarge"
      autoscale:
        min_workers: 1
        max_workers: 10
      aws_attributes:
        first_on_demand: 1
        availability: SPOT_WITH_FALLBACK
        zone_id: auto
      spark_conf:
        spark.databricks.cluster.profile: serverless
        spark.sql.adaptive.enabled: "true"
      custom_tags:
        team: analytics
        environment: ${bundle.target}
      autotermination_minutes: 60
      data_security_mode: USER_ISOLATION
```

### Unity Catalog Resources (Schemas, Volumes, Registered Models)

**resources/catalog_resources.yml:**
```yaml
resources:
  # Unity Catalog Schema
  schemas:
    analytics_schema:
      name: analytics
      catalog_name: ${var.catalog}
      comment: "Analytics output tables"
      grants:
        - principal: data-readers
          privileges:
            - USE_SCHEMA
            - SELECT
        - principal: data-writers
          privileges:
            - USE_SCHEMA
            - CREATE_TABLE
            - MODIFY

  # Unity Catalog Volume
  volumes:
    raw_data_volume:
      name: raw_data
      catalog_name: ${var.catalog}
      schema_name: ${resources.schemas.analytics_schema.name}
      volume_type: MANAGED
      comment: "Raw data landing zone"

  # MLflow Registered Model (Unity Catalog)
  registered_models:
    fraud_model:
      name: fraud_model
      catalog_name: ${var.catalog}
      schema_name: ml_models
      comment: "Fraud detection model"
      tags:
        - key: team
          value: ml-engineering
```

### SQL Warehouses

**resources/sql_warehouse.yml:**
```yaml
resources:
  sql_warehouses:
    analytics-warehouse:
      name: "[${bundle.target}] Analytics Warehouse"
      cluster_size: "Medium"
      enable_serverless_compute: true
      max_num_clusters: 3
      min_num_clusters: 1
      auto_stop_mins: 30
      warehouse_type: PRO
```

### SQL Alerts

**resources/alerts.yml:**
```yaml
resources:
  alerts:
    data-freshness-alert:
      display_name: "[${bundle.target}] Data Freshness Alert"
      query_text: |
        SELECT TIMESTAMPDIFF(HOUR, MAX(updated_at), CURRENT_TIMESTAMP()) AS hours_since_update
        FROM ${var.catalog}.${var.schema}.orders
      warehouse_id: ${var.warehouse_id}
      evaluation:
        comparison_operator: GREATER_THAN
        source:
          name: hours_since_update
        threshold:
          value:
            double_value: 6
        notification:
          notify_on_ok: true
          retrigger_seconds: 3600
          subscriptions:
            - user_email: ${var.notification_email}
      schedule:
        quartz_cron_schedule: "0 */30 * * * ?"
        timezone_id: "America/New_York"
```

### MLflow Experiments

**resources/experiments.yml:**
```yaml
resources:
  experiments:
    fraud-experiment:
      name: /Users/${workspace.current_user.userName}/fraud-detection-experiment
      permissions:
        - level: CAN_MANAGE
          group_name: ml-engineering
```

### Databricks Apps

**resources/app.yml:**
```yaml
resources:
  apps:
    dashboard-app:
      name: "analytics-dashboard-${bundle.target}"
      description: "Interactive analytics dashboard"
      source_code_path: ./src/app
      
      config:
        command:
          - "streamlit"
          - "run"
          - "app.py"
        env:
          - name: DATABRICKS_CATALOG
            value: ${var.catalog}
          - name: DATABRICKS_SCHEMA
            value: ${var.schema}

      resources:
        - name: analytics-job
          job:
            id: ${resources.jobs.daily-etl.id}
            permission: RUN
        - name: analytics-warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: USE
```

### Secret Scopes

**resources/secrets.yml:**
```yaml
resources:
  secret_scopes:
    pipeline-secrets:
      name: "pipeline-secrets-${bundle.target}"
      initial_manage_principal: users
```

## Bundle Substitutions & Variables

DABs support dynamic substitutions throughout configuration:

```yaml
# Built-in substitutions
${bundle.name}                          # Bundle name
${bundle.target}                        # Current target name (dev, staging, prod)
${workspace.host}                       # Workspace URL
${workspace.current_user.userName}      # Deploying user's email
${workspace.current_user.short_name}    # Username portion of email
${workspace.file_path}                  # Deployed file path in workspace

# Resource cross-references
${resources.jobs.daily-etl.id}          # Job ID after deployment
${resources.pipelines.my-pipeline.id}   # Pipeline ID after deployment
${resources.schemas.my-schema.name}     # Schema name

# Custom variables
${var.catalog}                          # Value from variables config
${var.schema}
```

## Deployment Modes

### Development Mode

```yaml
targets:
  dev:
    mode: development
```

Development mode automatically:
- Prefixes resource names with `[dev ${user}]`
- Pauses all schedules and triggers
- Enables concurrent runs for faster iteration
- Enables `--compute-id` override on deploy
- Disables deployment locking
- Sets pipelines to `development: true`

### Production Mode

```yaml
targets:
  prod:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com
      root_path: /Shared/.bundle/prod/${bundle.name}
    run_as:
      service_principal_name: prod-sp
```

Production mode:
- Validates the current Git branch matches the target's configured branch
- Validates that paths aren't user-specific
- Requires `run_as` for service principal execution
- Locks cluster override

### Custom Presets

```yaml
targets:
  staging:
    presets:
      name_prefix: "[staging] "
      trigger_pause_status: PAUSED
      jobs_max_concurrent_runs: 1
      tags:
        environment: staging
```

## CLI Commands for Bundle Lifecycle

```bash
# Validate bundle configuration (catches errors before deploy)
databricks bundle validate
databricks bundle validate -t prod

# Deploy the bundle to a target workspace
databricks bundle deploy
databricks bundle deploy -t staging
databricks bundle deploy -t prod --auto-approve

# Run a specific resource
databricks bundle run daily-etl
databricks bundle run daily-etl -t prod
databricks bundle run customer-analytics-pipeline --refresh-all

# View deployed bundle summary
databricks bundle summary
databricks bundle summary -t prod

# Generate YAML config from an existing workspace resource
databricks bundle generate job --existing-job-id 123456789
databricks bundle generate pipeline --existing-pipeline-id abc-def-123

# Bind a bundle resource to an existing workspace resource
databricks bundle deployment bind daily-etl 123456789

# Destroy all deployed resources for a target
databricks bundle destroy
databricks bundle destroy -t staging
```

## Python SDK Integration

While DABs are deployed via the CLI, you can use the Python SDK to interact with deployed resources programmatically — listing jobs, triggering runs, checking status, querying endpoints, etc.

### List and Inspect Deployed Jobs

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

client = WorkspaceClient()

def list_bundle_jobs(bundle_prefix: str = "[prod]"):
    """List jobs deployed by a specific bundle target."""
    try:
        jobs = client.jobs.list()
        bundle_jobs = []
        for job in jobs:
            if job.settings and job.settings.name and bundle_prefix in job.settings.name:
                bundle_jobs.append({
                    "job_id": job.job_id,
                    "name": job.settings.name,
                    "creator": job.creator_user_name,
                    "created_time": job.created_time,
                })
                print(f"Job: {job.settings.name} (ID: {job.job_id})")
        return bundle_jobs
    except DatabricksError as e:
        print(f"Error listing jobs: {e}")
        return []

# Usage
prod_jobs = list_bundle_jobs("[prod]")
```

### Trigger a Deployed Job Run

```python
import time

def trigger_job_run(job_id: int, parameters: dict = None, wait: bool = True):
    """Trigger a job run and optionally wait for completion."""
    try:
        # Start the run
        run = client.jobs.run_now(
            job_id=job_id,
            notebook_params=parameters or {}
        )
        print(f"Started run {run.run_id} for job {job_id}")

        if wait:
            # Poll for completion
            while True:
                run_status = client.jobs.get_run(run.run_id)
                life_cycle_state = run_status.state.life_cycle_state.value
                print(f"Run {run.run_id}: {life_cycle_state}")

                if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                    result_state = run_status.state.result_state
                    print(f"Final state: {result_state}")
                    return {
                        "run_id": run.run_id,
                        "state": result_state.value if result_state else "UNKNOWN",
                        "duration_ms": run_status.run_duration,
                    }
                time.sleep(30)
        
        return {"run_id": run.run_id, "state": "RUNNING"}

    except DatabricksError as e:
        print(f"Error triggering job: {e}")
        return None

# Usage
result = trigger_job_run(
    job_id=123456789,
    parameters={"catalog": "prod_catalog", "date": "2025-01-15"}
)
```

### Monitor Pipeline Updates

```python
def get_pipeline_status(pipeline_id: str):
    """Check the status of a deployed DLT pipeline."""
    try:
        pipeline = client.pipelines.get(pipeline_id)
        
        print(f"Pipeline: {pipeline.name}")
        print(f"State: {pipeline.state}")
        print(f"Catalog: {pipeline.spec.catalog}")
        print(f"Target Schema: {pipeline.spec.target}")

        if pipeline.latest_updates:
            latest = pipeline.latest_updates[0]
            print(f"Latest Update: {latest.update_id}")
            print(f"  State: {latest.state}")
            print(f"  Creation Time: {latest.creation_time}")
        
        return pipeline

    except DatabricksError as e:
        print(f"Error getting pipeline status: {e}")
        return None

def trigger_pipeline_update(pipeline_id: str, full_refresh: bool = False):
    """Trigger a pipeline update."""
    try:
        response = client.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh=full_refresh
        )
        print(f"Started pipeline update: {response.update_id}")
        return response.update_id
    except DatabricksError as e:
        print(f"Error triggering pipeline: {e}")
        return None

# Usage
get_pipeline_status("abc-def-123-456")
trigger_pipeline_update("abc-def-123-456", full_refresh=True)
```

### Query a Deployed Model Serving Endpoint

```python
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

def query_deployed_endpoint(endpoint_name: str, input_data: dict):
    """Query a model serving endpoint deployed via DABs."""
    try:
        response = client.serving_endpoints.query(
            name=endpoint_name,
            inputs=input_data
        )
        return response
    except DatabricksError as e:
        print(f"Error querying endpoint: {e}")
        return None

def query_chat_endpoint(endpoint_name: str, message: str):
    """Query a chat model endpoint deployed via DABs."""
    try:
        messages = [
            ChatMessage(role=ChatMessageRole.USER, content=message)
        ]
        response = client.serving_endpoints.query(
            name=endpoint_name,
            messages=messages,
            max_tokens=200,
            temperature=0.7
        )
        if response.choices:
            return response.choices[0].message.content
        return None
    except DatabricksError as e:
        print(f"Error querying chat endpoint: {e}")
        return None

# Usage
predictions = query_deployed_endpoint(
    "fraud-detection-prod",
    {"inputs": [[100.0, 0.5, 1.2, 0.8]]}
)
```

### Programmatic Bundle Deployment via SDK (subprocess)

```python
import subprocess
import json

def deploy_bundle(target: str, project_path: str, auto_approve: bool = False):
    """Deploy a bundle using the Databricks CLI from Python."""
    cmd = ["databricks", "bundle", "deploy", "-t", target]
    if auto_approve:
        cmd.append("--auto-approve")
    
    result = subprocess.run(
        cmd,
        cwd=project_path,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"Successfully deployed to {target}")
        print(result.stdout)
        return True
    else:
        print(f"Deployment failed: {result.stderr}")
        return False

def validate_bundle(project_path: str, target: str = None):
    """Validate bundle configuration."""
    cmd = ["databricks", "bundle", "validate"]
    if target:
        cmd.extend(["-t", target])
    
    result = subprocess.run(
        cmd,
        cwd=project_path,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("Bundle validation passed")
        return True
    else:
        print(f"Validation failed: {result.stderr}")
        return False

def get_bundle_summary(project_path: str, target: str = None):
    """Get summary of deployed bundle resources."""
    cmd = ["databricks", "bundle", "summary", "--output", "json"]
    if target:
        cmd.extend(["-t", target])
    
    result = subprocess.run(
        cmd,
        cwd=project_path,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        return json.loads(result.stdout)
    return None

# Usage
validate_bundle("/path/to/my-project", target="prod")
deploy_bundle("prod", "/path/to/my-project", auto_approve=True)
summary = get_bundle_summary("/path/to/my-project", target="prod")
```

## Python-Defined Resources (PyDABs)

As of CLI v0.275.0+, you can define bundle resources in Python rather than YAML using the `databricks-bundles` package.

```bash
# Install uv (required for PyDABs)
pip install uv

# Initialize a PyDABs project
databricks bundle init pydabs
```

**resources/etl_job.py:**
```python
from databricks.bundles.jobs import Job, Task, NotebookTask, JobCluster, NewCluster

etl_job = Job(
    name="daily-etl-pipeline",
    job_clusters=[
        JobCluster(
            job_cluster_key="etl-cluster",
            new_cluster=NewCluster(
                spark_version="15.4.x-scala2.12",
                node_type_id="m5d.xlarge",
                num_workers=4,
            ),
        )
    ],
    tasks=[
        Task(
            task_key="ingest",
            job_cluster_key="etl-cluster",
            notebook_task=NotebookTask(
                notebook_path="./src/notebooks/ingest.py",
            ),
        ),
        Task(
            task_key="transform",
            depends_on=[{"task_key": "ingest"}],
            job_cluster_key="etl-cluster",
            notebook_task=NotebookTask(
                notebook_path="./src/notebooks/transform.py",
            ),
        ),
    ],
)
```

**resources/__init__.py:**
```python
from databricks.bundles.core import Resources, load_resources_from_current_package_module

def load_resources() -> Resources:
    """Called by Databricks CLI during bundle deployment."""
    return load_resources_from_current_package_module()
```

**Dynamic resource creation:**
```python
import yaml
from databricks.bundles.jobs import Job, Task, NotebookTask

def load_resources():
    """Dynamically create jobs from a config file."""
    with open("config/pipelines.yml") as f:
        config = yaml.safe_load(f)
    
    resources = {}
    for pipeline in config["pipelines"]:
        job = Job(
            name=f"{pipeline['name']}-job",
            tasks=[
                Task(
                    task_key=task["key"],
                    notebook_task=NotebookTask(notebook_path=task["notebook"]),
                )
                for task in pipeline["tasks"]
            ],
        )
        resources[f"{pipeline['name']}_job"] = job
    
    return Resources(jobs=resources)
```

## CI/CD with GitHub Actions

### QA Deployment (Pull Requests)

**.github/workflows/ci.yml:**
```yaml
name: "QA Deployment"

concurrency: 1

on:
  pull_request:
    types:
      - opened
      - synchronize
    branches:
      - main

jobs:
  validate:
    name: "Validate bundle"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: databricks/setup-cli@main

      - name: Validate bundle configuration
        run: databricks bundle validate -t staging
        env:
          DATABRICKS_TOKEN: ${{ secrets.STAGING_SP_TOKEN }}
          DATABRICKS_HOST: ${{ secrets.STAGING_WORKSPACE_HOST }}

  test:
    name: "Run unit tests"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install -r requirements-dev.txt
          pip install pytest databricks-sdk

      - name: Run unit tests
        run: python -m pytest tests/unit/ -v

  deploy-staging:
    name: "Deploy to staging"
    runs-on: ubuntu-latest
    needs: [validate, test]
    steps:
      - uses: actions/checkout@v4

      - uses: databricks/setup-cli@main

      - name: Deploy bundle to staging
        run: databricks bundle deploy -t staging
        env:
          DATABRICKS_TOKEN: ${{ secrets.STAGING_SP_TOKEN }}
          DATABRICKS_HOST: ${{ secrets.STAGING_WORKSPACE_HOST }}

      - name: Run integration job
        run: databricks bundle run daily-etl -t staging
        env:
          DATABRICKS_TOKEN: ${{ secrets.STAGING_SP_TOKEN }}
          DATABRICKS_HOST: ${{ secrets.STAGING_WORKSPACE_HOST }}
```

### Production Deployment (Merge to Main)

**.github/workflows/cd.yml:**
```yaml
name: "Production Deployment"

concurrency: 1

on:
  push:
    branches:
      - main

jobs:
  deploy-prod:
    name: "Deploy to production"
    runs-on: ubuntu-latest
    environment: production    # Requires GitHub environment approval

    steps:
      - uses: actions/checkout@v4

      - uses: databricks/setup-cli@main

      - name: Validate bundle
        run: databricks bundle validate -t prod
        env:
          DATABRICKS_TOKEN: ${{ secrets.PROD_SP_TOKEN }}
          DATABRICKS_HOST: ${{ secrets.PROD_WORKSPACE_HOST }}

      - name: Deploy bundle to production
        run: databricks bundle deploy -t prod --auto-approve
        env:
          DATABRICKS_TOKEN: ${{ secrets.PROD_SP_TOKEN }}
          DATABRICKS_HOST: ${{ secrets.PROD_WORKSPACE_HOST }}
```

### Using OAuth Instead of PAT

For enhanced security, use OAuth service principal credentials:

```yaml
# In GitHub Actions
- name: Deploy bundle
  run: databricks bundle deploy -t prod
  env:
    DATABRICKS_HOST: ${{ secrets.PROD_WORKSPACE_HOST }}
    DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}
    DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
```

## Service Principal Setup (AWS)

For CI/CD, you need a service principal that can deploy and run resources.

```python
# Create and configure a service principal via the SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ServicePrincipalCreate

client = WorkspaceClient()

# List existing service principals
for sp in client.service_principals.list():
    print(f"SP: {sp.display_name} (App ID: {sp.application_id})")

# The service principal needs:
# 1. Workspace access (added in the Databricks admin console)
# 2. "Allow cluster creation" entitlement (for job clusters)
# 3. Appropriate Unity Catalog grants (for catalog/schema resources)
# 4. CAN_MANAGE permissions on any existing resources it needs to modify
```

In the Databricks workspace UI:
1. Navigate to **Settings → Identity and Access → Service Principals**
2. Add the service principal and enable "Allow cluster creation"
3. Under **Secrets**, generate an OAuth secret
4. Store the client ID and secret in your CI/CD system (GitHub Secrets, etc.)

## Generating Config from Existing Resources

If you have existing jobs or pipelines in a workspace, you can generate bundle YAML from them:

```bash
# Generate YAML for an existing job
databricks bundle generate job --existing-job-id 123456789

# Generate YAML for an existing pipeline
databricks bundle generate pipeline --existing-pipeline-id abc-def-123

# Generate YAML for an existing dashboard
databricks bundle generate dashboard --existing-dashboard-id abc123

# After generating, bind the resource so future deploys update the existing resource
# rather than creating a new one
databricks bundle deployment bind daily-etl 123456789
```

This is extremely useful for migrating existing workspace resources into DABs management without recreating them.

## Error Handling & Troubleshooting

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, BadRequest

client = WorkspaceClient()

def safe_get_job(job_id: int):
    """Safely retrieve a job with proper error handling."""
    try:
        job = client.jobs.get(job_id)
        return {
            "name": job.settings.name,
            "state": "found",
            "creator": job.creator_user_name,
        }
    except NotFound:
        return {"error": f"Job {job_id} not found — may not be deployed yet"}
    except PermissionDenied:
        return {"error": "Permission denied — check service principal permissions"}
    except BadRequest as e:
        return {"error": f"Bad request: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error: {e}"}

# Common CLI troubleshooting commands
# databricks bundle validate          # Check config syntax
# databricks bundle validate -t prod  # Check prod-specific config
# databricks bundle summary           # See what's deployed
# databricks bundle destroy           # Remove all deployed resources
```

## Best Practices

1. **Use `mode: development` for dev targets** — it auto-prefixes names and pauses schedules, preventing accidental production interference.
2. **Use `mode: production` for prod targets** — it enforces Git branch validation and disables cluster overrides.
3. **Always use `run_as` with a service principal in production** — separates the deployer identity from the runtime identity.
4. **Split resource definitions into separate files** — use `include: ["resources/*.yml"]` for modularity.
5. **Use variables for environment-specific values** — catalogs, schemas, warehouse IDs, and notification emails should be parameterized.
6. **Store secrets in GitHub Secrets** (or your CI/CD tool) — never commit tokens or credentials.
7. **Validate before deploying** — always run `databricks bundle validate` in CI before `deploy`.
8. **Use `databricks bundle generate`** — to migrate existing resources into DABs rather than rewriting from scratch.
9. **Pin your CLI version** — use `databricks_cli_version: ">=0.218.0"` in `databricks.yml` to ensure compatibility.
10. **Use concurrency controls in CI/CD** — prevent parallel deployments to the same target.

## Resource Links

- **What are Databricks Asset Bundles**: https://docs.databricks.com/aws/en/dev-tools/bundles
- **Bundle Configuration Reference**: https://docs.databricks.com/aws/en/dev-tools/bundles/reference
- **Bundle Resources Reference**: https://docs.databricks.com/aws/en/dev-tools/bundles/resources
- **Bundle Configuration Examples**: https://docs.databricks.com/aws/en/dev-tools/bundles/examples
- **CI/CD with Bundles**: https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd-bundles
- **GitHub Actions Integration**: https://docs.databricks.com/aws/en/dev-tools/ci-cd/github
- **Deployment Modes**: https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes
- **Bundle Templates**: https://docs.databricks.com/aws/en/dev-tools/bundles/templates
- **Python Bundle Support (PyDABs)**: https://docs.databricks.com/aws/en/dev-tools/bundles/python
- **databricks-bundles PyPI Package**: https://pypi.org/project/databricks-bundles/
- **Bundle Examples GitHub Repo**: https://github.com/databricks/bundle-examples
- **Databricks CLI Bundle Commands**: https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- **Databricks REST API Reference**: https://docs.databricks.com/api/workspace/introduction
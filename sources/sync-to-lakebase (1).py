# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Cell 1
# ============================================================================
# LAKEBASE PROVISIONED TABLE SYNC
# ============================================================================
# This notebook creates synced tables from gold materialized views to
# Lakebase provisioned tables for low-latency serving to applications.
#
# Architecture:
# - Source: Gold MVs in hk_catalog
# - Target: Lakebase synced tables in lakebase_marketplace.gold
# - Sync Mode: SNAPSHOT (materialized views don't have Change Data Feed)
# - Primary Keys: Composite keys based on table grain
# ============================================================================

# Check if databricks SDK is available and has the required modules
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import database
    
    # Access the classes from the database module
    DatabaseInstance = database.DatabaseInstance
    DatabaseCatalog = database.DatabaseCatalog
    SyncedDatabaseTable = database.SyncedDatabaseTable
    SyncedTableSpec = database.SyncedTableSpec
    NewPipelineSpec = database.NewPipelineSpec
    SyncedTableSchedulingPolicy = database.SyncedTableSchedulingPolicy
    
    SDK_AVAILABLE = True
    print("✓ Databricks SDK database module loaded successfully")
except (ImportError, AttributeError) as e:
    SDK_AVAILABLE = False
    print("✗ Databricks SDK database module not available.")
    print(f"  Error: {str(e)}")
    print("\nDiagnostic info:")
    try:
        import databricks.sdk
        print(f"  SDK version: {databricks.sdk.__version__}")
        from databricks.sdk import service
        print(f"  Available services: {dir(service)}")
    except Exception as diag_e:
        print(f"  Could not get diagnostic info: {diag_e}")

if not SDK_AVAILABLE:
    raise ImportError("Databricks SDK database module is required. The SDK may not support synced tables in this environment.")

# Initialize Databricks Workspace Client
w = WorkspaceClient()

# ============================================================================
# CONFIGURATION
# ============================================================================

# Source configuration (pipeline output)
SOURCE_CATALOG = "hk_catalog"
SOURCE_SCHEMA = "final"

# Target configuration (Lakebase)
LAKEBASE_CATALOG = "lakebase_marketplace_v2"
LAKEBASE_SCHEMA = "gold"
LAKEBASE_INSTANCE_NAME = "marketplace-intel-db-v2"
LAKEBASE_DATABASE_NAME = "databricks_postgres-v2"  # Default Postgres database name
LAKEBASE_CAPACITY = "CU_1"  # Start with smallest capacity

# Storage configuration for sync pipeline metadata
STORAGE_CATALOG = "hk_catalog"
STORAGE_SCHEMA = "wanderbricks_lakebase"

# ============================================================================
# TABLE DEFINITIONS WITH PRIMARY KEYS
# ============================================================================

SYNC_TABLE_CONFIGS = [
    {
        "source_table": "gold_city_funnel",
        "target_table": "city_funnel",
        "primary_keys": ["city", "event_date"],
        "description": "Daily conversion funnel metrics per city"
    },
    {
        "source_table": "gold_amenity_city_top",
        "target_table": "amenity_city_top",
        "primary_keys": ["city", "amenity_name"],
        "description": "Top 3 amenities by confirmation lift per city and property type"
    },
    {
        "source_table": "gold_amenity_lift",
        "target_table": "amenity_lift",
        "primary_keys": ["amenity_id", "city", "property_type"],
        "description": "Amenity conversion lift analysis by city and property type"
    },
    {
        "source_table": "gold_device_funnel",
        "target_table": "device_funnel",
        "primary_keys": ["city", "device", "week_start"],
        "description": "Weekly device-specific conversion funnel by city"
    },
    {
        "source_table": "gold_city_investment",
        "target_table": "city_investment",
        "primary_keys": ["city"],
        "description": "City-level investment priority with quadrant classification"
    },
    {
        "source_table": "gold_property_performance",
        "target_table": "property_performance",
        "primary_keys": ["property_id"],
        "description": "Property performance metrics with conversion funnel"
    },
    {
        "source_table": "gold_device_diagnosis",
        "target_table": "device_diagnosis",
        "primary_keys": ["city"],
        "description": "Device conversion gap diagnosis and mobile trend analysis"
    }
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_database_instance_if_not_exists(instance_name: str, capacity: str):
    """Create Lakebase database instance if it doesn't exist."""
    try:
        # Check if instance already exists
        try:
            existing_instance = w.database.get_database_instance(name=instance_name)
            print(f"✓ Database instance '{instance_name}' already exists")
            if hasattr(existing_instance, 'read_write_dns'):
                print(f"  Connection endpoint: {existing_instance.read_write_dns}")
            return existing_instance
        except Exception:
            # Instance doesn't exist, create it
            pass
        
        print(f"Creating database instance '{instance_name}' with capacity {capacity}...")
        wait_obj = w.database.create_database_instance(
            DatabaseInstance(
                name=instance_name,
                capacity=capacity
            )
        )
        
        # Wait for the instance to be fully provisioned
        print(f"  Waiting for instance to be provisioned...")
        instance = wait_obj.result()
        
        print(f"✓ Database instance '{instance_name}' created successfully")
        if hasattr(instance, 'read_write_dns'):
            print(f"  Connection endpoint: {instance.read_write_dns}")
        return instance
        
    except Exception as e:
        print(f"✗ Error creating database instance '{instance_name}': {str(e)}")
        raise

def create_database_catalog_if_not_exists(catalog_name: str, instance_name: str, database_name: str):
    """Create database catalog (Unity Catalog backed by Lakebase) if it doesn't exist."""
    try:
        # Check if catalog already exists
        try:
            existing_catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
            if catalog_name in existing_catalogs:
                print(f"✓ Database catalog '{catalog_name}' already exists")
                return
        except Exception:
            pass
        
        print(f"Creating database catalog '{catalog_name}' linked to instance '{instance_name}'...")
        catalog = w.database.create_database_catalog(
            DatabaseCatalog(
                name=catalog_name,
                database_instance_name=instance_name,
                database_name=database_name,
                create_database_if_not_exists=True
            )
        )
        
        print(f"✓ Database catalog '{catalog_name}' created successfully")
        return catalog
        
    except Exception as e:
        print(f"✗ Error creating database catalog '{catalog_name}': {str(e)}")
        raise

def create_synced_table(config: dict, instance_name: str):
    """
    Create a synced table from a gold materialized view to Lakebase.
    
    Args:
        config: Dictionary with source_table, target_table, primary_keys, description
        instance_name: Name of the Lakebase database instance
    """
    source_full_name = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{config['source_table']}"
    target_full_name = f"{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}.{config['target_table']}"
    
    try:
        print(f"\n{'='*80}")
        print(f"Creating synced table: {target_full_name}")
        print(f"  Source: {source_full_name}")
        print(f"  Primary Keys: {', '.join(config['primary_keys'])}")
        print(f"  Description: {config['description']}")
        print(f"{'='*80}")
        
        # Create the synced table
        # Note: create_database_objects_if_missing=True will automatically create the schema in PostgreSQL
        synced_table = w.database.create_synced_database_table(
            SyncedDatabaseTable(
                name=target_full_name,
                database_instance_name=instance_name,
                logical_database_name=LAKEBASE_DATABASE_NAME,
                spec=SyncedTableSpec(
                    source_table_full_name=source_full_name,
                    primary_key_columns=config['primary_keys'],
                    scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                    create_database_objects_if_missing=True,
                    new_pipeline_spec=NewPipelineSpec(
                        storage_catalog=STORAGE_CATALOG,
                        storage_schema=STORAGE_SCHEMA
                    )
                )
            )
        )
        
        print(f"✓ Successfully created synced table: {synced_table.name}")
        
        # Check initial status
        status = w.database.get_synced_database_table(name=target_full_name)
        if hasattr(status, 'data_synchronization_status'):
            print(f"  Status: {status.data_synchronization_status.detailed_state}")
            if hasattr(status.data_synchronization_status, 'message') and status.data_synchronization_status.message:
                print(f"  Message: {status.data_synchronization_status.message}")
        
        return synced_table
        
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            print(f"⚠ Synced table already exists: {target_full_name}")
            print(f"  Skipping creation. To recreate, manually drop the table first.")
        else:
            print(f"✗ Error creating synced table '{target_full_name}': {error_msg}")
            raise

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to orchestrate the sync process."""
    
    print("\n" + "="*80)
    print("LAKEBASE SYNC INITIALIZATION")
    print("="*80)
    
    # Step 1: Create Lakebase database instance
    print("\n[1/4] Creating Lakebase database instance...")
    instance = create_database_instance_if_not_exists(LAKEBASE_INSTANCE_NAME, LAKEBASE_CAPACITY)
    
    # Step 2: Create database catalog (Unity Catalog backed by Lakebase)
    print("\n[2/4] Creating database catalog...")
    create_database_catalog_if_not_exists(LAKEBASE_CATALOG, LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE_NAME)
    
    # Step 3: Create storage schema for sync pipeline metadata (regular Unity Catalog)
    print("\n[3/4] Setting up storage schema for sync pipeline metadata...")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STORAGE_CATALOG}.{STORAGE_SCHEMA}")
        print(f"✓ Storage schema '{STORAGE_CATALOG}.{STORAGE_SCHEMA}' ready")
    except Exception as e:
        print(f"⚠ Storage schema note: {str(e)}")
    
    # Step 4: Create synced tables
    # Note: The 'gold' schema in Lakebase catalog will be created automatically
    # by the first synced table via create_database_objects_if_missing=True
    print(f"\n[4/4] Creating synced tables...")
    print(f"  (Schema '{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}' will be auto-created in PostgreSQL)")
    print(f"Total tables to sync: {len(SYNC_TABLE_CONFIGS)}")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for config in SYNC_TABLE_CONFIGS:
        try:
            create_synced_table(config, LAKEBASE_INSTANCE_NAME)
            success_count += 1
        except Exception as e:
            if "already exists" in str(e).lower():
                skip_count += 1
            else:
                error_count += 1
    
    # Summary
    print("\n" + "="*80)
    print("SYNC SUMMARY")
    print("="*80)
    print(f"✓ Successfully created: {success_count}")
    print(f"⚠ Skipped (already exists): {skip_count}")
    print(f"✗ Errors: {error_count}")
    print(f"Total: {len(SYNC_TABLE_CONFIGS)}")
    
    if error_count == 0:
        print("\n✓ All synced tables are ready!")
        print(f"\nQuery your synced tables in: {LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}")
        print(f"Database instance: {LAKEBASE_INSTANCE_NAME}")
        if instance and hasattr(instance, 'read_write_dns'):
            print(f"Connection endpoint: {instance.read_write_dns}")
    else:
        print("\n⚠ Some tables failed to sync. Check errors above.")

# Execute the main function
main()
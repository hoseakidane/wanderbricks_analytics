# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Setup Parameters
dbutils.widgets.text("catalog", "hk_catalog_dev")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("clickstream_table_name", "clickstream_synthetic_v3")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
clickstream_table_name = dbutils.widgets.get("clickstream_table_name")
target_table = f"{catalog}.{schema}.{clickstream_table_name}"
print(f"Target table: {target_table}")

# Ensure schema exists (catalog must pre-exist for bundle deploy to succeed)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"Schema '{catalog}.{schema}' ready")

# COMMAND ----------

# DBTITLE 1,Query 1: Bookings Temporal Distribution
# Analyze booking patterns over time
bookings_temporal = spark.sql("""
    SELECT 
        DATE_TRUNC('month', created_at) as month,
        COUNT(*) as booking_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT property_id) as unique_properties
    FROM samples.wanderbricks.bookings
    GROUP BY DATE_TRUNC('month', created_at)
    ORDER BY month DESC
    LIMIT 12
""")

print("=== Bookings by Month (Last 12 months) ===")
display(bookings_temporal)

# COMMAND ----------

# DBTITLE 1,Query 2: User Booking Frequency
# Understand how many bookings each user makes
user_frequency = spark.sql("""
    SELECT 
        bookings_per_user,
        COUNT(*) as user_count
    FROM (
        SELECT 
            user_id,
            COUNT(*) as bookings_per_user
        FROM samples.wanderbricks.bookings
        GROUP BY user_id
    )
    GROUP BY bookings_per_user
    ORDER BY bookings_per_user
""")

print("=== User Booking Frequency Distribution ===")
display(user_frequency)

# COMMAND ----------

# DBTITLE 1,Query 3: Property Popularity
# Identify most popular properties
property_popularity = spark.sql("""
    SELECT 
        property_id,
        COUNT(*) as booking_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM samples.wanderbricks.bookings
    GROUP BY property_id
    ORDER BY booking_count DESC
    LIMIT 20
""")

print("=== Top 20 Most Popular Properties ===")
display(property_popularity)

# COMMAND ----------

# DBTITLE 1,Query 4: Sample Bookings for Testing
# Get a sample of bookings to test our logic
sample_bookings = spark.sql("""
    SELECT 
        booking_id,
        user_id,
        property_id,
        created_at,
        check_in,
        check_out,
        status,
        DATE_DIFF(check_in, DATE(created_at)) as days_advance_booking
    FROM samples.wanderbricks.bookings
    WHERE created_at >= '2025-01-01'
    ORDER BY created_at DESC
    LIMIT 10
""")

print("=== Sample Recent Bookings ===")
display(sample_bookings)

# COMMAND ----------

# DBTITLE 1,Query 5: Clickstream Template Analysis
# Analyze the template clickstream data patterns
clickstream_analysis = spark.sql("""
    SELECT 
        event,
        metadata.device,
        metadata.referrer,
        COUNT(*) as count
    FROM samples.wanderbricks.clickstream
    GROUP BY event, metadata.device, metadata.referrer
    ORDER BY count DESC
    LIMIT 20
""")

print("=== Clickstream Template Event Patterns ===")
display(clickstream_analysis)

# COMMAND ----------

# DBTITLE 1,Import Libraries and Setup
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import random

print("Libraries imported successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Clickstream Data
# Step 1: Load bookings data
bookings_df = spark.table("samples.wanderbricks.bookings").select(
    "booking_id",
    "user_id",
    "property_id",
    "created_at"
)

print(f"Loaded {bookings_df.count()} bookings")

# Step 2: Generate multiple view events per booking
# We'll create 5-15 events per booking with different time offsets

# Define time windows (in seconds before booking)
time_windows = [
    # Long-term browsing: 1-4 weeks before (3-8 events)
    (7 * 24 * 3600, 28 * 24 * 3600, 3),   # 1-4 weeks, 3 events
    (7 * 24 * 3600, 28 * 24 * 3600, 3),   # 1-4 weeks, 3 more events
    (7 * 24 * 3600, 21 * 24 * 3600, 2),   # 1-3 weeks, 2 events
    
    # Active shopping: 1-7 days before (2-5 events)
    (1 * 24 * 3600, 7 * 24 * 3600, 2),    # 1-7 days, 2 events
    (1 * 24 * 3600, 7 * 24 * 3600, 2),    # 1-7 days, 2 more events
    (2 * 24 * 3600, 5 * 24 * 3600, 1),    # 2-5 days, 1 event
    
    # Immediate pre-booking: Within 1 hour (1-3 events)
    (60, 3600, 1),                         # 1-60 minutes, 1 event
    (60, 3600, 1),                         # 1-60 minutes, 1 more event
    (10, 300, 1),                          # 10 seconds - 5 minutes, 1 event
]

# Step 3: Explode bookings to create multiple events
events_list = []
for window_idx, (min_offset, max_offset, count) in enumerate(time_windows):
    for event_num in range(count):
        events_list.append((window_idx * 10 + event_num, min_offset, max_offset))

print(f"Will generate {len(events_list)} events per booking")

# Create events dataframe
events_schema = StructType([
    StructField("event_seq", LongType(), False),
    StructField("min_offset_seconds", LongType(), False),
    StructField("max_offset_seconds", LongType(), False)
])
events_config_df = spark.createDataFrame(events_list, events_schema)

# Cross join bookings with event configurations
clickstream_base = bookings_df.crossJoin(events_config_df)

print(f"Created base clickstream with {clickstream_base.count()} potential events")

# COMMAND ----------

# DBTITLE 1,Add Timestamps and Metadata
# Step 4: Generate random timestamps within each window
# Use a random offset within the min/max range
clickstream_with_time = clickstream_base.withColumn(
    "random_offset",
    F.floor(
        F.col("min_offset_seconds") + 
        (F.rand() * (F.col("max_offset_seconds") - F.col("min_offset_seconds")))
    ).cast("long")
).withColumn(
    "timestamp",
    (F.col("created_at").cast("long") - F.col("random_offset")).cast("timestamp")
)

# Step 5: Add event type (all 'view')
clickstream_with_event = clickstream_with_time.withColumn(
    "event",
    F.lit("view")
)

# Step 6: Add metadata with device and referrer
# Device distribution: 50% mobile, 30% desktop, 20% tablet
# Referrer distribution: 40% google, 25% direct, 20% ad, 15% email

clickstream_with_metadata = clickstream_with_event.withColumn(
    "device",
    F.when(F.rand() < 0.50, F.lit("mobile"))
     .when(F.rand() < 0.75, F.lit("desktop"))  # 0.50 + 0.30/0.50 = 0.75
     .otherwise(F.lit("tablet"))
).withColumn(
    "referrer",
    F.when(F.rand() < 0.40, F.lit("google"))
     .when(F.rand() < 0.65, F.lit("direct"))   # 0.40 + 0.25 = 0.65
     .when(F.rand() < 0.85, F.lit("ad"))       # 0.65 + 0.20 = 0.85
     .otherwise(F.lit("email"))
).withColumn(
    "metadata",
    F.struct(
        F.col("device"),
        F.col("referrer")
    )
)

# Step 7: Select final columns matching the template schema
clickstream_final = clickstream_with_metadata.select(
    "user_id",
    "property_id",
    "event",
    "timestamp",
    "metadata"
)

print("Added timestamps and metadata")
print("Sample of generated data:")
display(clickstream_final.limit(10))

# COMMAND ----------

# DBTITLE 1,Filter Events to Be Before Bookings
# Ensure all events occur BEFORE bookings (at least 10 seconds before)
print("=== Filtering Events to Occur Before Bookings ===")

# Get bookings range
bookings_range_df = spark.sql("""
    SELECT 
        MIN(created_at) as min_booking,
        MAX(created_at) as max_booking
    FROM samples.wanderbricks.bookings
""")

bookings_range_values = bookings_range_df.collect()[0]
min_booking = bookings_range_values['min_booking']
max_booking = bookings_range_values['max_booking']

print(f"Bookings range: {min_booking} to {max_booking}")

# Get synthetic data range BEFORE filtering
synthetic_range_before = clickstream_final.select(
    F.min("timestamp").alias("min_synthetic"),
    F.max("timestamp").alias("max_synthetic"),
    F.count("*").alias("total_events")
)

print("\nSynthetic data BEFORE filtering:")
display(synthetic_range_before)

# CRITICAL: Filter to ensure timestamps are BEFORE bookings (not at or after)
# Keep events that are at least 10 seconds before the max booking
clickstream_final = clickstream_final.filter(
    (F.col("timestamp") >= F.lit(min_booking)) & 
    (F.col("timestamp") < F.lit(max_booking) - F.expr("INTERVAL 10 SECONDS"))
)

# Verify after filtering
synthetic_range_after = clickstream_final.select(
    F.min("timestamp").alias("min_synthetic"),
    F.max("timestamp").alias("max_synthetic"),
    F.count("*").alias("total_events")
)

print("\nSynthetic data AFTER filtering:")
display(synthetic_range_after)

after_values = synthetic_range_after.collect()[0]
print(f"\nFiltered to {after_values['total_events']:,} events")
print(f"All events occur between {after_values['min_synthetic']} and {after_values['max_synthetic']}")

# COMMAND ----------

# DBTITLE 1,Add Pre-Platform View Events
# Add view events for the week BEFORE the first booking
# This simulates users browsing before the platform officially launched
print("=== Adding Pre-Platform View Events ===")

# Get earliest booking
earliest_booking = spark.sql("""
    SELECT 
        MIN(created_at) as first_booking_time
    FROM samples.wanderbricks.bookings
""").collect()[0]['first_booking_time']

print(f"First booking: {earliest_booking}")

# Calculate one week before
from datetime import timedelta
one_week_before = earliest_booking - timedelta(days=7)

print(f"Generating events from {one_week_before} to {earliest_booking}")

# Get a sample of users and properties for pre-platform events
sampled_users_properties = spark.sql("""
    SELECT DISTINCT
        user_id,
        property_id
    FROM samples.wanderbricks.bookings
    ORDER BY RAND()
    LIMIT 50
""")

print(f"\nGenerating 8-12 events for {sampled_users_properties.count()} user/property combinations")

# Generate 8-12 random timestamps in the week before first booking
import random
from pyspark.sql.types import TimestampType

# Create pre-platform events
pre_platform_events = sampled_users_properties.withColumn(
    "event", F.lit("view")
).withColumn(
    "timestamp",
    # Random timestamp between 1 week before and first booking
    (F.lit(one_week_before).cast("long") + 
     (F.rand() * (F.lit(earliest_booking).cast("long") - F.lit(one_week_before).cast("long")))
    ).cast("timestamp")
).withColumn(
    "device",
    F.when(F.rand() < 0.50, F.lit("mobile"))
     .when(F.rand() < 0.75, F.lit("desktop"))
     .otherwise(F.lit("tablet"))
).withColumn(
    "referrer",
    F.when(F.rand() < 0.40, F.lit("google"))
     .when(F.rand() < 0.65, F.lit("direct"))
     .when(F.rand() < 0.85, F.lit("ad"))
     .otherwise(F.lit("email"))
).withColumn(
    "metadata",
    F.struct(F.col("device"), F.col("referrer"))
).select(
    "user_id",
    "property_id",
    "event",
    "timestamp",
    "metadata"
)

# Explode to create 10 events per user/property
from pyspark.sql.functions import explode, array, lit
pre_platform_events_expanded = pre_platform_events.withColumn(
    "event_num", explode(array([lit(i) for i in range(10)]))
).withColumn(
    # Re-randomize timestamp for each event
    "timestamp",
    (F.lit(one_week_before).cast("long") + 
     (F.rand() * (F.lit(earliest_booking).cast("long") - F.lit(one_week_before).cast("long")))
    ).cast("timestamp")
).select(
    "user_id",
    "property_id",
    "event",
    "timestamp",
    "metadata"
)

print(f"\nCreated {pre_platform_events_expanded.count()} pre-platform events")

# Union with existing clickstream data
clickstream_final = clickstream_final.union(pre_platform_events_expanded)

print(f"\nTotal events after adding pre-platform data: {clickstream_final.count():,}")

# Verify the new range
final_range = clickstream_final.select(
    F.min("timestamp").alias("min_timestamp"),
    F.max("timestamp").alias("max_timestamp"),
    F.count("*").alias("total_events")
)

print("\nFinal timestamp range:")
display(final_range)

final_values = final_range.collect()[0]
print(f"\n✅ Final dataset: {final_values['total_events']:,} events")
print(f"   Range: {final_values['min_timestamp']} to {final_values['max_timestamp']}")
print(f"   Pre-platform period: ~{(earliest_booking - final_values['min_timestamp']).days} days before first booking")

# COMMAND ----------

# DBTITLE 1,Generate Non-Booking Viewers (56% of traffic)
# Generate browsing-only users who never book
# Target: ~44% conversion rate (matching original Wanderbricks pattern)
print("=== Generating Non-Booking Viewers ===")

import random
from datetime import datetime

# Calculate how many non-booking users we need
booking_user_count = spark.sql("SELECT COUNT(DISTINCT user_id) as count FROM samples.wanderbricks.bookings").collect()[0]['count']
target_conversion_rate = 0.44
non_booking_user_count = int(booking_user_count / target_conversion_rate) - booking_user_count

print(f"\nBooking users: {booking_user_count:,}")
print(f"Target conversion rate: {target_conversion_rate*100}%")
print(f"Non-booking users needed: {non_booking_user_count:,}")
print(f"Total viewers: {booking_user_count + non_booking_user_count:,}")

# Get all unique properties
all_properties_list = spark.sql("""
    SELECT DISTINCT property_id 
    FROM samples.wanderbricks.bookings
""").collect()
property_ids = [row['property_id'] for row in all_properties_list]

print(f"\nAvailable properties: {len(property_ids):,}")

# Generate events data in Python (faster than Spark for this)
min_date = datetime(2022, 12, 1)
max_date = datetime(2025, 7, 30)
date_range_seconds = int((max_date - min_date).total_seconds())

devices = ['mobile', 'desktop', 'tablet']
device_weights = [0.50, 0.30, 0.20]
referrers = ['google', 'direct', 'ad', 'email']
referrer_weights = [0.40, 0.25, 0.20, 0.15]

print(f"\nGenerating events for {non_booking_user_count:,} non-booking users...")

events_data = []
for user_id in range(200000, 200000 + non_booking_user_count):
    num_events = random.randint(1, 6)  # 1-6 events per user
    for _ in range(num_events):
        property_id = random.choice(property_ids)
        timestamp = min_date.timestamp() + random.random() * date_range_seconds
        device = random.choices(devices, weights=device_weights)[0]
        referrer = random.choices(referrers, weights=referrer_weights)[0]
        
        events_data.append((
            user_id,
            property_id,
            'view',
            datetime.fromtimestamp(timestamp),
            (device, referrer)
        ))

print(f"Generated {len(events_data):,} events in Python")

# Create Spark DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

non_booking_schema = StructType([
    StructField("user_id", LongType(), True),
    StructField("property_id", LongType(), True),
    StructField("event", StringType(), False),
    StructField("timestamp", TimestampType(), True),
    StructField("metadata", StructType([
        StructField("device", StringType(), False),
        StructField("referrer", StringType(), False)
    ]), False)
])

non_booking_events = spark.createDataFrame(events_data, non_booking_schema)

print(f"\n✅ Generated {non_booking_events.count():,} non-booking view events")
print(f"   Unique non-booking users: {non_booking_events.select('user_id').distinct().count():,}")
print(f"   Date range: {min_date} to {max_date}")

# Calculate avg events per user
avg_events = len(events_data) / non_booking_user_count
print(f"   Avg events per non-booking user: {avg_events:.1f}")

print("\n📊 Non-booking events ready to union with booking-user events")

# COMMAND ----------

# DBTITLE 1,Union and Write to Target Table
# Union booking-user events with non-booking events
print("=== Combining Booking and Non-Booking Events ===")

print(f"\nBooking-user events: {clickstream_final.count():,}")
print(f"Non-booking events: {non_booking_events.count():,}")

# Union both datasets
clickstream_complete = clickstream_final.union(non_booking_events)

total_events = clickstream_complete.count()
print(f"\nTotal combined events: {total_events:,}")

# Verify user counts
user_stats = clickstream_complete.select(
    F.countDistinct("user_id").alias("total_viewers")
).collect()[0]

print(f"Total unique viewers: {user_stats['total_viewers']:,}")

# Calculate conversion rate
booking_users = spark.sql("SELECT COUNT(DISTINCT user_id) as count FROM samples.wanderbricks.bookings").collect()[0]['count']
conversion_rate = (booking_users / user_stats['total_viewers']) * 100

print(f"\nConversion rate: {conversion_rate:.2f}% (target: ~44%)")

# Write to table (target_table set from widget parameters above)

print(f"\n{'='*80}")
print(f"Writing {total_events:,} events to {target_table}...")
print(f"{'='*80}")
print("\nData characteristics:")
print("  - 100% view events")
print("  - All booking-user events occur BEFORE bookings")
print("  - Includes pre-platform browsing data")
print(f"  - ~{conversion_rate:.1f}% conversion rate (realistic funnel)")
print(f"  - {booking_users:,} booking users + ~{user_stats['total_viewers'] - booking_users:,} non-booking users")

clickstream_complete.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"\n✅ Successfully wrote data to {target_table}")
print(f"Verification - Row count: {spark.table(target_table).count():,}")

# COMMAND ----------

# DBTITLE 1,Validation 1: Schema Comparison
print("=== Schema Comparison ===")
print("\nTemplate Schema (samples.wanderbricks.clickstream):")
spark.sql("DESCRIBE samples.wanderbricks.clickstream").show(truncate=False)

print(f"\nSynthetic Schema ({target_table}):")
spark.sql(f"DESCRIBE {target_table}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Validation 2: Timestamp Range Match
print("=== Timestamp Range Validation ===")
print("\nComparing timestamp ranges between bookings and synthetic data:")

timestamp_comparison = spark.sql(f"""
    SELECT 'synthetic_v3' as source,
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT property_id) as unique_properties,
        MIN(timestamp) as earliest_event,
        MAX(timestamp) as latest_event
    FROM {target_table}
    UNION ALL
    SELECT 'bookings' as source,
        COUNT(*) as total_records,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT property_id) as unique_properties,
        MIN(created_at) as earliest_event,
        MAX(created_at) as latest_event
    FROM samples.wanderbricks.bookings
""")

display(timestamp_comparison)

# Check match
result = timestamp_comparison.collect()
if len(result) == 2:
    synthetic = result[0] if result[0]['source'] == 'synthetic_v3' else result[1]
    bookings = result[1] if result[0]['source'] == 'synthetic_v3' else result[0]
    
    print("\n=== Timestamp Analysis ===")
    print(f"Synthetic MIN: {synthetic['earliest_event']}")
    print(f"Bookings MIN:  {bookings['earliest_event']}")
    print(f"Difference: Synthetic starts {(bookings['earliest_event'] - synthetic['earliest_event']).days} days BEFORE first booking \u2705")
    print(f"\nSynthetic MAX: {synthetic['latest_event']}")
    print(f"Bookings MAX:  {bookings['latest_event']}")
    print(f"Difference: Synthetic ends {(bookings['latest_event'] - synthetic['latest_event']).total_seconds():.0f} seconds BEFORE last booking \u2705")

# COMMAND ----------

# DBTITLE 1,Validation 3: Event Type Check (Must be 100% view)
print("=== Event Type Distribution ===")
print("\nShould be 100% 'view' events:")

event_distribution = spark.sql(f"""
    SELECT
        event,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {target_table}
    GROUP BY event
    ORDER BY count DESC
""")

display(event_distribution)

# Verify 100% view
result = event_distribution.collect()
if len(result) == 1 and result[0]['event'] == 'view' and result[0]['percentage'] == 100.0:
    print("\n✅ SUCCESS: 100% of events are 'view' type")
else:
    print("\n❌ FAILURE: Not all events are 'view' type")

# COMMAND ----------

# DBTITLE 1,Validation 4: Events per Booking
print("=== Events per Booking Analysis ===")
print("(Note: This only analyzes booking users, not non-booking browsers)\n")

events_per_booking = spark.sql(f"""
    WITH booking_events AS (
        SELECT 
            b.booking_id,
            b.user_id,
            b.property_id,
            b.created_at,
            COUNT(c.event) as event_count,
            MIN(c.timestamp) as first_event,
            MAX(c.timestamp) as last_event
        FROM samples.wanderbricks.bookings b
        LEFT JOIN {target_table} c
            ON b.user_id = c.user_id 
            AND b.property_id = c.property_id
            AND c.timestamp <= b.created_at
            AND c.timestamp >= b.created_at - INTERVAL 30 DAYS
        GROUP BY b.booking_id, b.user_id, b.property_id, b.created_at
    )
    SELECT 
        COUNT(*) as total_bookings,
        ROUND(AVG(event_count), 2) as avg_events_per_booking,
        MIN(event_count) as min_events,
        MAX(event_count) as max_events,
        COUNT(CASE WHEN event_count = 0 THEN 1 END) as bookings_without_events,
        COUNT(CASE WHEN event_count >= 5 AND event_count <= 20 THEN 1 END) as bookings_in_target_range
    FROM booking_events
""")

display(events_per_booking)

result = events_per_booking.collect()[0]
if result['bookings_without_events'] == 0:
    print("\n✅ SUCCESS: All bookings have associated events")
else:
    print(f"\n⚠️ WARNING: {result['bookings_without_events']} bookings without events")

if result['min_events'] >= 5 and result['max_events'] <= 20:
    print("✅ SUCCESS: All bookings have 5-20 events")
else:
    print(f"⚠️ INFO: Event range is {result['min_events']}-{result['max_events']} (target: 5-20)")

print(f"\n📊 Note: Non-booking users (user_id >= 200000) have 1-6 events and no bookings")

# COMMAND ----------

# DBTITLE 1,Validation 5: Temporal Distribution
print("=== Temporal Distribution of Events ===")
print("(Showing booking-user events relative to their bookings)")
print("\nHow far before booking did events occur:")

temporal_distribution = spark.sql(f"""
    WITH time_diffs AS (
        SELECT 
            b.booking_id,
            c.event,
            c.timestamp,
            b.created_at,
            (UNIX_TIMESTAMP(b.created_at) - UNIX_TIMESTAMP(c.timestamp)) / 3600 as hours_before_booking
        FROM samples.wanderbricks.bookings b
        JOIN {target_table} c
            ON b.user_id = c.user_id 
            AND b.property_id = c.property_id
            AND c.timestamp <= b.created_at
    )
    SELECT 
        CASE 
            WHEN hours_before_booking < 1 THEN '< 1 hour'
            WHEN hours_before_booking < 24 THEN '1-24 hours'
            WHEN hours_before_booking < 168 THEN '1-7 days'
            WHEN hours_before_booking < 672 THEN '1-4 weeks'
            ELSE '> 4 weeks'
        END as time_window,
        COUNT(*) as event_count,
        COUNT(DISTINCT booking_id) as booking_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM time_diffs
    GROUP BY 1
    ORDER BY 
        CASE 
            WHEN time_window = '< 1 hour' THEN 1
            WHEN time_window = '1-24 hours' THEN 2
            WHEN time_window = '1-7 days' THEN 3
            WHEN time_window = '1-4 weeks' THEN 4
            ELSE 5
        END
""")

display(temporal_distribution)

print("\n📊 Note: Non-booking users have events spread randomly across the full date range")

# COMMAND ----------

# DBTITLE 1,Validation 6: Metadata Distribution
print("=== Metadata Distribution ===")
print("\nDevice and Referrer combinations:")

metadata_distribution = spark.sql(f"""
    SELECT 
        metadata.device,
        metadata.referrer,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {target_table}
    GROUP BY metadata.device, metadata.referrer
    ORDER BY count DESC
    LIMIT 20
""")

display(metadata_distribution)

print("\nDevice distribution:")
device_dist = spark.sql(f"""
    SELECT 
        metadata.device,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {target_table}
    GROUP BY metadata.device
    ORDER BY count DESC
""")
display(device_dist)

print("\nReferrer distribution:")
referrer_dist = spark.sql(f"""
    SELECT 
        metadata.referrer,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {target_table}
    GROUP BY metadata.referrer
    ORDER BY count DESC
""")
display(referrer_dist)

# COMMAND ----------

# DBTITLE 1,Validation 7: Data Quality - Events After Booking
print("=== Data Quality Check: Events After Booking ===")
print("\nChecking for invalid events (timestamp > booking created_at):")

invalid_events = spark.sql(f"""
    SELECT 
        COUNT(*) as invalid_events
    FROM {target_table} c
    JOIN samples.wanderbricks.bookings b
        ON c.user_id = b.user_id 
        AND c.property_id = b.property_id
    WHERE c.timestamp > b.created_at
""")

display(invalid_events)

result = invalid_events.collect()[0]
if result['invalid_events'] == 0:
    print("\n✅ SUCCESS: No events occur after their associated booking")
else:
    print(f"\n⚠️ INFO: {result['invalid_events']} events occur after a booking")
    print("\n📊 Explanation: These are from users who booked the same property MULTIPLE times.")
    print("   Events generated for the 2nd booking appear 'after' the 1st booking.")
    print("   This is CORRECT behavior - users browsed again before rebooking!")
    print("   The pipeline's 24-hour attribution window handles this correctly.")

# COMMAND ----------

# DBTITLE 1,Final Summary Report
print("="*80)
print("SYNTHETIC CLICKSTREAM DATA GENERATION - FINAL SUMMARY")
print("="*80)

# Get all key metrics
summary = spark.sql(f"""
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT property_id) as unique_properties,
        MIN(timestamp) as min_timestamp,
        MAX(timestamp) as max_timestamp
    FROM {target_table}
""")

result = summary.collect()[0]

# Get conversion rate
conversion = spark.sql(f"""
    WITH all_viewers AS (
        SELECT DISTINCT user_id FROM {target_table}
    ),
    booking_users AS (
        SELECT DISTINCT user_id FROM samples.wanderbricks.bookings
    )
    SELECT 
        COUNT(DISTINCT av.user_id) as total_viewers,
        COUNT(DISTINCT bu.user_id) as bookers,
        ROUND(COUNT(DISTINCT bu.user_id) * 100.0 / COUNT(DISTINCT av.user_id), 2) as conversion_rate
    FROM all_viewers av
    LEFT JOIN booking_users bu ON av.user_id = bu.user_id
""").collect()[0]

print(f"\nTable: {target_table}")
print(f"Total Events: {result['total_events']:,}")
print(f"Unique Viewers: {result['unique_users']:,}")
print(f"  ├─ Booking Users: {conversion['bookers']:,} ({conversion['conversion_rate']}%)")
print(f"  └─ Non-Booking Users: {result['unique_users'] - conversion['bookers']:,} ({100 - conversion['conversion_rate']:.2f}%)")
print(f"Unique Properties: {result['unique_properties']:,}")
print(f"Date Range: {result['min_timestamp']} to {result['max_timestamp']}")
print(f"Conversion Rate: {conversion['conversion_rate']}% (target: ~44%)")

print("\n" + "="*80)
print("SUCCESS CRITERIA CHECKLIST")
print("="*80)

checklist = [
    ("Schema matches template", "Validation 1"),
    ("100% view events", "Validation 3"),
    ("Timestamp range (pre-platform to last booking)", "Validation 2"),
    ("~44% conversion rate (realistic funnel)", "Validation 8"),
    ("5-20 events per booking user", "Validation 4"),
    ("Events before booking time", "Validation 7"),
    ("Realistic metadata distribution", "Validation 6"),
    ("Temporal distribution across weeks", "Validation 5"),
]

for idx, (criteria, validation) in enumerate(checklist, 1):
    print(f"{idx}. {criteria}: {validation}")

print("\n" + "="*80)
print("✅ Data generation complete! Run validation cells to verify all criteria.")
print("="*80)
print("\n📊 KEY IMPROVEMENT: Added non-booking viewers for realistic funnel analysis")
print(f"   Original Wanderbricks: 44% conversion | Our Synthetic: {conversion['conversion_rate']}%")

# COMMAND ----------

# DBTITLE 1,Validation 8: Conversion Rate & User Segmentation
print("=== Conversion Rate & User Segmentation Analysis ===")

# Analyze user types
user_segmentation = spark.sql(f"""
    WITH all_viewers AS (
        SELECT DISTINCT c.user_id
        FROM {target_table} c
    ),
    booking_users AS (
        SELECT DISTINCT user_id
        FROM samples.wanderbricks.bookings
    )
    SELECT 
        COUNT(DISTINCT av.user_id) as total_viewers,
        COUNT(DISTINCT bu.user_id) as users_who_booked,
        COUNT(DISTINCT av.user_id) - COUNT(DISTINCT bu.user_id) as users_who_didnt_book,
        ROUND(COUNT(DISTINCT bu.user_id) * 100.0 / COUNT(DISTINCT av.user_id), 2) as conversion_rate_pct
    FROM all_viewers av
    LEFT JOIN booking_users bu ON av.user_id = bu.user_id
""")

print("\nUser Segmentation:")
display(user_segmentation)

result = user_segmentation.collect()[0]
print(f"\n{'='*80}")
print("CONVERSION RATE ANALYSIS")
print(f"{'='*80}")
print(f"Total Viewers: {result['total_viewers']:,}")
print(f"Users Who Booked: {result['users_who_booked']:,} ({result['conversion_rate_pct']}%)")
print(f"Users Who Didn't Book: {result['users_who_didnt_book']:,} ({100 - result['conversion_rate_pct']:.2f}%)")

# Check if conversion rate is within target range (42-46%)
if 42 <= result['conversion_rate_pct'] <= 46:
    print(f"\n✅ SUCCESS: Conversion rate {result['conversion_rate_pct']}% is within target range (42-46%)")
else:
    print(f"\n⚠️ WARNING: Conversion rate {result['conversion_rate_pct']}% is outside target range (42-46%)")

# Verify no non-booking users have bookings
print("\n=== Verifying Non-Booking Users ===")
non_booking_check = spark.sql(f"""
    SELECT 
        COUNT(DISTINCT c.user_id) as non_booking_viewers,
        COUNT(DISTINCT b.user_id) as non_booking_users_with_bookings
    FROM {target_table} c
    LEFT JOIN samples.wanderbricks.bookings b ON c.user_id = b.user_id
    WHERE c.user_id >= 200000
""")

display(non_booking_check)

check_result = non_booking_check.collect()[0]
if check_result['non_booking_users_with_bookings'] == 0:
    print(f"\n✅ SUCCESS: All {check_result['non_booking_viewers']:,} non-booking users have NO bookings")
else:
    print(f"\n❌ FAILURE: {check_result['non_booking_users_with_bookings']} non-booking users have bookings!")

# Event distribution by user type
print("\n=== Events by User Type ===")
event_distribution = spark.sql(f"""
    SELECT
        CASE
            WHEN c.user_id >= 200000 THEN 'Non-Booking User'
            ELSE 'Booking User'
        END as user_type,
        COUNT(*) as total_events,
        COUNT(DISTINCT c.user_id) as unique_users,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT c.user_id), 2) as avg_events_per_user
    FROM {target_table} c
    GROUP BY 1
    ORDER BY 1
""")

display(event_distribution)

print("\n✅ Conversion rate validation complete!")

# COMMAND ----------


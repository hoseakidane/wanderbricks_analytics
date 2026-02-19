-- ============================================================================
-- MARKETPLACE INTELLIGENCE PIPELINE
-- ============================================================================
-- This pipeline implements the complete marketplace intelligence data flow
-- from source tables through silver layer (cleaned/enriched) to gold layer
-- (pre-aggregated analytics tables ready for serving via Lakebase).
--
-- Architecture: Bronze → Silver → Gold (Medallion)
-- Silver: 2 tables (clickstream + enriched bookings)
-- Gold: 7 tables (pre-aggregated for app consumption)
-- ============================================================================

-- ============================================================================
-- SILVER LAYER
-- ============================================================================

-- ----------------------------------------------------------------------------
-- silver_clickstream
-- Purpose: Clean clickstream with device/referrer extracted, date added
-- Source: hk_catalog.default.clickstream_synthetic_v3
-- Type: Streaming table (incremental append-only)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_clickstream (
  CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event EXPECT (event IN ('view', 'click', 'search', 'filter')) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned clickstream events with extracted device and referrer metadata'
AS
SELECT
  user_id,
  property_id,
  event,
  timestamp,
  metadata.device AS device,
  metadata.referrer AS referrer,
  DATE(timestamp) AS event_date
FROM STREAM(${source_catalog}.${source_schema}.clickstream_synthetic_v3);

-- ----------------------------------------------------------------------------
-- silver_bookings_enriched
-- Purpose: Wide denormalized booking table with all related data joined
-- Sources: bookings, payments, properties, destinations, hosts
-- Type: Materialized view (daily batch refresh)
-- Note: Kept as materialized view (not streaming) because bookings table has
--       frequent status updates (89% of records updated after creation).
--       Materialized view reads latest state, avoiding duplicate records.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_bookings_enriched (
  CONSTRAINT required_booking_id EXPECT (booking_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT required_property_id EXPECT (property_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Enriched bookings with payment, property, destination, and host details'
AS
SELECT
  -- Booking details
  b.booking_id,
  b.user_id,
  b.property_id,
  b.status AS booking_status,
  b.total_amount,
  b.created_at AS booking_created_at,
  b.check_in,
  b.check_out,
  DATE(b.created_at) AS booking_date,
  
  -- Payment details
  p.status AS payment_status,
  p.payment_method,
  p.amount AS payment_amount,
  
  -- Property details
  pr.destination_id,
  pr.property_type,
  pr.base_price,
  pr.host_id,
  pr.title AS property_title,
  
  -- Host details
  h.rating AS host_rating,
  h.is_verified AS host_verified,
  h.name AS host_name,
  
  -- Destination details
  d.destination AS city,
  d.country
FROM samples.wanderbricks.bookings b
LEFT JOIN samples.wanderbricks.payments p ON b.booking_id = p.booking_id
INNER JOIN samples.wanderbricks.properties pr ON b.property_id = pr.property_id
INNER JOIN samples.wanderbricks.destinations d ON pr.destination_id = d.destination_id
INNER JOIN samples.wanderbricks.hosts h ON pr.host_id = h.host_id;

-- ============================================================================
-- GOLD LAYER
-- ============================================================================

-- ----------------------------------------------------------------------------
-- temp_date_boundaries
-- Purpose: Shared date range from clickstream for consistent filtering across gold tables
-- Type: Temporary view (pipeline lifetime only, not materialized)
-- Usage: Referenced by all gold tables to ensure consistent date filtering
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW temp_date_boundaries
COMMENT 'Shared date boundaries from clickstream for consistent filtering'
AS
SELECT 
  MIN(event_date) AS min_date,
  MAX(event_date) AS max_date
FROM silver_clickstream;

-- ----------------------------------------------------------------------------
-- gold_city_funnel
-- Purpose: Daily conversion funnel metrics per city with three-stage funnel
-- Serves: Overview tab → Conversion Funnel panel
-- Grain: One row per city per day
-- Funnel stages:
--   1. Viewers → Initiated Bookers (demand signal)
--   2. Initiated Bookers → Confirmed Bookers (booking flow health)
--   3. Confirmed Bookers → Completers (final outcome, only past check-out)
-- Note: Completers only counts bookings where check_out < CURRENT_DATE() to ensure
--       accurate completion rates (no in-flight reservations)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_city_funnel (
  CONSTRAINT non_negative_viewers EXPECT (viewers >= 0),
  CONSTRAINT non_negative_bookers EXPECT (initiated_bookers >= 0 AND confirmed_bookers >= 0 AND completers >= 0),
  CONSTRAINT logical_funnel EXPECT (initiated_bookers >= confirmed_bookers AND confirmed_bookers >= completers)
)
COMMENT 'Daily conversion funnel: viewers → initiated → confirmed → completed (past check-out only)'
AS
WITH date_range AS (
  SELECT * FROM temp_date_boundaries
),
clickstream_events AS (
  SELECT
    d.destination AS city,
    sc.event_date,
    COUNT(DISTINCT CASE WHEN sc.event = 'view' THEN sc.user_id END) AS viewers,
    COUNT(DISTINCT CASE WHEN sc.event = 'click' THEN sc.user_id END) AS clickers,
    COUNT(DISTINCT CASE WHEN sc.event = 'search' THEN sc.user_id END) AS searchers,
    COUNT(DISTINCT CASE WHEN sc.event = 'filter' THEN sc.user_id END) AS filterers
  FROM silver_clickstream sc
  INNER JOIN samples.wanderbricks.properties p ON sc.property_id = p.property_id
  INNER JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
  GROUP BY d.destination, sc.event_date
),
initiated_bookers AS (
  SELECT
    city,
    booking_date AS event_date,
    COUNT(DISTINCT user_id) AS initiated_bookers
  FROM silver_bookings_enriched
  CROSS JOIN date_range
  WHERE booking_date BETWEEN date_range.min_date AND date_range.max_date
  GROUP BY city, booking_date
),
confirmed_bookers AS (
  SELECT
    city,
    booking_date AS event_date,
    COUNT(DISTINCT user_id) AS confirmed_bookers
  FROM silver_bookings_enriched
  CROSS JOIN date_range
  WHERE booking_status IN ('confirmed', 'completed')
    AND booking_date BETWEEN date_range.min_date AND date_range.max_date
  GROUP BY city, booking_date
),
completers AS (
  SELECT
    city,
    booking_date AS event_date,
    COUNT(DISTINCT user_id) AS completers
  FROM silver_bookings_enriched
  CROSS JOIN date_range
  WHERE booking_status = 'completed' 
    AND payment_status = 'completed'
    AND check_out < CURRENT_DATE()
    AND booking_date BETWEEN date_range.min_date AND date_range.max_date
  GROUP BY city, booking_date
)
SELECT
  COALESCE(ce.city, ib.city, cb.city, c.city) AS city,
  COALESCE(ce.event_date, ib.event_date, cb.event_date, c.event_date) AS event_date,
  COALESCE(ce.viewers, 0) AS viewers,
  COALESCE(ce.clickers, 0) AS clickers,
  COALESCE(ce.searchers, 0) AS searchers,
  COALESCE(ce.filterers, 0) AS filterers,
  COALESCE(ib.initiated_bookers, 0) AS initiated_bookers,
  COALESCE(cb.confirmed_bookers, 0) AS confirmed_bookers,
  COALESCE(c.completers, 0) AS completers
FROM clickstream_events ce
FULL OUTER JOIN initiated_bookers ib ON ce.city = ib.city AND ce.event_date = ib.event_date
FULL OUTER JOIN confirmed_bookers cb ON COALESCE(ce.city, ib.city) = cb.city AND COALESCE(ce.event_date, ib.event_date) = cb.event_date
FULL OUTER JOIN completers c ON COALESCE(ce.city, ib.city, cb.city) = c.city 
  AND COALESCE(ce.event_date, ib.event_date, cb.event_date) = c.event_date;

-- ----------------------------------------------------------------------------
-- gold_city_investment
-- Purpose: Investment priority matrix with quadrant classification
-- Serves: Overview tab → Investment Priority Matrix + quadrant cards
-- Grain: One row per city
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_city_investment
COMMENT 'City-level investment priority with quadrant classification'
AS
WITH city_metrics AS (
  SELECT
    city,
    SUM(viewers) AS total_viewers,
    SUM(completers) AS total_completers,
    CASE 
      WHEN SUM(viewers) > 0 THEN (SUM(completers) * 100.0 / SUM(viewers))
      ELSE 0
    END AS conversion_rate
  FROM gold_city_funnel
  GROUP BY city
),
avg_booking_value AS (
  SELECT AVG(total_amount) AS avg_value
  FROM silver_bookings_enriched
  WHERE booking_status = 'confirmed'
),
thresholds AS (
  SELECT
    PERCENTILE(total_viewers, 0.5) AS median_viewers,
    PERCENTILE(conversion_rate, 0.5) AS median_conversion
  FROM city_metrics
)
SELECT
  cm.city,
  cm.total_viewers,
  cm.total_completers,
  cm.conversion_rate,
  cm.total_completers * abv.avg_value AS estimated_revenue,
  CASE
    WHEN cm.total_viewers >= t.median_viewers AND cm.conversion_rate < t.median_conversion 
      THEN 'Fix Supply/UX'
    WHEN cm.total_viewers >= t.median_viewers AND cm.conversion_rate >= t.median_conversion 
      THEN 'Protect & Expand'
    WHEN cm.total_viewers < t.median_viewers AND cm.conversion_rate >= t.median_conversion 
      THEN 'Invest Marketing'
    ELSE 'Deprioritize'
  END AS quadrant
FROM city_metrics cm
CROSS JOIN avg_booking_value abv
CROSS JOIN thresholds t;

-- ----------------------------------------------------------------------------
-- gold_property_performance
-- Purpose: Property-level performance metrics with three-stage funnel
-- Serves: Property Performance tab → scatter, cards, table
-- Grain: One row per property
-- Note: Only counts bookings from users who viewed OR clicked the property
-- Funnel stages:
--   1. Views → Initiated Bookings (demand signal)
--   2. Initiated Bookings → Confirmed Bookings (booking flow health)
--   3. Confirmed Bookings → Completed Bookings (final outcome, only past check-out)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_property_performance (
  CONSTRAINT valid_initiation_rate EXPECT (initiation_rate BETWEEN 0 AND 100),
  CONSTRAINT valid_confirmation_rate EXPECT (confirmation_rate BETWEEN 0 AND 100),
  CONSTRAINT valid_completion_rate EXPECT (completion_rate BETWEEN 0 AND 100),
  CONSTRAINT logical_funnel EXPECT (initiated_bookings >= confirmed_bookings AND confirmed_bookings >= completed_bookings)
)
COMMENT 'Property performance metrics with three-stage conversion funnel'
AS
WITH date_range AS (
  SELECT * FROM temp_date_boundaries
),
traffic AS (
  SELECT
    property_id,
    COUNT(CASE WHEN event IN ('view', 'click') THEN 1 END) AS views,
    COUNT(DISTINCT CASE WHEN event IN ('view', 'click') THEN user_id END) AS unique_viewers
  FROM silver_clickstream
  WHERE property_id IS NOT NULL
  GROUP BY property_id
),
booking_stats AS (
  SELECT
    b.property_id,
    -- Stage 1: Initiated (all booking statuses)
    COUNT(DISTINCT b.booking_id) AS initiated_bookings,
    -- Stage 2: Confirmed (confirmed + completed status)
    COUNT(DISTINCT CASE WHEN b.booking_status IN ('confirmed', 'completed') THEN b.booking_id END) AS confirmed_bookings,
    -- Stage 3: Completed (completed status + completed payment + past check-out)
    COUNT(DISTINCT CASE WHEN b.booking_status = 'completed' AND b.payment_status = 'completed' AND b.check_out < CURRENT_DATE() THEN b.booking_id END) AS completed_bookings,
    -- Keep cancelled for diagnostics
    COUNT(DISTINCT CASE WHEN b.booking_status = 'cancelled' THEN b.booking_id END) AS cancelled_bookings,
    SUM(CASE WHEN b.booking_status = 'confirmed' THEN b.total_amount ELSE 0 END) AS total_revenue
  FROM silver_bookings_enriched b
  INNER JOIN silver_clickstream sc 
    ON b.user_id = sc.user_id 
    AND b.property_id = sc.property_id 
    AND sc.event IN ('view', 'click')
  CROSS JOIN date_range
  WHERE b.booking_date BETWEEN date_range.min_date AND date_range.max_date
  GROUP BY b.property_id
),
payment_stats AS (
  SELECT
    b.property_id,
    COUNT(DISTINCT CASE WHEN p.status = 'failed' THEN p.payment_id END) AS failed_payments,
    COUNT(DISTINCT p.payment_id) AS total_payments
  FROM samples.wanderbricks.bookings b
  INNER JOIN samples.wanderbricks.payments p ON b.booking_id = p.booking_id
  INNER JOIN silver_clickstream sc 
    ON b.user_id = sc.user_id 
    AND b.property_id = sc.property_id 
    AND sc.event IN ('view', 'click')
  CROSS JOIN date_range
  WHERE DATE(b.created_at) BETWEEN date_range.min_date AND date_range.max_date
  GROUP BY b.property_id
),
review_stats AS (
  SELECT
    property_id,
    AVG(rating) AS avg_review_rating,
    COUNT(*) AS review_count
  FROM samples.wanderbricks.reviews
  WHERE is_deleted = false
  GROUP BY property_id
)
SELECT
  p.property_id,
  p.title AS property_name,
  p.property_type,
  p.base_price,
  d.destination AS city,
  h.name AS host_name,
  h.rating AS host_rating,
  h.is_verified AS host_verified,
  COALESCE(t.views, 0) AS views,
  COALESCE(t.unique_viewers, 0) AS unique_viewers,
  -- Three-stage booking counts
  COALESCE(bs.initiated_bookings, 0) AS initiated_bookings,
  COALESCE(bs.confirmed_bookings, 0) AS confirmed_bookings,
  COALESCE(bs.completed_bookings, 0) AS completed_bookings,
  COALESCE(bs.cancelled_bookings, 0) AS cancelled_bookings,
  -- Three-stage conversion rates
  CASE 
    WHEN COALESCE(t.views, 0) > 0 THEN (COALESCE(bs.initiated_bookings, 0) * 100.0 / t.views)
    ELSE 0
  END AS initiation_rate,
  CASE 
    WHEN COALESCE(t.views, 0) > 0 THEN (COALESCE(bs.confirmed_bookings, 0) * 100.0 / t.views)
    ELSE 0
  END AS confirmation_rate,
  CASE 
    WHEN COALESCE(t.views, 0) > 0 THEN (COALESCE(bs.completed_bookings, 0) * 100.0 / t.views)
    ELSE 0
  END AS completion_rate,
  -- Other metrics
  CASE 
    WHEN COALESCE(bs.initiated_bookings, 0) > 0 THEN (COALESCE(bs.cancelled_bookings, 0) * 100.0 / bs.initiated_bookings)
    ELSE 0
  END AS cancel_rate,
  COALESCE(ps.failed_payments, 0) AS failed_payments,
  CASE 
    WHEN COALESCE(ps.total_payments, 0) > 0 THEN (COALESCE(ps.failed_payments, 0) * 100.0 / ps.total_payments)
    ELSE 0
  END AS payment_fail_rate,
  COALESCE(bs.total_revenue, 0) AS total_revenue,
  COALESCE(rs.avg_review_rating, 0) AS avg_review_rating,
  COALESCE(rs.review_count, 0) AS review_count,
  -- Performance category based on confirmation_rate
  CASE
    WHEN (COALESCE(t.views, 0) > 0 AND (COALESCE(bs.confirmed_bookings, 0) * 100.0 / t.views) < 5) 
      OR (COALESCE(ps.total_payments, 0) > 0 AND (COALESCE(ps.failed_payments, 0) * 100.0 / ps.total_payments) > 15)
      THEN 'intervention'
    WHEN COALESCE(t.views, 0) > 0 AND (COALESCE(bs.confirmed_bookings, 0) * 100.0 / t.views) BETWEEN 5 AND 10
      THEN 'at_risk'
    WHEN COALESCE(t.views, 0) > 0 AND (COALESCE(bs.confirmed_bookings, 0) * 100.0 / t.views) >= 10
      THEN 'promote'
    ELSE 'unknown'
  END AS performance_category
FROM samples.wanderbricks.properties p
INNER JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
INNER JOIN samples.wanderbricks.hosts h ON p.host_id = h.host_id
LEFT JOIN traffic t ON p.property_id = t.property_id
LEFT JOIN booking_stats bs ON p.property_id = bs.property_id
LEFT JOIN payment_stats ps ON p.property_id = ps.property_id
LEFT JOIN review_stats rs ON p.property_id = rs.property_id
WHERE COALESCE(t.views, 0) > 0;

-- ----------------------------------------------------------------------------
-- gold_amenity_lift
-- Purpose: Amenity impact analysis with three-stage conversion lift
-- Serves: Amenity Impact tab → bar chart + insight cards
-- Grain: One row per amenity × city × property_type
-- Note: Only counts bookings from users who viewed OR clicked the property
-- Funnel stages:
--   1. Initiation Lift (impact on getting users to initiate bookings)
--   2. Confirmation Lift (impact on getting initiated bookings confirmed)
--   3. Completion Lift (impact on getting confirmed bookings completed)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_amenity_lift (
  CONSTRAINT valid_amenity_data EXPECT (properties_with > 0 OR properties_without > 0)
)
COMMENT 'Amenity conversion lift analysis with three-stage funnel by city and property type'
AS
WITH date_range AS (
  SELECT * FROM temp_date_boundaries
),
property_conversion AS (
  SELECT
    p.property_id,
    d.destination AS city,
    p.property_type,
    COALESCE(t.unique_viewers, 0) AS unique_viewers,
    COALESCE(bs.initiated_bookings, 0) AS initiated_bookings,
    COALESCE(bs.confirmed_bookings, 0) AS confirmed_bookings,
    COALESCE(bs.completed_bookings, 0) AS completed_bookings,
    -- Three conversion rates
    CASE 
      WHEN COALESCE(t.unique_viewers, 0) > 5 THEN (COALESCE(bs.initiated_bookings, 0) * 100.0 / t.unique_viewers)
      ELSE NULL
    END AS initiation_rate,
    CASE 
      WHEN COALESCE(t.unique_viewers, 0) > 5 THEN (COALESCE(bs.confirmed_bookings, 0) * 100.0 / t.unique_viewers)
      ELSE NULL
    END AS confirmation_rate,
    CASE 
      WHEN COALESCE(t.unique_viewers, 0) > 5 THEN (COALESCE(bs.completed_bookings, 0) * 100.0 / t.unique_viewers)
      ELSE NULL
    END AS completion_rate
  FROM samples.wanderbricks.properties p
  INNER JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
  LEFT JOIN (
    SELECT property_id, COUNT(DISTINCT user_id) AS unique_viewers
    FROM silver_clickstream
    WHERE event IN ('view', 'click') AND property_id IS NOT NULL
    GROUP BY property_id
  ) t ON p.property_id = t.property_id
  LEFT JOIN (
    SELECT 
      b.property_id, 
      COUNT(DISTINCT b.booking_id) AS initiated_bookings,
      COUNT(DISTINCT CASE WHEN b.booking_status IN ('confirmed', 'completed') THEN b.booking_id END) AS confirmed_bookings,
      COUNT(DISTINCT CASE WHEN b.booking_status = 'completed' AND b.payment_status = 'completed' AND b.check_out < CURRENT_DATE() THEN b.booking_id END) AS completed_bookings
    FROM silver_bookings_enriched b
    INNER JOIN silver_clickstream sc 
      ON b.user_id = sc.user_id 
      AND b.property_id = sc.property_id 
      AND sc.event IN ('view', 'click')
    CROSS JOIN date_range
    WHERE b.booking_date BETWEEN date_range.min_date AND date_range.max_date
    GROUP BY b.property_id
  ) bs ON p.property_id = bs.property_id
),
amenity_analysis AS (
  SELECT
    a.amenity_id,
    a.name AS amenity_name,
    a.category AS amenity_category,
    pc.city,
    pc.property_type,
    -- Stage 1: Initiation lift
    AVG(CASE WHEN pa.property_id IS NOT NULL THEN pc.initiation_rate END) AS avg_initiation_with,
    AVG(CASE WHEN pa.property_id IS NULL THEN pc.initiation_rate END) AS avg_initiation_without,
    -- Stage 2: Confirmation lift
    AVG(CASE WHEN pa.property_id IS NOT NULL THEN pc.confirmation_rate END) AS avg_confirmation_with,
    AVG(CASE WHEN pa.property_id IS NULL THEN pc.confirmation_rate END) AS avg_confirmation_without,
    -- Stage 3: Completion lift
    AVG(CASE WHEN pa.property_id IS NOT NULL THEN pc.completion_rate END) AS avg_completion_with,
    AVG(CASE WHEN pa.property_id IS NULL THEN pc.completion_rate END) AS avg_completion_without,
    COUNT(DISTINCT CASE WHEN pa.property_id IS NOT NULL THEN pc.property_id END) AS properties_with,
    COUNT(DISTINCT CASE WHEN pa.property_id IS NULL THEN pc.property_id END) AS properties_without
  FROM samples.wanderbricks.amenities a
  CROSS JOIN property_conversion pc
  LEFT JOIN samples.wanderbricks.property_amenities pa 
    ON a.amenity_id = pa.amenity_id AND pc.property_id = pa.property_id
  WHERE pc.initiation_rate IS NOT NULL OR pc.confirmation_rate IS NOT NULL OR pc.completion_rate IS NOT NULL
  GROUP BY a.amenity_id, a.name, a.category, pc.city, pc.property_type
)
SELECT
  amenity_id,
  amenity_name,
  amenity_category,
  city,
  property_type,
  -- Initiation metrics
  COALESCE(avg_initiation_with, 0) AS avg_initiation_with,
  COALESCE(avg_initiation_without, 0) AS avg_initiation_without,
  COALESCE(avg_initiation_with, 0) - COALESCE(avg_initiation_without, 0) AS initiation_lift,
  -- Confirmation metrics
  COALESCE(avg_confirmation_with, 0) AS avg_confirmation_with,
  COALESCE(avg_confirmation_without, 0) AS avg_confirmation_without,
  COALESCE(avg_confirmation_with, 0) - COALESCE(avg_confirmation_without, 0) AS confirmation_lift,
  -- Completion metrics
  COALESCE(avg_completion_with, 0) AS avg_completion_with,
  COALESCE(avg_completion_without, 0) AS avg_completion_without,
  COALESCE(avg_completion_with, 0) - COALESCE(avg_completion_without, 0) AS completion_lift,
  properties_with,
  properties_without,
  -- Impact tier based on confirmation_lift (primary metric)
  CASE
    WHEN (COALESCE(avg_confirmation_with, 0) - COALESCE(avg_confirmation_without, 0)) >= 8 THEN 'must_have'
    WHEN (COALESCE(avg_confirmation_with, 0) - COALESCE(avg_confirmation_without, 0)) >= 4 THEN 'differentiator'
    ELSE 'low_impact'
  END AS impact_tier
FROM amenity_analysis
WHERE properties_with > 0 AND properties_without > 0;

-- ----------------------------------------------------------------------------
-- gold_amenity_city_top
-- Purpose: Top 3 amenities per city and property type by confirmation lift
-- Serves: Amenity Impact tab → per-city grid
-- Grain: One row per city × property_type × rank (3 rows per combination)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_amenity_city_top
COMMENT 'Top 3 amenities by confirmation lift for each city and property type'
AS
WITH ranked_amenities AS (
  SELECT
    city,
    property_type,
    amenity_name,
    confirmation_lift,
    ROW_NUMBER() OVER (
      PARTITION BY city, property_type 
      ORDER BY confirmation_lift DESC
    ) AS rank
  FROM gold_amenity_lift
)
SELECT
  city,
  property_type,
  amenity_name,
  confirmation_lift,
  rank
FROM ranked_amenities
WHERE rank <= 3;

-- ----------------------------------------------------------------------------
-- gold_device_funnel
-- Purpose: Device-specific three-stage conversion funnel with temporal attribution
-- Serves: Demand & Segmentation tab → device funnels + trend
-- Grain: One row per city × device × week
-- Note: Filters bookings to only include dates with clickstream data
-- Funnel stages:
--   1. Viewers → Initiated Bookers (demand signal)
--   2. Initiated Bookers → Confirmed Bookers (booking flow health)
--   3. Confirmed Bookers → Completers (final outcome, only past check-out)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_device_funnel (
  CONSTRAINT non_negative_viewers EXPECT (viewers >= 0),
  CONSTRAINT non_negative_bookers EXPECT (initiated_bookers >= 0 AND confirmed_bookers >= 0 AND completers >= 0),
  CONSTRAINT logical_funnel EXPECT (initiated_bookers >= confirmed_bookers AND confirmed_bookers >= completers)
)
COMMENT 'Weekly device-specific three-stage conversion funnel with 24h attribution window'
AS
WITH date_range AS (
  SELECT * FROM temp_date_boundaries
),
viewers_by_device AS (
  SELECT
    d.destination AS city,
    sc.device,
    DATE_TRUNC('WEEK', sc.event_date) AS week_start,
    COUNT(DISTINCT sc.user_id) AS viewers
  FROM silver_clickstream sc
  INNER JOIN samples.wanderbricks.properties p ON sc.property_id = p.property_id
  INNER JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
  WHERE sc.event IN ('view', 'click') AND sc.device IS NOT NULL
  GROUP BY d.destination, sc.device, DATE_TRUNC('WEEK', sc.event_date)
),
device_attribution AS (
  SELECT
    sbe.booking_id,
    sbe.user_id,
    sbe.city,
    sbe.booking_created_at,
    sbe.booking_status,
    sbe.payment_status,
    sbe.check_out,
    sc.device,
    ROW_NUMBER() OVER (
      PARTITION BY sbe.booking_id 
      ORDER BY sc.timestamp DESC
    ) AS recency_rank
  FROM silver_bookings_enriched sbe
  CROSS JOIN date_range
  INNER JOIN silver_clickstream sc 
    ON sbe.user_id = sc.user_id
    AND sc.timestamp <= sbe.booking_created_at
    AND sc.timestamp >= TIMESTAMPADD(HOUR, -24, sbe.booking_created_at)
  WHERE sc.device IS NOT NULL
    AND sbe.booking_date BETWEEN date_range.min_date AND date_range.max_date
),
initiated_bookers_by_device AS (
  SELECT
    city,
    device,
    DATE_TRUNC('WEEK', DATE(booking_created_at)) AS week_start,
    COUNT(DISTINCT user_id) AS initiated_bookers
  FROM device_attribution
  WHERE recency_rank = 1
  GROUP BY city, device, DATE_TRUNC('WEEK', DATE(booking_created_at))
),
confirmed_bookers_by_device AS (
  SELECT
    city,
    device,
    DATE_TRUNC('WEEK', DATE(booking_created_at)) AS week_start,
    COUNT(DISTINCT user_id) AS confirmed_bookers
  FROM device_attribution
  WHERE recency_rank = 1 
    AND booking_status IN ('confirmed', 'completed')
  GROUP BY city, device, DATE_TRUNC('WEEK', DATE(booking_created_at))
),
completers_by_device AS (
  SELECT
    city,
    device,
    DATE_TRUNC('WEEK', DATE(booking_created_at)) AS week_start,
    COUNT(DISTINCT user_id) AS completers
  FROM device_attribution
  WHERE recency_rank = 1 
    AND booking_status = 'completed' 
    AND payment_status = 'completed'
    AND check_out < CURRENT_DATE()
  GROUP BY city, device, DATE_TRUNC('WEEK', DATE(booking_created_at))
)
SELECT
  COALESCE(v.city, ib.city, cb.city, c.city) AS city,
  COALESCE(v.device, ib.device, cb.device, c.device) AS device,
  COALESCE(v.week_start, ib.week_start, cb.week_start, c.week_start) AS week_start,
  COALESCE(v.viewers, 0) AS viewers,
  COALESCE(ib.initiated_bookers, 0) AS initiated_bookers,
  COALESCE(cb.confirmed_bookers, 0) AS confirmed_bookers,
  COALESCE(c.completers, 0) AS completers,
  -- Three conversion rates
  CASE 
    WHEN COALESCE(v.viewers, 0) > 0 THEN (COALESCE(ib.initiated_bookers, 0) * 100.0 / v.viewers)
    ELSE 0
  END AS initiation_rate,
  CASE 
    WHEN COALESCE(v.viewers, 0) > 0 THEN (COALESCE(cb.confirmed_bookers, 0) * 100.0 / v.viewers)
    ELSE 0
  END AS confirmation_rate,
  CASE 
    WHEN COALESCE(v.viewers, 0) > 0 THEN (COALESCE(c.completers, 0) * 100.0 / v.viewers)
    ELSE 0
  END AS completion_rate
FROM viewers_by_device v
FULL OUTER JOIN initiated_bookers_by_device ib 
  ON v.city = ib.city AND v.device = ib.device AND v.week_start = ib.week_start
FULL OUTER JOIN confirmed_bookers_by_device cb 
  ON COALESCE(v.city, ib.city) = cb.city 
  AND COALESCE(v.device, ib.device) = cb.device
  AND COALESCE(v.week_start, ib.week_start) = cb.week_start
FULL OUTER JOIN completers_by_device c 
  ON COALESCE(v.city, ib.city, cb.city) = c.city 
  AND COALESCE(v.device, ib.device, cb.device) = c.device
  AND COALESCE(v.week_start, ib.week_start, cb.week_start) = c.week_start;

-- ----------------------------------------------------------------------------
-- gold_device_diagnosis
-- Purpose: Device gap analysis and UX vs Supply diagnosis using confirmation rate
-- Serves: Demand & Segmentation tab → diagnosis badge + mobile trend
-- Grain: One row per city
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_device_diagnosis
COMMENT 'Device conversion gap diagnosis and mobile trend analysis using confirmation rate'
AS
WITH device_rates AS (
  SELECT
    city,
    device,
    AVG(confirmation_rate) AS avg_confirmation_rate
  FROM gold_device_funnel
  GROUP BY city, device
),
device_pivot AS (
  SELECT
    city,
    MAX(CASE WHEN device = 'desktop' THEN avg_confirmation_rate ELSE 0 END) AS desktop_rate,
    MAX(CASE WHEN device = 'mobile' THEN avg_confirmation_rate ELSE 0 END) AS mobile_rate,
    MAX(CASE WHEN device = 'tablet' THEN avg_confirmation_rate ELSE 0 END) AS tablet_rate
  FROM device_rates
  GROUP BY city
),
mobile_trend_calc AS (
  SELECT
    city,
    device,
    week_start,
    confirmation_rate,
    ROW_NUMBER() OVER (PARTITION BY city, device ORDER BY week_start ASC) AS week_rank_asc,
    ROW_NUMBER() OVER (PARTITION BY city, device ORDER BY week_start DESC) AS week_rank_desc
  FROM gold_device_funnel
  WHERE device = 'mobile'
),
mobile_trend AS (
  SELECT
    city,
    MAX(CASE WHEN week_rank_asc = 1 THEN confirmation_rate END) AS first_week_rate,
    MAX(CASE WHEN week_rank_desc = 1 THEN confirmation_rate END) AS last_week_rate
  FROM mobile_trend_calc
  GROUP BY city
)
SELECT
  dp.city,
  dp.desktop_rate,
  dp.mobile_rate,
  dp.tablet_rate,
  CASE 
    WHEN dp.desktop_rate > 0 THEN ((dp.desktop_rate - dp.mobile_rate) * 100.0 / dp.desktop_rate)
    ELSE 0
  END AS device_gap_pct,
  CASE
    WHEN dp.desktop_rate > 0 AND ((dp.desktop_rate - dp.mobile_rate) * 100.0 / dp.desktop_rate) > 25 
      THEN 'likely_ux_issue'
    ELSE 'likely_supply_pricing_issue'
  END AS diagnosis,
  CASE
    WHEN mt.last_week_rate > mt.first_week_rate THEN 'improving'
    WHEN mt.last_week_rate < mt.first_week_rate THEN 'declining'
    ELSE 'stable'
  END AS mobile_trend
FROM device_pivot dp
LEFT JOIN mobile_trend mt ON dp.city = mt.city;

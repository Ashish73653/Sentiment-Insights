-- Athena DDL for Sentiment Insights
-- 1) Creates the analytics database
-- 2) Creates an external table over S3 JSON summaries with partition projection (dt)
-- 3) Creates a flattened view for BI/QuickSight
-- NOTE: Replace <YOUR_BUCKET> with your actual S3 bucket name that contains processed/summaries/

-- -------------------------------------------------------------------
-- 1) Database
-- -------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS product_analytics;

-- -------------------------------------------------------------------
-- 2) External table (JSON with partition projection on dt)
--    S3 path layout: s3://<YOUR_BUCKET>/processed/summaries/date=YYYY-MM-DD/product=<ID>.json
--    We project a virtual partition key "dt" (date) to avoid Glue crawlers/MSCK REPAIR.
-- -------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS product_analytics.review_summaries (
  product_id   string,
  date         string,
  summary      string,
  generated_at string,
  metadata     string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'='true'
)
LOCATION 's3://<YOUR_BUCKET>/processed/summaries/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.dt.type'='date',
  -- Adjust the range start to cover your earliest summary date
  'projection.dt.range'='2025-01-01,NOW',
  'projection.dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://<YOUR_BUCKET>/processed/summaries/date=${dt}/'
);

-- -------------------------------------------------------------------
-- 3) Flattened view (extracts fields from metadata for easy BI use)
--    - Athena views cannot directly expose JSON types; keep nested JSON as string via JSON_FORMAT
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW product_analytics.review_summary_flat AS
SELECT
  product_id,
  CAST(dt AS DATE) AS summary_date,
  summary,
  generated_at,
  JSON_EXTRACT_SCALAR(metadata, '$.orientation') AS orientation,
  JSON_EXTRACT_SCALAR(metadata, '$.method')      AS generation_method,
  CAST(JSON_EXTRACT_SCALAR(metadata, '$.polished') AS BOOLEAN) AS polished,
  CAST(JSON_EXTRACT_SCALAR(metadata, '$.review_counts.positive') AS INT) AS reviews_positive,
  CAST(JSON_EXTRACT_SCALAR(metadata, '$.review_counts.negative') AS INT) AS reviews_negative,
  CAST(JSON_EXTRACT_SCALAR(metadata, '$.review_counts.mixed')    AS INT) AS reviews_mixed,
  CAST(JSON_EXTRACT_SCALAR(metadata, '$.review_counts.neutral')  AS INT) AS reviews_neutral,
  JSON_FORMAT(JSON_EXTRACT(metadata, '$.theme_sentiment')) AS theme_sentiment_json
FROM product_analytics.review_summaries;

-- -------------------------------------------------------------------
-- 4) Quick validation queries (optional)
-- -------------------------------------------------------------------
-- Show today’s rows
-- SELECT * FROM product_analytics.review_summary_flat WHERE summary_date = CURRENT_DATE;

-- Orientation distribution for a day
-- SELECT orientation, COUNT(*) AS products
-- FROM product_analytics.review_summary_flat
-- WHERE summary_date = DATE '2025-11-15'
-- GROUP BY orientation
-- ORDER BY products DESC;

-- Top products by net sentiment (pos - neg)
-- SELECT
--   product_id,
--   summary_date,
--   (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral) AS total_reviews,
--   CASE WHEN (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral)=0
--        THEN 0
--        ELSE CAST(reviews_positive AS DOUBLE) /
--             (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral)
--   END AS positive_ratio,
--   CASE WHEN (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral)=0
--        THEN 0
--        ELSE CAST(reviews_negative AS DOUBLE) /
--             (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral)
--   END AS negative_ratio,
--   (CASE WHEN (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral)=0
--         THEN 0
--         ELSE CAST(reviews_positive AS DOUBLE) /
--              (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral) -
--              CAST(reviews_negative AS DOUBLE) /
--              (reviews_positive + reviews_negative + reviews_mixed + reviews_neutral)
--    END) AS net_sentiment_score
-- FROM product_analytics.review_summary_flat
-- WHERE summary_date = DATE '2025-11-15'
-- ORDER BY net_sentiment_score DESC;
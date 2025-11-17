# Sentiment Insights

End‑to‑end AWS serverless pipeline we actually built and ran:
Upload review CSVs → Automatic NLP enrichment → Daily product summarization polished with a live SageMaker DistilBART CNN 6‑6 endpoint → Stored in DynamoDB and S3 → Queried with Athena (partition projection) → Visualized in QuickSight.

---
![POC Architecture](docs/poc_architecture.png)

## 1. Implemented Components (Exact)

| Component | What We Did |
|-----------|-------------|
| S3 Bucket | Created single private bucket with prefixes: `uploads/`, `processed/summaries/`, `athena-results/`. |
| S3 Trigger | Configured ObjectCreated event for `uploads/*.csv` → ReviewProcessor Lambda. |
| Lambda: ReviewProcessorFunction | Parses CSV, truncates long text if needed, calls Amazon Comprehend (DetectSentiment, DetectKeyPhrases), converts floats to `Decimal`, writes each enriched review to DynamoDB ReviewAnalysis. |
| Amazon Comprehend | Used for sentiment label + scores and key phrase extraction for every review. |
| DynamoDB: ReviewAnalysis | Stores per‑review records with keys: `PK=REVIEW#<ReviewID>`, `SK=REVIEW#<ISO_TIMESTAMP>#<UUID>` plus attributes (ProductID, SentimentScores, KeyPhrases, ProcessedTimestampEpoch, OriginalHash, day discovery fields). |
| EventBridge Rule | Daily cron invocation (lookback 24h) of DailySummarizerFunction. |
| Lambda: DailySummarizerFunction | Auto‑discovers products from last 24h reviews; aggregates counts, orientation, themes; builds deterministic summary; invokes SageMaker polish; validates (2 sentences, length, no URLs); writes to DynamoDB ReviewSummaries and S3 JSON. |
| Amazon SageMaker Endpoint | Deployed DistilBART CNN 6‑6 model as real‑time endpoint on `ml.m5.large`; used every day for summary polishing (not optional in our build). |
| DynamoDB: ReviewSummaries | Stores final per‑product daily summary (`PK=ProductID`, `SK=SummaryTimestamp`) plus metadata (orientation, review_counts, theme_sentiment, polished flag, method). |
| S3 Processed Summaries | One JSON per product per day: `processed/summaries/date=YYYY-MM-DD/product=<ID>.json` containing summary + metadata. |
| Athena External Table | Created `product_analytics.review_summaries` using partition projection on `dt` (no crawler / no MSCK REPAIR). |
| Athena View | `product_analytics.review_summary_flat` extracts JSON fields for orientation, counts, polished flag, theme_sentiment (stringified). |
| QuickSight Dashboard | Built dataset from Athena view (SPICE import daily); visuals for product count, orientation distribution, net sentiment, summary table. |
| CloudWatch Logs | Enabled for both Lambdas and monitored ingestion & summarization runs. |
| IAM Roles | Separate least‑privilege roles for processor and summarizer (S3 read/write as needed, DynamoDB actions, Comprehend, SageMaker invoke, logging). |

---

## 2. Data Flow (What Actually Happens)

1. Upload CSV (headers: `ReviewID,ProductID,ReviewText`) to `uploads/`.
2. S3 event triggers ReviewProcessorFunction.
3. For each row:
   - Normalize text and IDs.
   - Call Comprehend (sentiment + key phrases).
   - Build item with epoch/time, hash, phrases, scores.
   - Write to `ReviewAnalysis`.
4. Daily cron fires DailySummarizerFunction.
5. Summarizer:
   - Scans for products with reviews in last 24h.
   - Aggregates counts and orientation.
   - Extracts theme sentiments and key phrases (filtered).
   - Builds deterministic two sentences.
   - Invokes SageMaker DistilBART endpoint for polish.
   - Validates polished output; falls back if invalid (2 sentence rule).
6. Writes final summary to `ReviewSummaries` and S3 JSON partition path.
7. Athena queries S3 via partition projection (no crawler).
8. View flattens metadata → QuickSight SPICE ingestion → Dashboard display.

---

## 3. DynamoDB Schemas (Implemented)

### ReviewAnalysis
| Field | Example |
|-------|---------|
| PK | `REVIEW#a1b2c3d4-0047` |
| SK | `REVIEW#2025-11-15T13:02:00.020246+00:00#<UUID>` |
| ProductID | `P743` |
| RedactedText | Original review text (no PII redaction yet) |
| Sentiment | `NEGATIVE` |
| SentimentScores | Map `{Positive, Negative, Neutral, Mixed}` |
| KeyPhrases | List of extracted phrases |
| ProcessedTimestampEpoch | `1763211720` |
| ProcessedTimestampIso | ISO timestamp |
| SourceS3Key | `uploads/mock-50-reviews-mixed_v3-productIDs-updated.csv` |
| OriginalHash | SHA‑1 of text |
| GSI1PK | `DAY#2025-11-15` |
| GSI1SK | `P743#1763211720#a1b2c3d4-0047` |

### ReviewSummaries
| Field | Example |
|-------|---------|
| ProductID | `P743` |
| SummaryTimestamp | `2025-11-15T13:04:14.249123+00:00` |
| SummaryDate | `2025-11-15` |
| SummaryText | Two sentences (polished or deterministic) |
| ReviewCount | `9` |
| SourceMeta.orientation | `predominantly negative` |
| SourceMeta.review_counts | `{positive:1, negative:6, mixed:2, neutral:0}` |
| SourceMeta.polished | `false` (or `true` when endpoint output accepted) |
| SourceMeta.method | `deterministic_first` |
| SourceMeta.theme_sentiment | Map (e.g. performance stability, design, quality) |

---

## 4. S3 Summary JSON (Actual Structure)
```
{
  "product_id": "P743",
  "date": "2025-11-15",
  "summary": "Overall sentiment leans negative, driven by issues in quality, performance stability, and design. Positive feedback is scarce.",
  "generated_at": "2025-11-15T13:04:14Z",
  "metadata": {
    "orientation": "predominantly negative",
    "review_counts": { "positive": 1, "negative": 6, "mixed": 2, "neutral": 0 },
    "theme_sentiment": { ... },
    "method": "deterministic_first",
    "polished": false
  }
}
```

---

## 5. Athena Setup (Implemented SQL)

```sql
CREATE DATABASE IF NOT EXISTS product_analytics;

CREATE EXTERNAL TABLE IF NOT EXISTS product_analytics.review_summaries (
  product_id   string,
  date         string,
  summary      string,
  generated_at string,
  metadata     string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<YOUR_BUCKET>/processed/summaries/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.dt.type'='date',
  'projection.dt.range'='2025-11-01,NOW',
  'projection.dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://<YOUR_BUCKET>/processed/summaries/date=${dt}/'
);

CREATE OR REPLACE VIEW product_analytics.review_summary_flat AS
SELECT
  product_id,
  CAST(dt AS DATE) AS summary_date,
  summary,
  generated_at,
  JSON_EXTRACT_SCALAR(metadata,'$.orientation') AS orientation,
  JSON_EXTRACT_SCALAR(metadata,'$.method')      AS generation_method,
  CAST(JSON_EXTRACT_SCALAR(metadata,'$.polished') AS BOOLEAN) AS polished,
  CAST(JSON_EXTRACT_SCALAR(metadata,'$.review_counts.positive') AS INT) AS reviews_positive,
  CAST(JSON_EXTRACT_SCALAR(metadata,'$.review_counts.negative') AS INT) AS reviews_negative,
  CAST(JSON_EXTRACT_SCALAR(metadata,'$.review_counts.mixed')    AS INT) AS reviews_mixed,
  CAST(JSON_EXTRACT_SCALAR(metadata,'$.review_counts.neutral')  AS INT) AS reviews_neutral,
  JSON_FORMAT(JSON_EXTRACT(metadata,'$.theme_sentiment')) AS theme_sentiment_json
FROM product_analytics.review_summaries;
```

---

## 6. Manual Console Setup (Exact Steps We Followed)

1. S3 bucket created; prefixes added manually.
2. DynamoDB tables `ReviewAnalysis` and `ReviewSummaries` created (on‑demand).
3. ReviewProcessor Lambda created; environment variables set; S3 trigger added for `uploads/` + `.csv`.
4. Comprehend permissions attached to Lambda role.
5. SageMaker DistilBART CNN 6‑6 model deployed as real‑time endpoint (`ml.m5.large`).
6. DailySummarizer Lambda created with environment variables (including endpoint name).
7. EventBridge rule created (daily cron) pointing to DailySummarizer.
8. Summaries written daily to DynamoDB & S3.
9. Athena database, external table (projection), view created.
10. QuickSight dataset created from Athena view; dashboard built.

---

## 7. Testing (Performed)

| Test | Result |
|------|--------|
| Upload CSV triggers ReviewProcessor | Items appear in ReviewAnalysis (50 test reviews) |
| Sentiment & key phrases populated | Verified in DynamoDB console |
| DailySummarizer manual invoke | Summaries written for discovered products |
| S3 summary files present | `processed/summaries/date=YYYY-MM-DD/` objects generated |
| Athena queries return rows | View `review_summary_flat` shows product rows |
| QuickSight refresh | Dashboard displays orientation & counts |
| SageMaker polish path | Endpoint invoked; validator enforces two sentences |

---

## 8. Environment Variables (Used)

ReviewProcessorFunction:
- REVIEW_TABLE
- DEFAULT_LANG
- COMPREHEND_ENABLED
- MAX_COMPREHEND_TEXT_LEN

DailySummarizerFunction:
- REVIEW_TABLE
- SUMMARY_TABLE
- PROCESS_LOOKBACK_HOURS
- SUMMARY_S3_BUCKET
- SUMMARY_S3_PREFIX
- POLISH_WITH_MODEL (set according to endpoint usage)
- SAGEMAKER_ENDPOINT
- SAGEMAKER_CONTENT_TYPE (`application/json`)

---
## 9. QuickSight Calculated Fields & Visuals

This section provides a copy‑paste friendly set of QuickSight calculated fields (aggregate‑safe variants included) and a visual blueprint to build an insightful, interactive dashboard.

### 9.1 Calculated Fields (Name | Formula | Notes)

| Name | Formula | Notes |
|---|---|---|
| total_reviews | `{reviews_positive} + {reviews_negative} + {reviews_mixed} + {reviews_neutral}` | Row-level total. Set default aggregation to sum in visuals. |
| total_reviews_agg | `sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral})` | Aggregate-safe total for use directly in visuals. |
| positive_rate | `ifelse({total_reviews}=0, 0, {reviews_positive}/{total_reviews})` | Row-level rate. Prefer the aggregate-safe variant below for visuals. |
| positive_rate_agg | `ifelse((sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral}))=0, 0, sum({reviews_positive}) / (sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral})))` | Aggregate-safe (ratio of sums) to avoid mismatched aggregation errors. |
| negative_rate_agg | `ifelse((sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral}))=0, 0, sum({reviews_negative}) / (sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral})))` | Aggregate-safe. |
| mixed_rate_agg | `ifelse((sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral}))=0, 0, sum({reviews_mixed}) / (sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral})))` | Aggregate-safe. |
| neutral_rate_agg | `ifelse((sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral}))=0, 0, sum({reviews_neutral}) / (sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral})))` | Aggregate-safe. |
| net_sentiment | `{positive_rate} - {negative_rate}` | Row-level. Use with `avg()` in visuals. |
| net_sentiment_agg | `{positive_rate_agg} - {negative_rate_agg}` | Aggregate-safe net sentiment. |
| positivity_index_0_100 | `50 + 50*{net_sentiment}` | Use `avg(positivity_index_0_100)` in visuals; or build from `_agg` version. |
| positivity_index_0_100_agg | `50 + 50*{net_sentiment_agg}` | Aggregate-safe alternative. |
| orientation_short (CASE) | `case when lower(trim({orientation}))='predominantly positive' then 'Positive' when lower(trim({orientation}))='predominantly negative' then 'Negative' else 'Mixed' end` | Prefer CASE if `ifelse/lower` throw errors. If exact case matches, you can use simple equals without `lower/trim`. |
| latest_flag_by_product | `ifelse(denseRank([{summary_date} DESC], [{product_id}])=1, 1, 0)` | Analysis-level calc (window function). Flags latest row per product. |
| week_start | `truncDate('WK', {summary_date})` | Week bucket for trends. |
| month_start | `truncDate('MM', {summary_date})` | Month bucket for trends. |
| days_since_summary | `dateDiff('DD', {summary_date}, truncDate('DD', now()))` | Ensures DATE‑to‑DATE comparison (truncate `now()`). |
| rolling_7d_reviews | `sumOver(sum({reviews_positive}+{reviews_negative}+{reviews_mixed}+{reviews_neutral}), [], [{summary_date ASC}], 6, 0)` | 7‑day trailing window ending on current row. Analysis-level calc. |
| wow_net_sentiment | `periodOverPeriodDifference(avg({net_sentiment}), {summary_date}, 'WK')` | Week‑over‑week change. Analysis-level calc. |
| wow_net_sentiment_pct | `periodOverPeriodPercentDifference(avg({net_sentiment}), {summary_date}, 'WK')` | WoW % change. Analysis-level calc. |
| search_match | `ifelse(length(trim(${p_search}))=0, 1=1, contains(lower({summary}), lower(${p_search})))` | Always true when search parameter is empty; else case-insensitive contains. Use as a filter equals TRUE. |
| theme_quality_sentiment | `jsonExtractScalar({theme_sentiment_json}, '$.quality.sentiment')` | Returns VARCHAR. |
| theme_quality_count | `toInt(jsonExtractScalar({theme_sentiment_json}, '$.quality.count'))` | Cast to INT. |
| theme_performance_sentiment | `jsonExtractScalar({theme_sentiment_json}, '$.performance.sentiment')` | — |
| theme_performance_count | `toInt(jsonExtractScalar({theme_sentiment_json}, '$.performance.count'))` | — |
| theme_design_sentiment | `jsonExtractScalar({theme_sentiment_json}, '$.design.sentiment')` | — |
| theme_design_count | `toInt(jsonExtractScalar({theme_sentiment_json}, '$.design.count'))` | — |
| theme_support_sentiment | `jsonExtractScalar({theme_sentiment_json}, '$.support.sentiment')` | — |
| theme_support_count | `toInt(jsonExtractScalar({theme_sentiment_json}, '$.support.count'))` | — |
| theme_value_sentiment | `jsonExtractScalar({theme_sentiment_json}, '$.value.sentiment')` | — |
| theme_value_count | `toInt(jsonExtractScalar({theme_sentiment_json}, '$.value.count'))` | — |
| quality_negative_flag | `ifelse(lower({theme_quality_sentiment})='negative',1,0)` | Repeat for other themes if needed. |
| performance_negative_flag | `ifelse(lower({theme_performance_sentiment})='negative',1,0)` | — |
| metric_selected | `ifelse(${p_metric}='Total Reviews', sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral}), ${p_metric}='Positive Rate', avg({positive_rate_agg}), ${p_metric}='Negative Rate', avg({negative_rate_agg}), ${p_metric}='Net Sentiment', avg({net_sentiment}), ${p_metric}='Positivity Index', avg({positivity_index_0_100}), sum({reviews_positive}) + sum({reviews_negative}) + sum({reviews_mixed}) + sum({reviews_neutral}))` | Keeps each branch aggregate-safe. |
| summary_140 | `ifelse(length({summary})>140, concat(substring({summary},1,140),'…'), {summary})` | Use in tooltips and tables. |

Notes:
- Dataset-level: Simple row‑level calcs (e.g., `total_reviews`, `orientation_short`).
- Analysis-level (inside an analysis): Window/period functions (e.g., `denseRank`, `sumOver`, `periodOverPeriodDifference`).
- When you see “mismatched aggregation,” switch to the `_agg` variants or wrap all operands in the same aggregation.
- Ensure `summary_date` is a Date type. If needed, parse strings or truncate `now()` to day for dateDiff.

---

### 9.2 Visuals Blueprint (10 visuals)

1. KPI Tile Strip (Top Row)
   - Measures: `sum(total_reviews_agg)`, `avg(positivity_index_0_100)` (or `_agg`), `avg(positive_rate_agg)`, `avg(negative_rate_agg)`, `avg(net_sentiment)`
   - Conditional formatting:
     - Positive rate > 0.60 = green
     - Positive rate 0.40–0.60 = amber
     - Positive rate < 0.40 = red
     - Negative rate > 0.40 = red

2. Time Series: Net Sentiment Over Time
   - X: `summary_date`
   - Y: `avg(net_sentiment)`
   - Color (optional): `orientation_short`
   - Enable Forecast (next 7–14 days) and Anomaly Detection (Insights).

3. Stacked Bar: Orientation Mix by Product (Top N)
   - Group: `product_id`
   - Values: `sum(reviews_positive)`, `sum(reviews_negative)`, `sum(reviews_mixed)`, `sum(reviews_neutral)`
   - Sort: `sum(total_reviews_agg)` descending
   - Filter: Top `${p_top_n}` by `sum(total_reviews_agg)`

4. Scatter: Positive vs Negative by Product
   - X: `avg(positive_rate_agg)`
   - Y: `avg(negative_rate_agg)`
   - Size: `total_reviews_agg`
   - Color: `orientation_short`
   - Tooltip: `summary_140`, `positivity_index_0_100`

5. Table: Latest Summaries (One Row per Product)
   - Filter: `latest_flag_by_product = 1`
   - Columns: `product_id`, `summary_date`, `orientation_short`, `total_reviews_agg`, `positive_rate_agg`, `negative_rate_agg`, `net_sentiment`, `summary`
   - Conditional formatting: `orientation_short` (green/amber/red)
   - (Optional) URL action to open the S3 JSON for the selected product/date.

6. Heatmap: Theme Negativity by Product
   - Rows: `product_id`
   - Columns: `quality`, `performance`, `design`, `support`, `value`
   - Values: corresponding `theme_*_count`
   - Color rules: red if `theme_*_sentiment='negative'`, green if `'positive'`, neutral otherwise.

7. Small‑Multiples Line Charts: Weekly Trend per Product
   - Small multiple field: `product_id` (Top `${p_top_n}`)
   - X: `week_start`
   - Y: `avg(net_sentiment)` or `avg(positive_rate_agg)`

8. Bar: Dynamic Metric by Product (Driven by Parameter)
   - X: `product_id`
   - Y: `metric_selected`
   - Title bound to `${p_metric}`
   - Sort: `metric_selected` descending

9. Watchlist (Quality & Support)
   - Bar: Top N by `avg(negative_rate_agg)` or `sum(reviews_negative)`
   - Table filter: `negative_rate_agg > 0.4` OR `theme_quality_sentiment='negative'` OR `theme_support_sentiment='negative'`
   - Highlight rows with high `negative_rate_agg`.

10. Word Cloud (Optional)
    - Field: `summary` (respect global filters)
    - Exclude stopwords in visual configuration to surface topical words.

---

### 9.3 Quick Tips

- If CASE/LOWER isn’t accepted in dataset-level edits, switch to analysis-level and/or use exact match equals.
- Keep operands either all aggregated or all non‑aggregated in a single calc (use `_agg` variants to help).
- For `dateDiff/truncDate`, ensure both operands are DATE/TIMESTAMP (truncate `now()` to day) to avoid type errors.
- When window functions are disabled in dataset prep, add them in the analysis.
---------------

## 10. Cost (Actual Structure Used)

| Service | Notes |
|---------|-------|
| S3 | Storage of uploads + daily JSON summaries (small). |
| Lambda | Short executions for ingestion + daily summarization. |
| Comprehend | Called per review; low volume manageable. |
| DynamoDB | On‑demand tables; modest provision at our scale. |
| SageMaker | Continuous `ml.m5.large` endpoint (main fixed cost). |
| Athena | Partition projection reduces overhead; minimal scan cost. |
| QuickSight | SPICE dataset; author license cost. |
| CloudWatch Logs | 14‑day retention to limit growth. |

---

## 11. Repository Contents (Must Include)

```
lambdas/
  review_processor/handler.py
  daily_summarizer/handler.py
docs/
  architecture.png
sample_data/
  reviews.csv
sql/
  athena_ddl.sql
README.md
```

(Only implemented code and docs—no placeholders for unbuilt features.)

---

## 12. Quick Start Recap

1. Upload CSV to `uploads/`.
2. Check DynamoDB `ReviewAnalysis` for enriched items.
3. (Wait for cron or manually invoke DailySummarizer).
4. Confirm summaries in `ReviewSummaries` and S3 partition path.
5. Query Athena view.
6. Refresh QuickSight dashboard.

---

## 13. Troubleshooting (Observed Cases)

| Issue | Cause | Fix |
|-------|-------|-----|
| No Lambda trigger | Misconfigured S3 event prefix/suffix | Re-add trigger with `uploads/` and `.csv` suffix |
| Missing sentiment | COMPREHEND_ENABLED false or IAM missing | Set env var true; add Comprehend permissions |
| Empty summaries | No reviews in last 24h | Upload new CSV or adjust time window |
| Polishing failure | Endpoint name/content-type mismatch | Confirm endpoint name; use `application/json` payload |
| Athena returns zero | Date folder missing or outside projection range | Ensure correct `date=YYYY-MM-DD` path; adjust projection range |

---

Everything above reflects only what we implemented—no speculative or future features included.

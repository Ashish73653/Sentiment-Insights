# Sentiment Insights

End‑to‑end AWS serverless pipeline we actually built and ran:
Upload review CSVs → Automatic NLP enrichment → Daily product summarization polished with a live SageMaker DistilBART CNN 6‑6 endpoint → Stored in DynamoDB and S3 → Queried with Athena (partition projection) → Visualized in QuickSight.

---

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

## 9. Cost (Actual Structure Used)

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

## 10. Repository Contents (Must Include)

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

## 11. Quick Start Recap

1. Upload CSV to `uploads/`.
2. Check DynamoDB `ReviewAnalysis` for enriched items.
3. (Wait for cron or manually invoke DailySummarizer).
4. Confirm summaries in `ReviewSummaries` and S3 partition path.
5. Query Athena view.
6. Refresh QuickSight dashboard.

---

## 12. Troubleshooting (Observed Cases)

| Issue | Cause | Fix |
|-------|-------|-----|
| No Lambda trigger | Misconfigured S3 event prefix/suffix | Re-add trigger with `uploads/` and `.csv` suffix |
| Missing sentiment | COMPREHEND_ENABLED false or IAM missing | Set env var true; add Comprehend permissions |
| Empty summaries | No reviews in last 24h | Upload new CSV or adjust time window |
| Polishing failure | Endpoint name/content-type mismatch | Confirm endpoint name; use `application/json` payload |
| Athena returns zero | Date folder missing or outside projection range | Ensure correct `date=YYYY-MM-DD` path; adjust projection range |

---

Everything above reflects only what we implemented—no speculative or future features included.

# # handler.py (updated)
# import os
# import json
# import csv
# import io
# import logging
# import uuid
# import hashlib
# from datetime import datetime, timezone
# from decimal import Decimal
# from urllib.parse import unquote_plus
# import time
# import traceback
# import boto3
# from botocore.exceptions import ClientError

# # --- Configuration (update names if needed) ---
# REVIEW_TABLE = os.environ.get("REVIEW_TABLE", "ReviewAnalysis")
# FAILURE_TABLE = os.environ.get("FAILURE_TABLE", "ReviewProcessingFailures")  # optional
# DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")
# COMPREHEND_ENABLED = os.environ.get("COMPREHEND_ENABLED", "true").lower() == "true"
# FAILURES_S3_BUCKET = os.environ.get("FAILURES_S3_BUCKET", None)  # optional fallback bucket

# # --- AWS clients/resources ---
# s3 = boto3.client("s3")
# comprehend = boto3.client("comprehend")
# dynamodb = boto3.resource("dynamodb")
# review_table = dynamodb.Table(REVIEW_TABLE)
# failure_table = dynamodb.Table(FAILURE_TABLE)

# # --- Logging ---
# LOG = logging.getLogger()
# LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# # --- Helpers ---
# def convert_floats_to_decimal(obj):
#     """
#     Recursively convert any float in dict/list to Decimal for DynamoDB.
#     Use Decimal(str(f)) to avoid binary float precision issues.
#     """
#     if isinstance(obj, dict):
#         return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
#     if isinstance(obj, list):
#         return [convert_floats_to_decimal(v) for v in obj]
#     if isinstance(obj, float):
#         return Decimal(str(obj))
#     # leave Decimal alone
#     if isinstance(obj, Decimal):
#         return obj
#     return obj

# def compute_hash(text: str) -> str:
#     return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

# def safe_comprehend_call(fn, *args, **kwargs):
#     """
#     Call a Comprehend API function (detect_sentiment/detect_key_phrases).
#     If COMPREHEND_ENABLED is False or a subscription/access error occurs, return None.
#     """
#     if not COMPREHEND_ENABLED:
#         LOG.debug("Comprehend disabled via env var; skipping call to %s", getattr(fn, "__name__", str(fn)))
#         return None
#     try:
#         return fn(*args, **kwargs)
#     except ClientError as e:
#         code = e.response.get("Error", {}).get("Code")
#         msg = e.response.get("Error", {}).get("Message")
#         # Common when service not activated: SubscriptionRequiredException
#         LOG.warning("Comprehend call failed: code=%s message=%s", code, msg)
#         return None
#     except Exception as e:
#         LOG.exception("Unexpected error calling Comprehend: %s", e)
#         return None

# def parse_csv_bytes(content_bytes):
#     """
#     Parse CSV bytes into list of dict rows.
#     Expect header row. If no header, produce rows with 'text' key.
#     """
#     text = content_bytes.decode("utf-8")
#     f = io.StringIO(text)
#     reader = csv.DictReader(f)
#     rows = []
#     if not reader.fieldnames:
#         f.seek(0)
#         simple_reader = csv.reader(f)
#         for r in simple_reader:
#             if len(r) == 0:
#                 continue
#             rows.append({"text": r[0]})
#     else:
#         for r in reader:
#             rows.append(r)
#     return rows

# def epoch_seconds_decimal():
#     """Return current epoch seconds as int (suitable for numeric GSI keys)."""
#     return int(time.time())

# def iso_now():
#     # Return a timezone-aware ISO timestamp (e.g. 2025-11-11T14:20:04.776467+00:00)
#     return datetime.now(timezone.utc).isoformat()

# def build_item_from_row(row, s3_key, default_lang=DEFAULT_LANG):
#     """
#     Build a DynamoDB item for a single review row.
#     Uses numeric ProcessedTimestampEpoch (Number) and a readable ISO copy.
#     """
#     # normalize keys (case-insensitive common variants)
#     text = (row.get("text") or row.get("review_text") or row.get("ReviewText") or row.get("review") or "").strip()
#     review_id = (row.get("review_id") or row.get("id") or row.get("ReviewID") or row.get("reviewId") or str(uuid.uuid4()))
#     product_id = (row.get("product_id") or row.get("productId") or row.get("ProductID") or row.get("product") or "unknown")

#     sentiment = "NOT_AVAILABLE"
#     sentiment_scores = {}
#     key_phrases = []
#     lang_code = row.get("language") or row.get("lang") or default_lang

#     if text:
#         # attempt to call Comprehend (may return None if service not available)
#         resp = safe_comprehend_call(comprehend.detect_sentiment, Text=text, LanguageCode=lang_code)
#         if resp:
#             sentiment = resp.get("Sentiment", "NOT_AVAILABLE")
#             sentiment_scores = resp.get("SentimentScore", {})

#         kp_resp = safe_comprehend_call(comprehend.detect_key_phrases, Text=text, LanguageCode=lang_code)
#         if kp_resp:
#             key_phrases = [p.get("Text") for p in kp_resp.get("KeyPhrases", []) if p.get("Text")]

#     processed_ts_num = epoch_seconds_decimal()
#     processed_ts_iso = iso_now()

#     item = {
#         "PK": f"REVIEW#{review_id}",
#         "SK": f"REVIEW#{processed_ts_iso}#{str(uuid.uuid4())}",
#         "ReviewID": review_id,
#         "ProductID": product_id,
#         "SourceS3Key": s3_key,
#         "RedactedText": text,
#         # IMPORTANT: numeric epoch for GSI lookups (Number type)
#         "ProcessedTimestampEpoch": processed_ts_num,
#         # readable ISO for debugging / display
#         "ProcessedTimestampIso": processed_ts_iso,
#         "Sentiment": sentiment,
#         "SentimentScores": sentiment_scores,
#         "KeyPhrases": key_phrases,
#         "OriginalHash": compute_hash(text),
#     }

#     safe_item = convert_floats_to_decimal(item)
#     return safe_item

# def write_failure_record(original_row, s3_key, reason):
#     """
#     Write a failure record to the failure table; if that fails, fallback to S3 (if configured).
#     """
#     rec = {
#         "PK": f"FAIL#{str(uuid.uuid4())}",
#         "SK": f"FAIL#{iso_now()}",
#         "SourceS3Key": s3_key,
#         "RowData": original_row,
#         "Reason": reason,
#         "Timestamp": iso_now()
#     }

#     try:
#         rec_ddb = convert_floats_to_decimal(rec)
#         failure_table.put_item(Item=rec_ddb)
#         LOG.info("Wrote failure record to DDB table %s PK=%s", FAILURE_TABLE, rec["PK"])
#         return
#     except ClientError as e:
#         code = e.response.get("Error", {}).get("Code")
#         LOG.warning("Failed to write failure record to DynamoDB (code=%s). Falling back to S3. msg=%s", code, e)
#     except Exception as e:
#         LOG.exception("Unexpected error writing failure to DDB — falling back to S3: %s", e)

#     if FAILURES_S3_BUCKET:
#         try:
#             key = f"failures/{iso_now()}-{str(uuid.uuid4())}.json"
#             body = json.dumps(rec, default=str)
#             s3.put_object(Bucket=FAILURES_S3_BUCKET, Key=key, Body=body.encode("utf-8"))
#             LOG.info("Wrote failure record to S3 s3://%s/%s", FAILURES_S3_BUCKET, key)
#             return
#         except Exception as e:
#             LOG.exception("Failed to write failure record to S3 fallback: %s", e)

#     LOG.error("No persistence available for failure record. Rec: %s", json.dumps(rec, default=str))
#     LOG.error("Stacktrace: %s", traceback.format_exc())

# # --- Lambda handler ---
# def lambda_handler(event, context):
#     LOG.info("Event received: %s", json.dumps(event))

#     records = event.get("Records", [])
#     if not records:
#         LOG.error("No records in event")
#         return {"status": "no_records"}

#     successes = 0
#     failures = 0

#     for rec in records:
#         s3_info = rec.get("s3") or {}
#         bucket = s3_info.get("bucket", {}).get("name")
#         key = s3_info.get("object", {}).get("key")
#         if not bucket or not key:
#             LOG.warning("Skipping record without bucket/key: %s", rec)
#             continue

#         s3_key = unquote_plus(key)
#         LOG.info("Processing s3://%s/%s", bucket, s3_key)

#         # Download object
#         try:
#             obj = s3.get_object(Bucket=bucket, Key=s3_key)
#             body = obj["Body"].read()
#         except ClientError as e:
#             code = e.response.get("Error", {}).get("Code")
#             if code == "NoSuchKey":
#                 LOG.error("S3 object not found: s3://%s/%s", bucket, s3_key)
#                 write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, "NoSuchKey")
#                 failures += 1
#                 continue
#             else:
#                 LOG.exception("Failed to read S3 object: %s", e)
#                 write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"S3GetError:{code}")
#                 failures += 1
#                 continue
#         except Exception as e:
#             LOG.exception("Unexpected error downloading s3 object: %s", e)
#             write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"S3GetUnexpected:{e}")
#             failures += 1
#             continue

#         # Parse CSV
#         try:
#             rows = parse_csv_bytes(body)
#             LOG.info("Parsed %d rows from %s", len(rows), s3_key)
#         except Exception as e:
#             LOG.exception("Failed to parse CSV: %s", e)
#             write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"CSVParseError:{e}")
#             failures += 1
#             continue

#         # Build items
#         items_to_write = []
#         for idx, row in enumerate(rows):
#             try:
#                 item = build_item_from_row(row, s3_key)
#                 items_to_write.append(item)
#             except Exception as e:
#                 LOG.exception("Failed building item for row %s: %s", idx, e)
#                 write_failure_record(row, s3_key, f"BuildItemError:{e}")
#                 failures += 1

#         # Batch write to DynamoDB (batch_writer handles chunking)
#         if items_to_write:
#             try:
#                 with review_table.batch_writer() as batch:
#                     for it in items_to_write:
#                         batch.put_item(Item=it)
#                 LOG.info("Wrote %d items to %s", len(items_to_write), REVIEW_TABLE)
#                 successes += len(items_to_write)
#             except TypeError as te:
#                 LOG.warning("Batch write TypeError (likely float issue). Attempting per-item put_item. %s", te)
#                 for it in items_to_write:
#                     try:
#                         safe_it = convert_floats_to_decimal(it)
#                         review_table.put_item(Item=safe_it)
#                         successes += 1
#                     except Exception as e:
#                         LOG.exception("put_item failed for item: %s", e)
#                         write_failure_record(it, s3_key, f"DynamoPutError:{e}")
#                         failures += 1
#             except ClientError as ce:
#                 # Likely DynamoDB validation errors or access issues. Try per-item and record failures.
#                 LOG.exception("DynamoDB ClientError during batch write: %s", ce)
#                 for it in items_to_write:
#                     try:
#                         safe_it = convert_floats_to_decimal(it)
#                         review_table.put_item(Item=safe_it)
#                         successes += 1
#                     except Exception as e:
#                         LOG.exception("put_item failed for item: %s", e)
#                         write_failure_record(it, s3_key, f"DynamoPutError:{e}")
#                         failures += 1
#             except Exception as e:
#                 LOG.exception("Unexpected error writing to DynamoDB: %s", e)
#                 for it in items_to_write:
#                     write_failure_record(it, s3_key, f"DynamoUnknownWriteError:{e}")
#                 failures += len(items_to_write)

#     LOG.info("Processing complete. successes=%d failures=%d", successes, failures)
#     return {"status": "finished", "successes": successes, "failures": failures}


# # ------------------------------
# # Optional helper scripts (run separately, not invoked during Lambda)
# # ------------------------------
# def scan_table_for_bad_timestamp(region_name="us-east-1", page_limit=1000):
#     """
#     Scan the table and print items missing ProcessedTimestampEpoch (i.e. bad items).
#     Run this locally (make sure AWS creds configured) or in a management Lambda.
#     """
#     client = boto3.client("dynamodb", region_name=region_name)
#     table = REVIEW_TABLE
#     LOG.info("Scanning table %s for items missing ProcessedTimestampEpoch...", table)
#     paginator = client.get_paginator("scan")
#     bad_items = []
#     for page in paginator.paginate(TableName=table, ProjectionExpression="PK,SK,ProcessedTimestampEpoch,ProcessedTimestampIso"):
#         for it in page.get("Items", []):
#             if "ProcessedTimestampEpoch" not in it:
#                 bad_items.append({"PK": it["PK"], "SK": it["SK"], "ProcessedTimestampIso": it.get("ProcessedTimestampIso")})
#     LOG.info("Found %d items missing ProcessedTimestampEpoch", len(bad_items))
#     return bad_items

# def delete_items_by_key(keys, region_name="us-east-1"):
#     """
#     Delete list of items given keys = [{'PK': {'S': '...'}, 'SK': {'S': '...'}}, ...]
#     This is a low-level boto3 client call. Use with extreme caution.
#     """
#     client = boto3.client("dynamodb", region_name=region_name)
#     table = REVIEW_TABLE
#     for k in keys:
#         try:
#             client.delete_item(TableName=table, Key={"PK": k["PK"], "SK": k["SK"]})
#             LOG.info("Deleted item %s / %s", k["PK"], k["SK"])
#         except Exception as e:
#             LOG.exception("Failed to delete %s/%s: %s", k["PK"], k["SK"], e)
# handler.py (updated to support "By Day" GSI attributes and safer Comprehend calls)
import os
import json
import csv
import io
import logging
import uuid
import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import unquote_plus
import time
import traceback
import boto3
from botocore.exceptions import ClientError

# --- Configuration (update names if needed) ---
REVIEW_TABLE = os.environ.get("REVIEW_TABLE", "ReviewAnalysis")
FAILURE_TABLE = os.environ.get("FAILURE_TABLE", "ReviewProcessingFailures")  # optional
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")
COMPREHEND_ENABLED = os.environ.get("COMPREHEND_ENABLED", "true").lower() == "true"
FAILURES_S3_BUCKET = os.environ.get("FAILURES_S3_BUCKET", None)  # optional fallback bucket
MAX_COMPREHEND_TEXT_LEN = int(os.environ.get("MAX_COMPREHEND_TEXT_LEN", "4500"))  # Comprehend limit safety

# --- AWS clients/resources ---
s3 = boto3.client("s3")
comprehend = boto3.client("comprehend")
dynamodb = boto3.resource("dynamodb")
review_table = dynamodb.Table(REVIEW_TABLE)
failure_table = dynamodb.Table(FAILURE_TABLE)

# --- Logging ---
LOG = logging.getLogger()
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# --- Helpers ---
def convert_floats_to_decimal(obj):
    """
    Recursively convert any float in dict/list to Decimal for DynamoDB.
    Use Decimal(str(f)) to avoid binary float precision issues.
    """
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimal(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    # leave Decimal alone
    if isinstance(obj, Decimal):
        return obj
    return obj

def compute_hash(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

def truncate_for_comprehend(text: str) -> str:
    """
    Comprehend single-document APIs accept up to 5000 bytes.
    Keep a safety margin via MAX_COMPREHEND_TEXT_LEN (chars).
    """
    if not text:
        return text
    if len(text) <= MAX_COMPREHEND_TEXT_LEN:
        return text
    return text[:MAX_COMPREHEND_TEXT_LEN]

def safe_comprehend_call(fn, *args, **kwargs):
    """
    Call a Comprehend API function (detect_sentiment/detect_key_phrases).
    If COMPREHEND_ENABLED is False or a subscription/access error occurs, return None.
    """
    if not COMPREHEND_ENABLED:
        LOG.debug("Comprehend disabled via env var; skipping call to %s", getattr(fn, "__name__", str(fn)))
        return None
    try:
        return fn(*args, **kwargs)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        msg = e.response.get("Error", {}).get("Message")
        # Common when service not activated: SubscriptionRequiredException
        LOG.warning("Comprehend call failed: code=%s message=%s", code, msg)
        return None
    except Exception as e:
        LOG.exception("Unexpected error calling Comprehend: %s", e)
        return None

def parse_csv_bytes(content_bytes):
    """
    Parse CSV bytes into list of dict rows.
    Expect header row. If no header, produce rows with 'text' key.
    """
    text = content_bytes.decode("utf-8")
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = []
    if not reader.fieldnames:
        f.seek(0)
        simple_reader = csv.reader(f)
        for r in simple_reader:
            if len(r) == 0:
                continue
            rows.append({"text": r[0]})
    else:
        for r in reader:
            rows.append(r)
    return rows

def epoch_seconds_decimal():
    """Return current epoch seconds as int (suitable for numeric GSI keys)."""
    return int(time.time())

def iso_now():
    # Return a timezone-aware ISO timestamp (e.g. 2025-11-11T14:20:04.776467+00:00)
    return datetime.now(timezone.utc).isoformat()

def build_item_from_row(row, s3_key, default_lang=DEFAULT_LANG):
    """
    Build a DynamoDB item for a single review row.
    Uses numeric ProcessedTimestampEpoch (Number) and a readable ISO copy.
    Also adds "By Day" GSI attributes:
      - GSI1PK = DAY#YYYY-MM-DD (UTC)
      - GSI1SK = {ProductID}#{ProcessedTimestampEpoch}#{ReviewID}
    """
    # normalize keys (case-insensitive common variants)
    text = (row.get("text") or row.get("review_text") or row.get("ReviewText") or row.get("review") or "").strip()
    review_id = (row.get("review_id") or row.get("id") or row.get("ReviewID") or row.get("reviewId") or str(uuid.uuid4()))
    product_id = (row.get("product_id") or row.get("productId") or row.get("ProductID") or row.get("product") or "unknown")

    # Comprehend (optional)
    sentiment = "NOT_AVAILABLE"
    sentiment_scores = {}
    key_phrases = []
    lang_code = row.get("language") or row.get("lang") or default_lang

    if text:
        t = truncate_for_comprehend(text)
        # attempt to call Comprehend (may return None if service not available)
        resp = safe_comprehend_call(comprehend.detect_sentiment, Text=t, LanguageCode=lang_code)
        if resp:
            sentiment = resp.get("Sentiment", "NOT_AVAILABLE")
            sentiment_scores = resp.get("SentimentScore", {})

        kp_resp = safe_comprehend_call(comprehend.detect_key_phrases, Text=t, LanguageCode=lang_code)
        if kp_resp:
            key_phrases = [p.get("Text") for p in kp_resp.get("KeyPhrases", []) if p.get("Text")]

    processed_ts_num = epoch_seconds_decimal()
    processed_ts_iso = iso_now()
    date_utc = processed_ts_iso.split("T", 1)[0]  # YYYY-MM-DD (UTC)

    item = {
        "PK": f"REVIEW#{review_id}",
        "SK": f"REVIEW#{processed_ts_iso}#{str(uuid.uuid4())}",
        "ReviewID": review_id,
        "ProductID": product_id,
        "SourceS3Key": s3_key,
        "RedactedText": text,
        # IMPORTANT: numeric epoch for GSI lookups (Number type)
        "ProcessedTimestampEpoch": processed_ts_num,
        # readable ISO for debugging / display
        "ProcessedTimestampIso": processed_ts_iso,
        "Sentiment": sentiment,
        "SentimentScores": sentiment_scores,
        "KeyPhrases": key_phrases,
        "OriginalHash": compute_hash(text),

        # --- Added: "By Day" GSI attributes (optional index) ---
        # Create a GSI with:
        #   IndexName: GSI1_Day
        #   Partition key: GSI1PK  (String)
        #   Sort key:      GSI1SK  (String)
        # Then the summarizer can: Query GSI1PK = "DAY#YYYY-MM-DD"
        "GSI1PK": f"DAY#{date_utc}",
        "GSI1SK": f"{product_id}#{processed_ts_num}#{review_id}",
    }

    safe_item = convert_floats_to_decimal(item)
    return safe_item

def write_failure_record(original_row, s3_key, reason):
    """
    Write a failure record to the failure table; if that fails, fallback to S3 (if configured).
    """
    rec = {
        "PK": f"FAIL#{str(uuid.uuid4())}",
        "SK": f"FAIL#{iso_now()}",
        "SourceS3Key": s3_key,
        "RowData": original_row,
        "Reason": reason,
        "Timestamp": iso_now()
    }

    try:
        rec_ddb = convert_floats_to_decimal(rec)
        failure_table.put_item(Item=rec_ddb)
        LOG.info("Wrote failure record to DDB table %s PK=%s", FAILURE_TABLE, rec["PK"])
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        LOG.warning("Failed to write failure record to DynamoDB (code=%s). Falling back to S3. msg=%s", code, e)
    except Exception as e:
        LOG.exception("Unexpected error writing failure to DDB — falling back to S3: %s", e)

    if FAILURES_S3_BUCKET:
        try:
            key = f"failures/{iso_now()}-{str(uuid.uuid4())}.json"
            body = json.dumps(rec, default=str)
            s3.put_object(Bucket=FAILURES_S3_BUCKET, Key=key, Body=body.encode("utf-8"))
            LOG.info("Wrote failure record to S3 s3://%s/%s", FAILURES_S3_BUCKET, key)
            return
        except Exception as e:
            LOG.exception("Failed to write failure record to S3 fallback: %s", e)

    LOG.error("No persistence available for failure record. Rec: %s", json.dumps(rec, default=str))
    LOG.error("Stacktrace: %s", traceback.format_exc())

# --- Lambda handler ---
def lambda_handler(event, context):
    LOG.info("Event received: %s", json.dumps(event))

    records = event.get("Records", [])
    if not records:
        LOG.error("No records in event")
        return {"status": "no_records"}

    successes = 0
    failures = 0

    for rec in records:
        s3_info = rec.get("s3") or {}
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        if not bucket or not key:
            LOG.warning("Skipping record without bucket/key: %s", rec)
            continue

        s3_key = unquote_plus(key)
        LOG.info("Processing s3://%s/%s", bucket, s3_key)

        # Download object
        try:
            obj = s3.get_object(Bucket=bucket, Key=s3_key)
            body = obj["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "NoSuchKey":
                LOG.error("S3 object not found: s3://%s/%s", bucket, s3_key)
                write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, "NoSuchKey")
                failures += 1
                continue
            else:
                LOG.exception("Failed to read S3 object: %s", e)
                write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"S3GetError:{code}")
                failures += 1
                continue
        except Exception as e:
            LOG.exception("Unexpected error downloading s3 object: %s", e)
            write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"S3GetUnexpected:{e}")
            failures += 1
            continue

        # Parse CSV
        try:
            rows = parse_csv_bytes(body)
            LOG.info("Parsed %d rows from %s", len(rows), s3_key)
        except Exception as e:
            LOG.exception("Failed to parse CSV: %s", e)
            write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"CSVParseError:{e}")
            failures += 1
            continue

        # Build items
        items_to_write = []
        for idx, row in enumerate(rows):
            try:
                item = build_item_from_row(row, s3_key)
                items_to_write.append(item)
            except Exception as e:
                LOG.exception("Failed building item for row %s: %s", idx, e)
                write_failure_record(row, s3_key, f"BuildItemError:{e}")
                failures += 1

        # Batch write to DynamoDB (batch_writer handles chunking)
        if items_to_write:
            try:
                with review_table.batch_writer() as batch:
                    for it in items_to_write:
                        batch.put_item(Item=it)
                LOG.info("Wrote %d items to %s", len(items_to_write), REVIEW_TABLE)
                successes += len(items_to_write)
            except TypeError as te:
                LOG.warning("Batch write TypeError (likely float issue). Attempting per-item put_item. %s", te)
                for it in items_to_write:
                    try:
                        safe_it = convert_floats_to_decimal(it)
                        review_table.put_item(Item=safe_it)
                        successes += 1
                    except Exception as e:
                        LOG.exception("put_item failed for item: %s", e)
                        write_failure_record(it, s3_key, f"DynamoPutError:{e}")
                        failures += 1
            except ClientError as ce:
                # Likely DynamoDB validation errors or access issues. Try per-item and record failures.
                LOG.exception("DynamoDB ClientError during batch write: %s", ce)
                for it in items_to_write:
                    try:
                        safe_it = convert_floats_to_decimal(it)
                        review_table.put_item(Item=safe_it)
                        successes += 1
                    except Exception as e:
                        LOG.exception("put_item failed for item: %s", e)
                        write_failure_record(it, s3_key, f"DynamoPutError:{e}")
                        failures += 1
            except Exception as e:
                LOG.exception("Unexpected error writing to DynamoDB: %s", e)
                for it in items_to_write:
                    write_failure_record(it, s3_key, f"DynamoUnknownWriteError:{e}")
                failures += len(items_to_write)

    LOG.info("Processing complete. successes=%d failures=%d", successes, failures)
    return {"status": "finished", "successes": successes, "failures": failures}


# ------------------------------
# Optional helper scripts (run separately, not invoked during Lambda)
# ------------------------------
def scan_table_for_bad_timestamp(region_name="us-east-1", page_limit=1000):
    """
    Scan the table and print items missing ProcessedTimestampEpoch (i.e. bad items).
    Run this locally (make sure AWS creds configured) or in a management Lambda.
    """
    client = boto3.client("dynamodb", region_name=region_name)
    table = REVIEW_TABLE
    LOG.info("Scanning table %s for items missing ProcessedTimestampEpoch...", table)
    paginator = client.get_paginator("scan")
    bad_items = []
    for page in paginator.paginate(TableName=table, ProjectionExpression="PK,SK,ProcessedTimestampEpoch,ProcessedTimestampIso"):
        for it in page.get("Items", []):
            if "ProcessedTimestampEpoch" not in it:
                bad_items.append({"PK": it["PK"], "SK": it["SK"], "ProcessedTimestampIso": it.get("ProcessedTimestampIso")})
    LOG.info("Found %d items missing ProcessedTimestampEpoch", len(bad_items))
    return bad_items

def delete_items_by_key(keys, region_name="us-east-1"):
    """
    Delete list of items given keys = [{'PK': {'S': '...'}, 'SK': {'S': '...'}}, ...]
    This is a low-level boto3 client call. Use with extreme caution.
    """
    client = boto3.client("dynamodb", region_name=region_name)
    table = REVIEW_TABLE
    for k in keys:
        try:
            client.delete_item(TableName=table, Key={"PK": k["PK"], "SK": k["SK"]})
            LOG.info("Deleted item %s / %s", k["PK"], k["SK"])
        except Exception as e:
            LOG.exception("Failed to delete %s/%s: %s", k["PK"], k["SK"], e)
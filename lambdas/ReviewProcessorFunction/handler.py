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
import re
import boto3
from botocore.exceptions import ClientError

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
REVIEW_TABLE = os.environ.get("REVIEW_TABLE", "ReviewAnalysis")
FAILURE_TABLE = os.environ.get("FAILURE_TABLE", "ReviewProcessingFailures")
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")
COMPREHEND_ENABLED = os.environ.get("COMPREHEND_ENABLED", "true").lower() == "true"
FAILURES_S3_BUCKET = os.environ.get("FAILURES_S3_BUCKET", None)
MAX_COMPREHEND_TEXT_LEN = int(os.environ.get("MAX_COMPREHEND_TEXT_LEN", "4500"))

# PII redaction
PII_REDACTION_ENABLED = os.environ.get("PII_REDACTION_ENABLED", "true").lower() == "true"
PII_TYPES_ENV = os.environ.get(
    "PII_TYPES",
    "EMAIL,PHONE,ADDRESS,CREDIT_DEBIT_NUMBER,CREDIT_DEBIT_CVV,BANK_ACCOUNT_NUMBER,NAME"
)
PII_TYPES = {t.strip().upper() for t in PII_TYPES_ENV.split(",") if t.strip()}
PII_MASK_STYLE = os.environ.get("PII_MASK_STYLE", "FIXED").upper()  # FIXED or TYPE
PII_MIN_SCORE = float(os.environ.get("PII_MIN_SCORE", "0.3"))

CUSTOM_PII_FALLBACK_ENABLED = os.environ.get("CUSTOM_PII_FALLBACK_ENABLED", "true").lower() == "true"
BANK_ACCOUNT_MIN_DIGITS = int(os.environ.get("BANK_ACCOUNT_MIN_DIGITS", "10"))

# Alerts (SNS) configuration (raw value first; will normalize)
ALERTS_ENABLED = os.environ.get("ALERTS_ENABLED", "false").lower() == "true"
ALERT_SNS_TOPIC_ARN_RAW = os.environ.get("ALERT_SNS_TOPIC_ARN", "")
ALERT_NEGATIVE_THRESHOLD = float(os.environ.get("ALERT_NEGATIVE_THRESHOLD", "0.9"))
ALERT_ALLOW_MIXED = os.environ.get("ALERT_ALLOW_MIXED", "false").lower() == "true"
ALERT_MIN_REDACTIONS = int(os.environ.get("ALERT_MIN_REDACTIONS", "1"))
ALERT_KEYWORDS = {
    k.strip().lower()
    for k in os.environ.get(
        "ALERT_KEYWORDS",
        "refund,return,unsafe,overheat,fire,leak,shocked,broken,scam,fraud,privacy"
    ).split(",")
    if k.strip()
}

# --------------------------------------------------------------------
# AWS clients
# --------------------------------------------------------------------
s3 = boto3.client("s3")
comprehend = boto3.client("comprehend")
dynamodb = boto3.resource("dynamodb")
review_table = dynamodb.Table(REVIEW_TABLE)
failure_table = dynamodb.Table(FAILURE_TABLE)
sns = boto3.client("sns")

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------
LOG = logging.getLogger()
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def convert_floats_to_decimal(obj):
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimal(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, Decimal):
        return obj
    return obj

def compute_hash(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

def truncate_for_comprehend(text: str) -> str:
    if not text:
        return text
    if len(text) <= MAX_COMPREHEND_TEXT_LEN:
        return text
    return text[:MAX_COMPREHEND_TEXT_LEN]

def safe_comprehend_call(fn, *args, **kwargs):
    if not COMPREHEND_ENABLED:
        LOG.debug("Comprehend disabled; skipping %s", getattr(fn, "__name__", str(fn)))
        return None
    try:
        return fn(*args, **kwargs)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        LOG.warning("Comprehend call failed: code=%s msg=%s", code, e.response.get("Error", {}).get("Message"))
        return None
    except Exception as e:
        LOG.exception("Unexpected Comprehend error: %s", e)
        return None

def parse_csv_bytes(content_bytes):
    text = content_bytes.decode("utf-8")
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = []
    if not reader.fieldnames:
        f.seek(0)
        simple_reader = csv.reader(f)
        for r in simple_reader:
            if r:
                rows.append({"text": r[0]})
    else:
        for r in reader:
            rows.append(r)
    return rows

def epoch_seconds_decimal():
    return int(time.time())

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def mask_replacement(entity_type: str) -> str:
    if PII_MASK_STYLE == "TYPE":
        return f"[REDACTED:{entity_type}]"
    return "[REDACTED]"

# --------------------------------------------------------------------
# PII Redaction (Comprehend)
# --------------------------------------------------------------------
def redact_pii(text: str, lang_code: str):
    if not text or not PII_REDACTION_ENABLED:
        return text, {"applied": False, "count": 0, "types": []}

    resp = safe_comprehend_call(
        comprehend.detect_pii_entities,
        Text=text,
        LanguageCode=lang_code or "en"
    )
    if not resp:
        LOG.warning("PII redaction skipped (no response).")
        return text, {"applied": False, "count": 0, "types": []}

    entities = resp.get("Entities", []) or []
    all_mode = "ALL" in PII_TYPES
    filt = [
        e for e in entities
        if e.get("Type") and (all_mode or e["Type"].upper() in PII_TYPES) and e.get("Score", 0) >= PII_MIN_SCORE
    ]
    if not filt:
        return text, {"applied": False, "count": 0, "types": []}

    filt.sort(key=lambda e: e.get("BeginOffset", 0), reverse=True)
    red_text = text
    red_types = set()
    for e in filt:
        start = e.get("BeginOffset")
        end = e.get("EndOffset")
        etype = (e.get("Type") or "PII").upper()
        if isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(red_text):
            red_text = red_text[:start] + mask_replacement(etype) + red_text[end:]
            red_types.add(etype)

    return red_text, {"applied": True, "count": len(filt), "types": sorted(red_types)}

# --------------------------------------------------------------------
# Custom Fallback (raw text before primary redaction)
# --------------------------------------------------------------------
FALLBACK_PATTERNS = [
    re.compile(r'\bACC[T]?[-–—: ]?\d{8,}\b', re.IGNORECASE),
    re.compile(r'\b\d{10,}\b'),
    re.compile(r'\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b'),
]

LABEL_CAPTURE = re.compile(
    r'(BANK_ACCOUNT_NUMBER\s*:\s*)([A-Z]{2}\d{2}[A-Z0-9]{10,30}|\bACC[T]?[-–—: ]?\d{8,}\b|\b\d{10,}\b)',
    re.IGNORECASE
)

def fallback_redact_bank_accounts(raw_text: str):
    if not CUSTOM_PII_FALLBACK_ENABLED or not raw_text:
        return raw_text, [], 0

    working = raw_text
    matched_any = False
    def _label_sub(m):
        nonlocal matched_any
        matched_any = True
        return m.group(1) + "[REDACTED]"
    working = LABEL_CAPTURE.sub(_label_sub, working)

    for rgx in FALLBACK_PATTERNS:
        def _generic_sub(m):
            val = m.group(0)
            if "[REDACTED]" in val:
                return val
            nonlocal matched_any
            matched_any = True
            return "[REDACTED]"
        working = rgx.sub(_generic_sub, working)

    types = ["BANK_ACCOUNT_NUMBER"] if matched_any else []
    count = working.count("[REDACTED]") - raw_text.count("[REDACTED]")
    return working, types, max(count, 0)

# --------------------------------------------------------------------
# SNS Topic ARN normalization
# --------------------------------------------------------------------
def normalize_topic_arn(raw_arn: str) -> str:
    """
    If a subscription ARN (topicArn:uuid) was provided, strip the trailing UUID.
    Topic ARN format: arn:aws:sns:region:account:topicName
    Subscription ARN format: arn:aws:sns:region:account:topicName:subscriptionUUID
    """
    if not raw_arn:
        return raw_arn
    parts = raw_arn.split(':')
    # subscription ARNs have 7 parts (including the UUID at the end)
    if len(parts) == 7:
        tail = parts[-1]
        if len(tail) >= 8 and '-' in tail:  # crude UUID check
            return ':'.join(parts[:-1])
    return raw_arn

ALERT_SNS_TOPIC_ARN = normalize_topic_arn(ALERT_SNS_TOPIC_ARN_RAW)
if ALERTS_ENABLED:
    LOG.info("Alerts enabled. Using SNS Topic ARN: %s (raw provided: %s)", ALERT_SNS_TOPIC_ARN, ALERT_SNS_TOPIC_ARN_RAW)

# --------------------------------------------------------------------
# Critical Alert Evaluation & Publish (SNS)
# --------------------------------------------------------------------
def evaluate_critical(text: str, sentiment_label: str, sentiment_scores: dict, key_phrases: list, redaction_count: int):
    reasons = []
    if not ALERTS_ENABLED or not ALERT_SNS_TOPIC_ARN:
        return False, reasons

    label = (sentiment_label or "").upper()
    neg = float((sentiment_scores or {}).get("Negative", 0) or 0)
    low_text = (text or "").lower()

    if label == "NEGATIVE" and neg >= ALERT_NEGATIVE_THRESHOLD:
        reasons.append(f"negative>={ALERT_NEGATIVE_THRESHOLD}")
    if ALERT_ALLOW_MIXED and label == "MIXED" and neg >= ALERT_NEGATIVE_THRESHOLD:
        reasons.append(f"mixed_negative>={ALERT_NEGATIVE_THRESHOLD}")

    if ALERT_KEYWORDS:
        if any(k in low_text for k in ALERT_KEYWORDS):
            reasons.append("keyword_in_text")
        else:
            for kp in (key_phrases or []):
                kl = (kp or "").lower()
                if any(k in kl for k in ALERT_KEYWORDS):
                    reasons.append("keyword_in_keyphrases")
                    break

    if ALERT_MIN_REDACTIONS > 0 and (redaction_count or 0) >= ALERT_MIN_REDACTIONS:
        reasons.append(f"redactions>={ALERT_MIN_REDACTIONS}")

    return (len(reasons) > 0), reasons

def publish_alert_sns(item: dict) -> bool:
    if not (ALERTS_ENABLED and ALERT_SNS_TOPIC_ARN):
        return False
    if not ALERT_SNS_TOPIC_ARN.startswith("arn:aws:sns:"):
        LOG.error("Invalid SNS Topic ARN format (after normalization): %s", ALERT_SNS_TOPIC_ARN)
        return False

    review_id = item.get("ReviewID")
    product_id = item.get("ProductID")
    iso_ts = item.get("ProcessedTimestampIso")
    s3_key = item.get("SourceS3Key")
    text = item.get("RedactedText", "")
    excerpt = (text[:280] + "…") if isinstance(text, str) and len(text) > 280 else (text or "")
    scores = item.get("SentimentScores", {}) or {}

    msg = {
        "type": "critical_review",
        "review_id": review_id,
        "product_id": product_id,
        "timestamp": iso_ts,
        "s3_key": s3_key,
        "sentiment": item.get("Sentiment"),
        "scores": {k: float(v) if isinstance(v, (int, float, Decimal)) else v for k, v in scores.items()},
        "excerpt": excerpt,
        "redaction_count": item.get("RedactionCount", 0),
        "redaction_types": item.get("RedactionTypes", []),
        "reasons": item.get("AlertReasons", [])
    }
    subject = f"[ALERT] Critical review for {product_id} (neg={scores.get('Negative', 0)})"
    try:
        resp = sns.publish(
            TopicArn=ALERT_SNS_TOPIC_ARN,
            Subject=str(subject)[:100],
            Message=json.dumps(msg, default=str),
            MessageAttributes={
                "product_id": {"DataType": "String", "StringValue": str(product_id)},
                "severity": {"DataType": "String", "StringValue": "critical"},
                "review_id": {"DataType": "String", "StringValue": str(review_id)}
            }
        )
        LOG.info("Alert publish success review_id=%s MessageId=%s reasons=%s",
                 review_id, resp.get("MessageId"), item.get("AlertReasons"))
        return True
    except ClientError as e:
        LOG.warning("SNS publish failed: %s", e)
        return False
    except Exception as e:
        LOG.exception("SNS unexpected error: %s", e)
        return False

def mark_and_publish_once(review_id: str, item: dict) -> None:
    if not (ALERTS_ENABLED and ALERT_SNS_TOPIC_ARN):
        return
    try:
        review_table.update_item(
            Key={"ReviewID": review_id},
            ConditionExpression="attribute_not_exists(AlertSent)",
            UpdateExpression="SET AlertSent = :t, AlertReasons = if_not_exists(AlertReasons, :r)",
            ExpressionAttributeValues={":t": True, ":r": item.get("AlertReasons", [])}
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return
        LOG.exception("Failed to mark AlertSent for %s: %s", review_id, e)
        return
    except Exception as e:
        LOG.exception("Unexpected error marking AlertSent for %s: %s", review_id, e)
        return

    published = publish_alert_sns(item)
    if not published:
        LOG.warning("SNS publish returned False for review_id=%s", review_id)

# --------------------------------------------------------------------
# Build Item
# --------------------------------------------------------------------
def build_item_from_row(row, s3_key, default_lang=DEFAULT_LANG):
    raw_text = (row.get("text") or row.get("review_text") or row.get("ReviewText") or
                row.get("review") or "").strip()
    review_id = (row.get("review_id") or row.get("id") or row.get("ReviewID") or
                 row.get("reviewId") or str(uuid.uuid4()))
    product_id = (row.get("product_id") or row.get("productId") or
                  row.get("ProductID") or row.get("product") or "unknown")
    lang_code = row.get("language") or row.get("lang") or default_lang

    fb_text, fb_types, fb_count = fallback_redact_bank_accounts(raw_text)
    redacted_text, redaction_info = redact_pii(fb_text, lang_code)

    sentiment = "NOT_AVAILABLE"
    sentiment_scores = {}
    key_phrases = []
    if redacted_text:
        t = truncate_for_comprehend(redacted_text)
        s_resp = safe_comprehend_call(comprehend.detect_sentiment, Text=t, LanguageCode=lang_code)
        if s_resp:
            sentiment = s_resp.get("Sentiment", "NOT_AVAILABLE")
            sentiment_scores = s_resp.get("SentimentScore", {})
        kp_resp = safe_comprehend_call(comprehend.detect_key_phrases, Text=t, LanguageCode=lang_code)
        if kp_resp:
            key_phrases = [p.get("Text") for p in kp_resp.get("KeyPhrases", []) if p.get("Text")]

    key_phrases = [p for p in key_phrases if "[REDACTED]" not in p]

    processed_ts_num = epoch_seconds_decimal()
    processed_ts_iso = iso_now()
    date_utc = processed_ts_iso.split("T", 1)[0]

    all_redaction_types = sorted(set(redaction_info.get("types", []) + fb_types))
    total_redaction_count = redaction_info.get("count", 0) + fb_count
    redaction_applied = redaction_info.get("applied", False) or bool(fb_types)

    item = {
        "ReviewID": review_id,
        "PK": f"REVIEW#{review_id}",
        "SK": f"REVIEW#{processed_ts_iso}#{str(uuid.uuid4())}",
        "ProductID": product_id,
        "SourceS3Key": s3_key,
        "RedactedText": redacted_text,
        "OriginalHash": compute_hash(raw_text),
        "ProcessedTimestampEpoch": processed_ts_num,
        "ProcessedTimestampIso": processed_ts_iso,
        "Sentiment": sentiment,
        "SentimentScores": sentiment_scores,
        "KeyPhrases": key_phrases,
        "RedactionApplied": redaction_applied,
        "RedactionCount": total_redaction_count,
        "RedactionTypes": all_redaction_types,
        "GSI1PK": f"DAY#{date_utc}",
        "GSI1SK": f"{product_id}#{processed_ts_num}#{review_id}",
    }

    should_alert, reasons = evaluate_critical(
        redacted_text, sentiment, sentiment_scores, key_phrases, total_redaction_count
    )
    if should_alert:
        item["AlertCandidate"] = True
        item["AlertReasons"] = reasons

    return convert_floats_to_decimal(item)

# --------------------------------------------------------------------
# Failure record
# --------------------------------------------------------------------
def write_failure_record(original_row, s3_key, reason):
    rec = {
        "PK": f"FAIL#{str(uuid.uuid4())}",
        "SK": f"FAIL#{iso_now()}",
        "SourceS3Key": s3_key,
        "RowDataKeys": list(original_row.keys()) if isinstance(original_row, dict) else "N/A",
        "Reason": reason,
        "Timestamp": iso_now()
    }
    try:
        failure_table.put_item(Item=convert_floats_to_decimal(rec))
        LOG.info("Failure record written.")
    except ClientError as e:
        LOG.warning("DynamoDB failure log error (%s). Trying S3 fallback.", e.response.get("Error", {}).get("Code"))
        if FAILURES_S3_BUCKET:
            try:
                key = f"failures/{iso_now()}-{str(uuid.uuid4())}.json"
                s3.put_object(Bucket=FAILURES_S3_BUCKET, Key=key, Body=json.dumps(rec).encode("utf-8"))
                LOG.info("Failure record written to S3.")
            except Exception as se:
                LOG.exception("S3 fallback failure: %s", se)
    except Exception as e:
        LOG.exception("Unexpected failure log error: %s", e)

# --------------------------------------------------------------------
# Lambda handler
# --------------------------------------------------------------------
def lambda_handler(event, context):
    LOG.info("Event received with %d record(s)", len(event.get("Records", [])))
    records = event.get("Records", [])
    if not records:
        LOG.error("No records in event.")
        return {"status": "no_records"}

    successes = 0
    failures = 0

    for rec in records:
        s3_info = rec.get("s3") or {}
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        if not bucket or not key:
            LOG.warning("Skipping record without bucket/key.")
            continue

        s3_key = unquote_plus(key)
        LOG.info("Processing s3://%s/%s", bucket, s3_key)

        try:
            obj = s3.get_object(Bucket=bucket, Key=s3_key)
            body = obj["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            LOG.exception("S3 get error code=%s", code)
            write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"S3GetError:{code}")
            failures += 1
            continue
        except Exception as e:
            LOG.exception("Unexpected S3 get error: %s", e)
            write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"S3GetUnexpected:{e}")
            failures += 1
            continue

        try:
            rows = parse_csv_bytes(body)
            LOG.info("Parsed %d rows from %s", len(rows), s3_key)
        except Exception as e:
            LOG.exception("CSV parse error: %s", e)
            write_failure_record({"s3_bucket": bucket, "s3_key": s3_key}, s3_key, f"CSVParseError:{e}")
            failures += 1
            continue

        items_to_write = []
        for idx, row in enumerate(rows):
            try:
                item = build_item_from_row(row, s3_key)
                items_to_write.append(item)
            except Exception as e:
                LOG.exception("Item build error row=%d: %s", idx, e)
                write_failure_record(row, s3_key, f"BuildItemError:{e}")
                failures += 1

        if items_to_write:
            try:
                with review_table.batch_writer() as batch:
                    for it in items_to_write:
                        batch.put_item(Item=it)
                successes += len(items_to_write)
                LOG.info("Wrote %d items.", len(items_to_write))

                for it in items_to_write:
                    if it.get("AlertCandidate"):
                        try:
                            mark_and_publish_once(it["ReviewID"], it)
                        except Exception as e:
                            LOG.exception("Alert publish failed for %s: %s", it.get("ReviewID"), e)

            except Exception as e:
                LOG.exception("Batch write error: %s", e)
                for it in items_to_write:
                    try:
                        review_table.put_item(Item=it)
                        successes += 1
                        if it.get("AlertCandidate"):
                            try:
                                mark_and_publish_once(it["ReviewID"], it)
                            except Exception as ie:
                                LOG.exception("Alert publish failed for %s: %s", it.get("ReviewID"), ie)
                    except Exception as ie:
                        LOG.exception("Per-item put failed: %s", ie)
                        write_failure_record(it, s3_key, f"DynamoPutError:{ie}")
                        failures += 1

    LOG.info("Processing complete. successes=%d failures=%d", successes, failures)
    return {"status": "finished", "successes": successes, "failures": failures}

# --------------------------------------------------------------------
# Utility functions (manual use)
# --------------------------------------------------------------------
def scan_table_for_bad_timestamp(region_name="us-east-1"):
    client = boto3.client("dynamodb", region_name=region_name)
    paginator = client.get_paginator("scan")
    bad = []
    for page in paginator.paginate(TableName=REVIEW_TABLE, ProjectionExpression="ReviewID,ProcessedTimestampEpoch,ProcessedTimestampIso"):
        for it in page.get("Items", []):
            if "ProcessedTimestampEpoch" not in it:
                bad.append(it)
    return bad

def delete_items_by_key(review_ids, region_name="us-east-1"):
    client = boto3.client("dynamodb", region_name=region_name)
    for rid in review_ids:
        try:
            client.delete_item(TableName=REVIEW_TABLE, Key={"ReviewID": {"S": rid}})
            LOG.info("Deleted ReviewID=%s", rid)
        except Exception as e:
            LOG.exception("Delete failed for ReviewID=%s: %s", rid, e)

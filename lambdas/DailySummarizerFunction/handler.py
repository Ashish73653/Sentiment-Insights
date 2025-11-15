"""
lambda_daily_summarizer.py

Daily product review summarization Lambda (Comprehend-aware deterministic aggregation + optional SageMaker polish).

Current revision (Corrected, Refined & Finalized):
- Comprehend per-review sentiment aggregation for orientation.
- Theme sentiment with shipping vs packaging (logistics) separation + durability/performance stability refinements.
- Phrase extraction with polarity gating, generic artifact filtering, punctuation/digit noise removal.
- Negative/mixed fragments (e.g., "bad parts", "software glitches") blocked from positive highlights.
- Design only treated as positive in negative orientation if strongly positive evidence exists.
- Micro phrase cleanup: exclude weak/generic phrases ("occasional use", "mixed feelings", "my expectations").
- Color phrase rewrite to "color fading" for clearer issue wording.
- Safe Decimal conversion before DynamoDB put_item (no float serialization errors).
- Two-sentence validated output (exactly two sentences; length bounds; no quotes/URLs/instruction echoes).
- Optional model polish pass (POLISH_WITH_MODEL) preserving factual themes.
- Orientation, theme_sentiment, review_counts stored in SourceMeta.
- Environment-tunable thresholds (orientation ratios, theme ratio, phrase lengths).
"""

import os
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from collections import Counter

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

# ----------------- Environment -----------------
REVIEW_TABLE = os.environ.get("REVIEW_TABLE", "ReviewAnalysis")
SUMMARY_TABLE = os.environ.get("SUMMARY_TABLE", "ReviewSummaries")
REGION = os.environ.get("REGION", "us-east-1")
PROCESS_LOOKBACK_HOURS = int(os.environ.get("PROCESS_LOOKBACK_HOURS", "24"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

UNIQUE_PER_DAY = os.environ.get("UNIQUE_PER_DAY", "false").lower() == "true"
SUMMARY_S3_BUCKET = os.environ.get("SUMMARY_S3_BUCKET")
SUMMARY_S3_PREFIX = os.environ.get("SUMMARY_S3_PREFIX", "processed/summaries/")

USE_DETERMINISTIC_FIRST = os.environ.get("USE_DETERMINISTIC_FIRST", "true").lower() == "true"
POLISH_WITH_MODEL = os.environ.get("POLISH_WITH_MODEL", "true").lower() == "true"

SAGEMAKER_ENDPOINT = os.environ.get("SAGEMAKER_ENDPOINT")
SAGEMAKER_CONTENT_TYPE = os.environ.get("SAGEMAKER_CONTENT_TYPE", "application/x-text").lower()
SAGEMAKER_MAX_LENGTH = int(os.environ.get("SAGEMAKER_MAX_LENGTH", "180"))
SAGEMAKER_DO_SAMPLE = os.environ.get("SAGEMAKER_DO_SAMPLE", "FALSE").upper() == "TRUE"
SAGEMAKER_TEMPERATURE = float(os.environ.get("SAGEMAKER_TEMPERATURE", "0"))

MAX_CHUNK_CHARS = int(os.environ.get("MAX_CHUNK_CHARS", "2500"))  # legacy path only
SECOND_PASS_ENABLED = os.environ.get("SECOND_PASS_ENABLED", "true").lower() == "true"

DEBUG_PROMPTS = os.environ.get("DEBUG_PROMPTS", "false").lower() == "true"
DEBUG_S3_BUCKET = os.environ.get("DEBUG_S3_BUCKET")
DEBUG_S3_PREFIX = os.environ.get("DEBUG_S3_PREFIX", "debug_prompts/")

ORIENTATION_POS_RATIO = float(os.environ.get("ORIENTATION_POS_RATIO", "1.5"))
ORIENTATION_NEG_RATIO = float(os.environ.get("ORIENTATION_NEG_RATIO", "1.5"))
THEME_RATIO = float(os.environ.get("THEME_RATIO", "1.4"))
SHIPPING_NEG_EXTRA = float(os.environ.get("SHIPPING_NEG_EXTRA", "0.5"))
SHIPPING_POS_EXTRA = float(os.environ.get("SHIPPING_POS_EXTRA", "0.5"))
MIN_PHRASE_LEN = int(os.environ.get("MIN_PHRASE_LEN", "4"))
PHRASE_MAX = int(os.environ.get("PHRASE_MAX", "4"))
MIX_FORCE_BOTH = os.environ.get("MIX_FORCE_BOTH", "true").lower() == "true"

# ----------------- AWS Clients -----------------
dynamodb = boto3.resource("dynamodb", region_name=REGION)
review_table = dynamodb.Table(REVIEW_TABLE)
summary_table = dynamodb.Table(SUMMARY_TABLE)
sagemaker_rt = boto3.client("sagemaker-runtime", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# ----------------- Logging -----------------
LOG = logging.getLogger()
LOG.setLevel(LOG_LEVEL)

# ----------------- Time helpers -----------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def epoch_seconds_ago(hours):
    return int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())

def today_date_str():
    return datetime.utcnow().strftime("%Y-%m-%d")

def normalize_endpoint_name(endpoint):
    if not endpoint:
        return None
    if endpoint.startswith("arn:"):
        if "endpoint/" in endpoint:
            return endpoint.split("endpoint/")[-1]
        return endpoint.split("/")[-1]
    return endpoint

# ----------------- Data Fetch -----------------
def query_reviews_for_product(product_id, since_epoch):
    projection = "ReviewID,RedactedText,ReviewText,ProductID,ProcessedTimestampEpoch,Sentiment,SentimentScores,KeyPhrases"
    filter_expr = Attr("ProductID").eq(product_id) & Attr("ProcessedTimestampEpoch").gte(since_epoch)
    start_key = None
    reviews = []
    while True:
        kwargs = {
            "FilterExpression": filter_expr,
            "ProjectionExpression": projection
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = review_table.scan(**kwargs)
        for it in resp.get("Items", []):
            text = (it.get("RedactedText") or it.get("ReviewText") or "").strip()
            if not text:
                continue
            scores_raw = it.get("SentimentScores", {})
            scores = {}
            if isinstance(scores_raw, dict):
                for k, v in scores_raw.items():
                    if isinstance(v, (int, float, Decimal)):
                        scores[k] = float(v)
                    elif isinstance(v, dict) and "N" in v:
                        try:
                            scores[k] = float(v["N"])
                        except Exception:
                            pass
            kp_raw = it.get("KeyPhrases") or []
            phrases = []
            for kp in kp_raw:
                if isinstance(kp, str):
                    phrases.append(kp)
                elif isinstance(kp, dict) and "S" in kp:
                    phrases.append(kp["S"])
            reviews.append({
                "ReviewID": it.get("ReviewID"),
                "Text": text,
                "Sentiment": it.get("Sentiment"),
                "SentimentScores": scores,
                "KeyPhrases": phrases
            })
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return reviews

# ----------------- Legacy chunk (if disabled deterministic) -----------------
def chunk_texts(reviews, max_chars):
    chunks, mapping = [], []
    current, ids, cur_len = [], [], 0
    for r in reviews:
        txt = r["Text"]
        add_len = len(txt) + 1
        if current and cur_len + add_len > max_chars:
            chunks.append("\n".join(current)); mapping.append(list(ids))
            current, ids, cur_len = [txt], [r["ReviewID"]], add_len
        else:
            current.append(txt); ids.append(r["ReviewID"]); cur_len += add_len
    if current:
        chunks.append("\n".join(current)); mapping.append(list(ids))
    return chunks, mapping

LEGACY_PROMPT = "Provide a two sentence summary of overall sentiment and main recurring themes across these customer reviews."

def prepare_prompt_from_chunk(chunk_text, instruction=None):
    instruction = instruction or LEGACY_PROMPT
    lines = [l.strip().strip('"') for l in chunk_text.split("\n") if l.strip()]
    return f"{instruction}\n\nReviews:\n" + "\n".join(lines) + "\n\nSummary:"

# ----------------- SageMaker Inference -----------------
def parse_sagemaker_response_bytes(raw_bytes):
    if raw_bytes is None:
        return None
    try:
        txt = raw_bytes.decode("utf-8")
    except Exception:
        try:
            txt = raw_bytes.decode("latin-1")
        except Exception:
            txt = None
    if not txt:
        return None
    try:
        parsed = json.loads(txt)
        if isinstance(parsed, dict):
            for k in ("summary_text","summary","generated_text","text"):
                if k in parsed:
                    return parsed[k]
            if "predictions" in parsed and isinstance(parsed["predictions"], list) and parsed["predictions"]:
                first = parsed["predictions"][0]
                if isinstance(first, dict):
                    for k in ("summary_text","summary","generated_text","text"):
                        if k in first:
                            return first[k]
                elif isinstance(first, str):
                    return first
            return txt
        if isinstance(parsed, list):
            out = []
            for el in parsed:
                if isinstance(el, dict):
                    for k in ("summary_text","summary","generated_text","text"):
                        if k in el:
                            out.append(el[k]); break
                else:
                    out.append(str(el))
            return " ".join(out)
    except Exception:
        pass
    return txt.strip()

def call_sagemaker(body_text):
    endpoint = normalize_endpoint_name(SAGEMAKER_ENDPOINT)
    if not endpoint:
        raise RuntimeError("SAGEMAKER_ENDPOINT not set for polish/model path")
    attempts = [SAGEMAKER_CONTENT_TYPE]
    if SAGEMAKER_CONTENT_TYPE == "application/x-text":
        attempts.append("application/json")
    elif SAGEMAKER_CONTENT_TYPE == "application/json":
        attempts.append("application/x-text")
    last_err = None
    for ct in attempts:
        try:
            if ct == "application/json":
                payload = {
                    "inputs": body_text,
                    "parameters": {
                        "max_length": SAGEMAKER_MAX_LENGTH,
                        "do_sample": SAGEMAKER_DO_SAMPLE,
                        "temperature": SAGEMAKER_TEMPERATURE
                    }
                }
                body = json.dumps(payload).encode("utf-8")
                content_type = "application/json"
            else:
                body = body_text.encode("utf-8")
                content_type = "application/x-text"
            resp = sagemaker_rt.invoke_endpoint(
                EndpointName=endpoint,
                Body=body,
                ContentType=content_type,
                Accept="application/json"
            )
            raw = resp["Body"].read()
            parsed = parse_sagemaker_response_bytes(raw)
            if DEBUG_PROMPTS and DEBUG_S3_BUCKET:
                try:
                    key = f"{DEBUG_S3_PREFIX}{endpoint}/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
                    dbg = {
                        "endpoint": endpoint,
                        "content_type": content_type,
                        "input_len": len(body),
                        "raw_preview": raw[:1024].decode("utf-8", errors="ignore")
                    }
                    s3.put_object(Bucket=DEBUG_S3_BUCKET, Key=key,
                                  Body=json.dumps(dbg).encode("utf-8"),
                                  ContentType="application/json")
                except Exception:
                    LOG.exception("Debug write failed")
            return parsed
        except Exception as e:
            last_err = e
            LOG.exception("Endpoint invoke failed content_type=%s", ct)
    raise last_err or RuntimeError("All content types failed")

# ----------------- Text cleanup / validation -----------------
def cleanup_text(text):
    if not text:
        return text
    t = re.sub(r'\s+', ' ', text).strip()
    t = re.sub(r'\s+([.,!?;:])', r'\1', t)
    t = re.sub(r'([.,!?;:])([A-Za-z])', r'\1 \2', t)
    t = re.sub(r'\b(\w+)\b\s+\b\1\b', r'\1', t)
    parts = re.split(r'(?<=[.!?])\s+', t)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) > 2:
        t = " ".join(parts[:2])
    elif len(parts) == 1 and len(parts[0]) > 170:
        seg = re.split(r',\s+', parts[0], maxsplit=1)
        if len(seg) == 2:
            t = f"{seg[0]}. {seg[1]}"
    return t.strip()

STOPWORDS = {
    "the","and","is","are","to","of","for","a","an","in","on","it","was","with","very",
    "this","that","but","after","at","be","as","so","has","its","my","i","they","their",
    "product","customer","customers","review","reviews"
}

PHRASE_IGNORE_SINGLE = {
    "works","lovely","great","good","fine","nice","value","quality","design"
}

THEME_TOKENS = {
    "shipping": {"shipping","delivery","delay","late","forever","cost"},
    "logistics": {"packaging","damaged","damage"},
    "quality": {"quality","premium","cheap","durability","cracked","faded","materials","battery","screen","scratches","damaged"},
    "performance": {"performance","works","working","freezes","inconsistent","charging","instructions","assembly","speed"},
    "design": {"design","look","modern","stylish","appearance","color"},
    "support": {"support","refund","returns","customer","response","respond"},
    "value": {"price","value","money","cost","worth","expensive","cheaper","alternatives"}
}
GENERIC_IGNORES = {
    "tiny","box","unit","material","experience","parts","product","issue","issues","bad","good","build","my","expectations"
}
POSITIVE_PHRASE_TERMS = {
    "great","excellent","amazing","love","loved","happy","pleased","premium",
    "perfect","fast","intuitive","clear","reliable","superb","fantastic","impressed"
}
NEGATIVE_PHRASE_TERMS = {
    "bad","poor","slow","broken","faulty","issue","issues","delay","late","unreliable",
    "cheap","cracked","faded","damaged","smell","smells","freezes","inconsistent",
    "disappointed","terrible","costly","expensive","unpleasant","scratches","returned"
}
NEGATIVE_FRAGMENT_BLOCKLIST = {
    "software glitches","bad parts","faulty unit","unpleasant experience",
    "color faded","faded color","scratches","cracked screen","poor durability"
}
SHIPPING_POS = {"fast","early","quick","on-time"}
SHIPPING_NEG = {"delay","late","forever","cost","expensive","high","took"}

PHRASE_IGNORE_SINGLE = PHRASE_IGNORE_SINGLE.union(GENERIC_IGNORES)

PHRASE_POSITIVE_EXCLUDE = {"occasional use","mixed feelings","my expectations"}
PHRASE_REWRITE = {
    "color": "color fading",
    "mixed feelings": "design"
}

def refine_display_list(lst, positive=False):
    out = []
    for item in lst:
        li = item.lower()
        if positive and li in PHRASE_POSITIVE_EXCLUDE:
            continue
        if li in PHRASE_REWRITE:
            out.append(PHRASE_REWRITE[li])
        else:
            out.append(item)
    seen = set()
    deduped = []
    for v in out:
        low = v.lower()
        if low in seen:
            continue
        seen.add(low)
        deduped.append(v)
    return deduped

def tokenize_phrase(phrase: str):
    return re.findall(r"[a-z']+", phrase.lower())

def analyze_phrase_sentiment(tokens):
    pos_hits = sum(1 for t in tokens if t in POSITIVE_PHRASE_TERMS)
    neg_hits = sum(1 for t in tokens if t in NEGATIVE_PHRASE_TERMS)
    return pos_hits, neg_hits

def filter_phrases(raw_phrases, positive=True):
    cleaned = []
    for ph in raw_phrases:
        low = ph.lower()
        if low in NEGATIVE_FRAGMENT_BLOCKLIST and positive:
            continue
        toks = tokenize_phrase(ph)
        if not toks:
            continue
        if all(t in STOPWORDS or t in GENERIC_IGNORES for t in toks):
            continue
        pos_hits, neg_hits = analyze_phrase_sentiment(toks)
        if positive:
            if neg_hits > 0 and pos_hits == 0:
                continue
        else:
            if pos_hits > 0 and neg_hits == 0:
                continue
        cleaned.append(ph)
    return cleaned

def classify_design_positive(reviews):
    for rev in reviews:
        txt = rev["Text"].lower()
        if any(k in txt for k in ["design","look","stylish","appearance","color"]):
            sc = rev.get("SentimentScores", {})
            p = float(sc.get("Positive", 0) or 0)
            n = float(sc.get("Negative", 0) or 0)
            if p > n * 2.0:
                return True
    return False

def derive_orientation(enriched):
    total_pos = total_neg = 0.0
    pos_reviews = neg_reviews = mixed_reviews = neutral_reviews = 0
    for rev in enriched:
        sc = rev.get("SentimentScores", {})
        p = float(sc.get("Positive", 0) or 0)
        n = float(sc.get("Negative", 0) or 0)
        total_pos += p
        total_neg += n
        label = (rev.get("Sentiment") or "").upper()
        if label == "POSITIVE":
            pos_reviews += 1
        elif label == "NEGATIVE":
            neg_reviews += 1
        elif label == "MIXED":
            mixed_reviews += 1
        elif label == "NEUTRAL":
            neutral_reviews += 1
    if total_pos >= total_neg * ORIENTATION_POS_RATIO and pos_reviews >= neg_reviews:
        orientation = "predominantly positive"
    elif total_neg >= total_pos * ORIENTATION_NEG_RATIO and neg_reviews >= pos_reviews:
        orientation = "predominantly negative"
    else:
        orientation = "mixed"
    return orientation, {
        "positive": pos_reviews,
        "negative": neg_reviews,
        "mixed": mixed_reviews,
        "neutral": neutral_reviews
    }

def derive_theme_sentiment(enriched):
    scores = {t: {"pos":0.0,"neg":0.0,"count":0,"pos_override":0,"neg_override":0,"tokens":set()} for t in THEME_TOKENS}
    for rev in enriched:
        tokens = set(re.findall(r"[a-z']+", rev["Text"].lower()))
        present = [t for t, vocab in THEME_TOKENS.items() if vocab & tokens]
        sc = rev.get("SentimentScores", {})
        p = float(sc.get("Positive", 0) or 0)
        n = float(sc.get("Negative", 0) or 0)
        for th in present:
            scores[th]["pos"] += p
            scores[th]["neg"] += n
            scores[th]["count"] += 1
            scores[th]["tokens"].update(tokens)
            if th == "shipping":
                if SHIPPING_NEG & tokens:
                    scores[th]["neg_override"] += 1
                if SHIPPING_POS & tokens:
                    scores[th]["pos_override"] += 1
    classified = {}
    for th, vals in scores.items():
        if vals["count"] == 0:
            continue
        pos, neg = vals["pos"], vals["neg"]
        if vals["pos_override"] > 0 and vals["neg_override"] > 0:
            sentiment = "mixed"
        else:
            if pos > neg * THEME_RATIO:
                sentiment = "positive"
            elif neg > pos * THEME_RATIO:
                sentiment = "negative"
            else:
                sentiment = "mixed"
            if th == "shipping" and vals["neg_override"] > 0 and vals["pos_override"] == 0:
                sentiment = "negative"
        label = th
        if th == "quality":
            if any(tok in vals["tokens"] for tok in ["cracked","scratches","faded","damaged","durability"]):
                label = "durability"
        if th == "performance" and any(tok in vals["tokens"] for tok in ["freezes","inconsistent","charging"]):
            label = "performance stability"
        if th == "logistics":
            label = "packaging"
        classified[label] = {
            "sentiment": sentiment,
            "pos": pos,
            "neg": neg,
            "count": vals["count"]
        }
    return classified

def extract_keyphrases(enriched):
    phrase_pos = Counter()
    phrase_neg = Counter()
    for rev in enriched:
        sc = rev.get("SentimentScores", {})
        p = float(sc.get("Positive", 0) or 0)
        n = float(sc.get("Negative", 0) or 0)
        for raw in rev.get("KeyPhrases", []):
            base = raw.lower().strip()
            if not base or len(base) < MIN_PHRASE_LEN:
                continue
            toks = tokenize_phrase(base)
            if not toks:
                continue
            if all(t in STOPWORDS or t in GENERIC_IGNORES for t in toks):
                continue
            if ' ' not in base and base in PHRASE_IGNORE_SINGLE:
                continue
            letters = sum(c.isalpha() for c in base)
            if letters / max(len(base),1) < 0.6:
                continue
            if p >= n * 1.5:
                phrase_pos[base] += p
            elif n >= p * 1.3:
                phrase_neg[base] += n
    top_pos = [w for w,_ in phrase_pos.most_common(PHRASE_MAX)]
    top_neg = [w for w,_ in phrase_neg.most_common(PHRASE_MAX)]
    top_pos = filter_phrases(top_pos, positive=True)
    top_neg = filter_phrases(top_neg, positive=False)
    return top_pos, top_neg

def join_list(items):
    if not items: return ""
    if len(items) == 1: return items[0]
    if len(items) == 2: return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"

def build_summary(orientation, theme_sentiment, top_pos_phrases, top_neg_phrases, reviews):
    positives = [t for t, info in theme_sentiment.items() if info["sentiment"] == "positive"]
    negatives = [t for t, info in theme_sentiment.items() if info["sentiment"] == "negative"]
    mixed     = [t for t, info in theme_sentiment.items() if info["sentiment"] == "mixed"]

    if orientation == "predominantly negative" and "design" in positives and not classify_design_positive(reviews):
        positives = [t for t in positives if t != "design"]

    def is_neg_fragment(label: str) -> bool:
        return any(term in label.lower() for term in NEGATIVE_FRAGMENT_BLOCKLIST)

    pos_show = [p for p in positives if not is_neg_fragment(p)][:3]
    if not pos_show and orientation != "predominantly negative":
        pos_show = [m for m in mixed if not is_neg_fragment(m)][:2]

    neg_show = negatives[:3]
    if not neg_show and orientation != "predominantly positive":
        neg_show = mixed[:2]

    pos_show = refine_display_list(pos_show, positive=True)
    neg_show = refine_display_list(neg_show, positive=False)

    def dedup(seq):
        seen = set()
        out = []
        for x in seq:
            xl = x.lower()
            if xl in seen:
                continue
            seen.add(xl)
            out.append(x)
        return out

    if len(pos_show) < 2 and orientation != "predominantly negative" and top_pos_phrases:
        extras = [ph for ph in top_pos_phrases if ph not in pos_show]
        extras = refine_display_list(extras, positive=True)
        pos_show += extras[: (2 - len(pos_show))]

    if len(neg_show) < 2 and top_neg_phrases:
        extras_n = [ph for ph in top_neg_phrases if ph not in neg_show]
        extras_n = refine_display_list(extras_n, positive=False)
        neg_show += extras_n[: (2 - len(neg_show))]

    if orientation == "mixed" and MIX_FORCE_BOTH:
        if not pos_show and top_pos_phrases:
            add = refine_display_list(top_pos_phrases, positive=True)
            if add:
                pos_show = [add[0]]
        if not neg_show and top_neg_phrases:
            addn = refine_display_list(top_neg_phrases, positive=False)
            if addn:
                neg_show = [addn[0]]

    pos_show = dedup(pos_show)
    neg_show = dedup(neg_show)

    def jl(lst):
        return join_list(lst)

    if orientation == "predominantly positive":
        s1 = "Overall sentiment is predominantly positive"
        if pos_show:
            s1 += f", with customers praising {jl(pos_show)}"
        s1 += "."
    elif orientation == "predominantly negative":
        s1 = "Overall sentiment leans negative"
        if neg_show:
            s1 += f", driven by issues in {jl(neg_show)}"
        s1 += "."
    else:
        if pos_show and neg_show:
            s1 = f"Overall sentiment is mixed, balancing praise for {jl(pos_show)} with concerns about {jl(neg_show)}."
        elif pos_show:
            s1 = f"Overall sentiment is mixed, with appreciation for {jl(pos_show)}."
        elif neg_show:
            s1 = f"Overall sentiment is mixed, with recurring issues in {jl(neg_show)}."
        else:
            s1 = "Overall sentiment is mixed."

    if orientation == "predominantly positive":
        if neg_show:
            s2 = f"Minor concerns remain around {jl(neg_show)}."
        else:
            s2 = "Few notable concerns are raised."
    elif orientation == "predominantly negative":
        if pos_show:
            s2 = f"Some positives persist in {jl(pos_show)} despite the broader issues."
        else:
            s2 = "Positive feedback is scarce."
    else:
        if top_pos_phrases and top_neg_phrases:
            s2 = f"Key positives include {jl(top_pos_phrases[:PHRASE_MAX])}, while problems center on {jl(top_neg_phrases[:PHRASE_MAX])}."
        elif top_neg_phrases:
            s2 = f"Problems center on {jl(top_neg_phrases[:PHRASE_MAX])}."
        elif top_pos_phrases:
            s2 = f"Highlights include {jl(top_pos_phrases[:PHRASE_MAX])}."
        else:
            s2 = "No single theme clearly dominates."
    return cleanup_text(f"{s1} {s2}")

def make_deterministic_summary(reviews):
    orientation, review_counts = derive_orientation(reviews)
    theme_sentiment = derive_theme_sentiment(reviews)
    top_pos_ph, top_neg_ph = extract_keyphrases(reviews)
    summary = build_summary(orientation, theme_sentiment, top_pos_ph, top_neg_ph, reviews)
    return summary, orientation, review_counts, theme_sentiment

# ----------------- Validation -----------------
def validate_final(summary):
    if not summary:
        return False
    s = summary.strip()
    if any(x in s.lower() for x in ["summarize","reviewer","www.","http://","https://"]):
        return False
    parts = re.split(r'(?<=[.!?])\s+', s)
    parts = [p for p in parts if p]
    if len(parts) != 2:
        return False
    for p in parts:
        if len(p) < 25 or len(p) > 280:
            return False
    if '"' in s or '“' in s or '”' in s:
        return False
    return True

def finalize_summary(summary):
    return cleanup_text(summary)

# ----------------- Legacy model path -----------------
def legacy_model_summary(reviews):
    chunks, mapping = chunk_texts(reviews, MAX_CHUNK_CHARS)
    outputs = []
    for idx, ch in enumerate(chunks):
        try:
            raw = call_sagemaker(prepare_prompt_from_chunk(ch))
            outputs.append(cleanup_text(raw or ""))
        except Exception as e:
            LOG.exception("Legacy chunk summarization failed chunk=%d: %s", idx, e)
    if not outputs:
        return ""
    if len(outputs) == 1:
        return outputs[0]
    agg = " ".join(outputs)
    if SECOND_PASS_ENABLED and SAGEMAKER_ENDPOINT:
        try:
            second = call_sagemaker(prepare_prompt_from_chunk(agg))
            return cleanup_text(second or agg)
        except Exception:
            LOG.exception("Second pass failed; using aggregated legacy summary.")
    return cleanup_text(agg)

# ----------------- Polish (optional model rewrite) -----------------
def polish_summary(base_summary):
    if not SAGEMAKER_ENDPOINT or not POLISH_WITH_MODEL:
        return base_summary
    prompt = (
        "Rewrite the following two sentences in neutral, fluent business English without quotes. "
        "Keep exactly two sentences and retain all factual themes.\n\nInput:\n"
        f"{base_summary}\n\nOutput:"
    )
    try:
        raw = call_sagemaker(prompt)
        polished = cleanup_text(raw or base_summary)
        parts = re.split(r'(?<=[.!?])\s+', polished)
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) > 2:
            polished = " ".join(parts[:2])
        return polished
    except Exception as e:
        LOG.exception("Polish failed: %s", e)
        return base_summary

# ----------------- DynamoDB numeric conversion helpers -----------------
def is_number(val):
    return isinstance(val, (int, float, Decimal))

def convert_numeric_types(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, int):
        return obj
    if isinstance(obj, Decimal):
        return obj
    if isinstance(obj, dict):
        return {k: convert_numeric_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_numeric_types(v) for v in obj]
    return obj

# ----------------- Write Summary -----------------
def write_summary_to_table(product_id, summary_text, review_count, source_meta=None):
    now = now_iso()
    date_str = today_date_str()
    safe_meta = convert_numeric_types(source_meta or {})
    item = {
        "ProductID": product_id,
        "SummaryTimestamp": now,
        "GeneratedAt": now,
        "SummaryDate": date_str,
        "SummaryText": summary_text,
        "ReviewCount": review_count,
        "SourceMeta": safe_meta
    }
    if UNIQUE_PER_DAY:
        try:
            summary_table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(ProductID) OR SummaryDate <> :d",
                ExpressionAttributeValues={":d": date_str}
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                LOG.info("Overwriting existing daily summary for %s.", product_id)
                summary_table.put_item(Item=item)
            else:
                raise
    else:
        summary_table.put_item(Item=item)

    if SUMMARY_S3_BUCKET:
        key = f"{SUMMARY_S3_PREFIX}date={date_str}/product={product_id}.json"
        obj = {
            "product_id": product_id,
            "date": date_str,
            "summary": summary_text,
            "metadata": safe_meta,
            "generated_at": now
        }
        try:
            s3.put_object(
                Bucket=SUMMARY_S3_BUCKET,
                Key=key,
                Body=json.dumps(obj, default=lambda x: float(x) if isinstance(x, Decimal) else x).encode("utf-8"),
                ContentType="application/json"
            )
        except Exception:
            LOG.exception("Failed S3 write summary for %s", product_id)

# ----------------- Handler -----------------
def handler(event, context):
    LOG.info("Event: %s", json.dumps(event))
    product_list = event.get("product_list")
    since_hours = int(event.get("since_hours", PROCESS_LOOKBACK_HOURS))
    since_epoch = epoch_seconds_ago(since_hours)
    results = []

    if not product_list:
        seen = set()
        scan_kwargs = {
            "FilterExpression": Attr("ProcessedTimestampEpoch").gte(since_epoch),
            "ProjectionExpression": "ProductID"
        }
        start_key = None
        while True:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            resp = review_table.scan(**scan_kwargs)
            for it in resp.get("Items", []):
                pid = it.get("ProductID")
                if pid:
                    seen.add(pid)
            start_key = resp.get("LastEvaluatedKey")
            if not start_key:
                break
        product_list = list(seen)
        LOG.info("Discovered products: %s", product_list)

    for product in product_list:
        try:
            reviews = query_reviews_for_product(product, since_epoch)
            if not reviews:
                results.append({"product": product, "status": "no_reviews"})
                continue

            if USE_DETERMINISTIC_FIRST:
                deterministic_summary, orientation, review_counts, theme_sentiment = make_deterministic_summary(reviews)
                candidate = deterministic_summary
                polished_flag = False
                if POLISH_WITH_MODEL and SAGEMAKER_ENDPOINT:
                    polished = polish_summary(deterministic_summary)
                    if validate_final(polished):
                        candidate = polished
                        polished_flag = True
                if not validate_final(candidate):
                    LOG.info("Invalid polished candidate for %s; reverting.", product)
                    candidate = deterministic_summary
                final_summary = finalize_summary(candidate)
                if not validate_final(final_summary):
                    final_summary = finalize_summary(deterministic_summary)
                source_meta = {
                    "method": "deterministic_first",
                    "polished": polished_flag,
                    "orientation": orientation,
                    "review_counts": review_counts,
                    "theme_sentiment": theme_sentiment
                }
            else:
                legacy = legacy_model_summary(reviews)
                orientation = "legacy_unknown"
                review_counts = {}
                theme_sentiment = {}
                if not legacy or not validate_final(legacy):
                    deterministic_summary, orientation, review_counts, theme_sentiment = make_deterministic_summary(reviews)
                    legacy = deterministic_summary
                final_summary = finalize_summary(legacy)
                if not validate_final(final_summary):
                    final_summary = finalize_summary(deterministic_summary)
                source_meta = {
                    "method": "legacy_model_first",
                    "orientation": orientation,
                    "review_counts": review_counts,
                    "theme_sentiment": theme_sentiment
                }

            write_summary_to_table(product, final_summary, len(reviews), source_meta=source_meta)
            results.append({
                "product": product,
                "status": "summarized",
                "summary_len": len(final_summary),
                "reviews": len(reviews),
                "orientation": source_meta.get("orientation")
            })
        except Exception as e:
            LOG.exception("Processing error for %s: %s", product, e)
            results.append({"product": product, "status": "error", "error": str(e)})

    LOG.info("Completed summarization run. results=%s", json.dumps(results))
    return {"status": "finished", "results": results}
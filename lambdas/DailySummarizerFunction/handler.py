"""
Daily product review summarization Lambda (anti-echo + less repetitive)

Goals
- Eliminate prompt/context leakage (“Focus on the overall picture…”, “Orientation is…”, “Reviewer:”)
- Always produce EXACTLY two paraphrased sentences, neutral business English, no quotes/pipes/PII labels
- Prevent verbatim copying of reviews (Jaccard + n-gram checks)
- Fallbacks:
  1) Structured prompt → model
  2) Retry with shuffled snippets
  3) Deterministic paraphrase (two sentences) with stylistic variation to avoid repetition

Env
REQUIRE_MODEL=true
SAGEMAKER_ENDPOINT=<your endpoint or ARN>
DEBUG_PROMPTS=true
IGNORE_PII_LABELS includes CreditCard,CVV,Phone,PhoneNumber,SSN
MIN_SENT_LEN=12
SECOND_PASS_ENABLED=false
"""

import os
import json
import logging
import re
import random
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from collections import Counter
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

# ----------------- Environment -----------------
REVIEW_TABLE              = os.environ.get("REVIEW_TABLE", "ReviewAnalysis")
SUMMARY_TABLE             = os.environ.get("SUMMARY_TABLE", "ReviewSummaries")
REGION                    = os.environ.get("REGION", "us-east-1")
PROCESS_LOOKBACK_HOURS    = int(os.environ.get("PROCESS_LOOKBACK_HOURS", "24"))
LOG_LEVEL                 = os.environ.get("LOG_LEVEL", "INFO")

UNIQUE_PER_DAY            = os.environ.get("UNIQUE_PER_DAY", "false").lower() == "true"
SUMMARY_S3_BUCKET         = os.environ.get("SUMMARY_S3_BUCKET")
SUMMARY_S3_PREFIX         = os.environ.get("SUMMARY_S3_PREFIX", "processed/summaries/")

REQUIRE_MODEL             = os.environ.get("REQUIRE_MODEL", "true").lower() == "true"
SAGEMAKER_ENDPOINT        = os.environ.get("SAGEMAKER_ENDPOINT")
SAGEMAKER_TEMPERATURE     = float(os.environ.get("SAGEMAKER_TEMPERATURE", "0.2"))
SAGEMAKER_MAX_LENGTH      = int(os.environ.get("SAGEMAKER_MAX_LENGTH", "180"))

MAX_CHUNK_CHARS           = int(os.environ.get("MAX_CHUNK_CHARS", "2500"))
SECOND_PASS_ENABLED       = os.environ.get("SECOND_PASS_ENABLED", "false").lower() == "true"

DEBUG_PROMPTS             = os.environ.get("DEBUG_PROMPTS", "true").lower() == "true"
DEBUG_S3_BUCKET           = os.environ.get("DEBUG_S3_BUCKET")
DEBUG_S3_PREFIX           = os.environ.get("DEBUG_S3_PREFIX", "debug_prompts/")

ORIENTATION_POS_RATIO     = float(os.environ.get("ORIENTATION_POS_RATIO", "1.5"))
ORIENTATION_NEG_RATIO     = float(os.environ.get("ORIENTATION_NEG_RATIO", "1.5"))
THEME_RATIO               = float(os.environ.get("THEME_RATIO", "1.4"))
PHRASE_MAX                = int(os.environ.get("PHRASE_MAX", "4"))
MIN_PHRASE_LEN            = int(os.environ.get("MIN_PHRASE_LEN", "4"))
MIX_FORCE_BOTH            = os.environ.get("MIX_FORCE_BOTH", "true").lower() == "true"

MIN_SENT_LEN              = int(os.environ.get("MIN_SENT_LEN", "12"))
MAX_SENT_LEN              = int(os.environ.get("MAX_SENT_LEN", "300"))

REDACTED_TOKEN            = os.environ.get("REDACTED_TOKEN", "[REDACTED]")
IGNORE_PII_LABELS         = [l.strip() for l in os.environ.get(
    "IGNORE_PII_LABELS",
    "Reviewer,Email,Contact,Address,CREDIT_DEBIT_NUMBER,CREDIT_DEBIT_CVV,BANK_ACCOUNT_NUMBER,CreditCard,CVV,Phone,PhoneNumber,SSN"
).split(",") if l.strip()]

# ----------------- AWS Clients -----------------
dynamodb      = boto3.resource("dynamodb", region_name=REGION)
review_table  = dynamodb.Table(REVIEW_TABLE)
summary_table = dynamodb.Table(SUMMARY_TABLE)
sagemaker_rt  = boto3.client("sagemaker-runtime", region_name=REGION)
sm_client     = boto3.client("sagemaker", region_name=REGION)
s3            = boto3.client("s3", region_name=REGION)

# ----------------- Logging -----------------
LOG = logging.getLogger()
LOG.setLevel(LOG_LEVEL)

# ----------------- Time helpers -----------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def epoch_seconds_ago(hours: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())

def today_date_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def normalize_endpoint_name(endpoint):
    if not endpoint:
        return None
    if isinstance(endpoint, str) and endpoint.startswith("arn:"):
        if "endpoint/" in endpoint:
            return endpoint.split("endpoint/")[-1]
        return endpoint.split("/")[-1]
    return endpoint

def model_ready():
    if not SAGEMAKER_ENDPOINT:
        return False, "no_endpoint_env"
    name = normalize_endpoint_name(SAGEMAKER_ENDPOINT)
    try:
        resp = sm_client.describe_endpoint(EndpointName=name)
        status = resp.get("EndpointStatus")
        return status == "InService", status
    except ClientError as e:
        return False, f"describe_failed:{e.response.get('Error', {}).get('Code', 'Error')}"

# ----------------- PII / instruction removal -----------------
LABEL_ALT = "|".join([re.escape(lbl) for lbl in IGNORE_PII_LABELS])
PIPE_SEGMENT = re.compile(r'\s*\|\s*(?:' + LABEL_ALT + r')\s*(?::\s*[^|]*)?(?=\s*\||$)', flags=re.IGNORECASE)
PIPE_REDACTED_ONLY = re.compile(r'\s*\|\s*' + re.escape(REDACTED_TOKEN) + r'(?:\s*(?=\|)|\s*$)', flags=re.IGNORECASE)
ANY_LABEL_REDACTED = re.compile(
    r'(?:(?<=^)|(?<=\s)|(?<=\|))([A-Za-z_][A-Za-z0-9_\s\-]{0,24})\s*:\s*' + re.escape(REDACTED_TOKEN) + r'(?=(\s*\|)|\s|$)',
    flags=re.IGNORECASE
)

def strip_redacted_segments(text: str) -> str:
    if not text:
        return text
    t = text.replace("–", "-").replace("—", "-")
    while True:
        t2 = PIPE_SEGMENT.sub('', t)
        t2 = PIPE_REDACTED_ONLY.sub('', t2)
        t2 = ANY_LABEL_REDACTED.sub('', t2)
        if t2 == t:
            break
        t = t2
    t = re.sub(r'\b' + re.escape(REDACTED_TOKEN) + r'\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*\|\s*', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.strip('|').strip()

# Echo/context patterns to strip
INSTRUCTION_ECHO_KEYWORDS = [
    # instruction-y
    "output exactly two sentences", "produce exactly two sentences", "write two concise sentences",
    "write two brief phrases", "return two phrases", "two-sentence summary",
    "you are generating an executive summary", "neutral english", "neutral business english",
    "paraphrase the ideas", "paraphrase", "exclude pii", "exclude urls",
    "exclude headings", "exclude lists",
    # context-y (plain text)
    "there are", "orientation is", "positives include", "negatives include", "mixed themes include",
    "notable positive phrases", "notable negative phrases",
    "examples from reviews", "examples for context", "use this context",
    "consider strengths", "consider concerns", "consider ",
    "focus on the overall picture", "reviewer:"
]
INSTRUCTION_ECHO_RE = re.compile("|".join([re.escape(k) for k in INSTRUCTION_ECHO_KEYWORDS]), flags=re.I)

# Label-with-colon/equal detector (drop entire sentence if present)
INSTRUCTION_LABEL_RE = re.compile(
    r"\b(reviews?|orientation|positives?|negatives?|mixed|phrases?|"
    r"pos(?:itive)?\s*phrases?|neg(?:ative)?\s*phrases?|samples?|sample|summary|context)\s*[:=]",
    flags=re.I
)

META_INSTRUCTION_PATTERNS = [
    r"rewrite the following[^.]*\.", r"keep (it )?to exactly two sentences[^.]*\.",
    r"do not mention individual reviewers[^.]*\.", r"neutral,? fluent business english[^.]*\.",
    r"no (quotes|headings|bullet points|urls|line breaks)[^.]*\.", r"rules?:[^.]*\."
]
META_INSTRUCTION_RE = re.compile(
    r"(?i)\b(" + "|".join(p.strip("()") for p in [
        "rewrite the following","keep it to exactly two sentences","keep to exactly two sentences",
        "do not mention individual reviewers","neutral, fluent business english",
        "no quotes","no headings","no bullet points","no urls","no line breaks","rules:"
    ]) + r")"
)

def strip_meta_instructions(text: str) -> str:
    if not text:
        return text
    m = META_INSTRUCTION_RE.search(text)
    t = text
    if m:
        t = t[:m.start()].rstrip()
    for pat in META_INSTRUCTION_PATTERNS:
        t = re.sub(pat, "", t, flags=re.I)
    return re.sub(r'\s+', ' ', t).strip()

def strip_instruction_sentences(text: str) -> str:
    if not text:
        return text
    parts = re.split(r'(?<=[.!?])\s+', text)
    kept = [p for p in parts if p and not INSTRUCTION_ECHO_RE.search(p) and not INSTRUCTION_LABEL_RE.search(p)]
    out = " ".join(kept).strip()
    return re.sub(r'\s+', ' ', out).strip()

def strip_meta_instructions_and_pii(txt: str) -> str:
    t = strip_meta_instructions(txt)
    # Remove Reviewer:/Reviewers:
    t = re.sub(r'(?i)\breviewers?\s*:\s*', '', t)
    # Remove any label:[REDACTED] leftovers
    t = ANY_LABEL_REDACTED.sub('', t)
    t = re.sub(r'\b' + re.escape(REDACTED_TOKEN) + r'\b', '', t)
    # Drop sentences that look like prompt/context
    t = strip_instruction_sentences(t)
    return re.sub(r'\s+', ' ', t).strip()

# ----------------- Data Fetch -----------------
def query_reviews_for_product(product_id: str, since_epoch: int):
    projection = "ReviewID,RedactedText,ReviewText,ProductID,ProcessedTimestampEpoch,Sentiment,SentimentScores,KeyPhrases"
    filter_expr = Attr("ProductID").eq(product_id) & Attr("ProcessedTimestampEpoch").gte(since_epoch)
    start_key = None
    reviews = []
    while True:
        kwargs = {"FilterExpression": filter_expr, "ProjectionExpression": projection}
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = review_table.scan(**kwargs)
        for it in resp.get("Items", []):
            raw = (it.get("RedactedText") or it.get("ReviewText") or "").strip()
            if not raw:
                continue
            sanitized = strip_redacted_segments(raw)
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
                val = kp if isinstance(kp, str) else (kp.get("S") if isinstance(kp, dict) else "")
                if not val:
                    continue
                lo = val.lower()
                if REDACTED_TOKEN.lower() in lo:
                    continue
                norm = lo.lstrip(" |:;-").strip()
                if any(norm.startswith(lbl.lower()) or lbl.lower() in norm for lbl in IGNORE_PII_LABELS):
                    continue
                if lo.startswith("|"):
                    continue
                phrases.append(val)
            reviews.append({
                "ReviewID": it.get("ReviewID"),
                "Text": sanitized,
                "Sentiment": it.get("Sentiment"),
                "SentimentScores": scores,
                "KeyPhrases": phrases
            })
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return reviews

# ----------------- Orientation / Themes -----------------
STOPWORDS = set("""
the and is are to of for a an in on it was with very this that but after at be as so has its my i they their product
customer customers review reviews""".split())

GENERIC_IGNORES = {"tiny","box","unit","material","experience","parts","product","issue","issues","bad","good","build","my","expectations"}
POS_TERMS = {"great","excellent","amazing","love","loved","happy","pleased","premium","perfect","fast","intuitive","clear","reliable","superb","fantastic","impressed","solid","durable","smooth","efficient"}
NEG_TERMS = {"bad","poor","slow","broken","faulty","issue","issues","delay","late","unreliable","cheap","cracked","faded","damaged","smell","freezes","inconsistent","disappointed","terrible","costly","expensive","unpleasant","scratches","returned","unsafe","malfunctioned"}
THEME_TOKENS = {
    "quality": {"quality","premium","cheap","durability","durable","cracked","faded","materials","battery","screen","scratches","damaged"},
    "performance": {"performance","works","working","freezes","inconsistent","charging","speed","smooth","efficient","reliable"},
    "design": {"design","look","modern","stylish","appearance","color"},
    "support": {"support","refund","returns","customer","response","respond","requesting"},
    "value": {"price","value","money","cost","worth","expensive","cheaper","alternatives"}
}

def derive_orientation(reviews):
    total_pos = total_neg = 0.0
    pos_r = neg_r = mixed_r = neutral_r = 0
    for r in reviews:
        sc = r.get("SentimentScores", {})
        p = float(sc.get("Positive", 0) or 0)
        n = float(sc.get("Negative", 0) or 0)
        total_pos += p
        total_neg += n
        lab = (r.get("Sentiment") or "").upper()
        if lab == "POSITIVE": pos_r += 1
        elif lab == "NEGATIVE": neg_r += 1
        elif lab == "MIXED": mixed_r += 1
        elif lab == "NEUTRAL": neutral_r += 1
    if total_pos >= total_neg * ORIENTATION_POS_RATIO and pos_r >= neg_r:
        o = "predominantly positive"
    elif total_neg >= total_pos * ORIENTATION_NEG_RATIO and neg_r >= pos_r:
        o = "predominantly negative"
    else:
        o = "mixed"
    return o, {"positive": pos_r, "negative": neg_r, "mixed": mixed_r, "neutral": neutral_r}

def derive_theme_sentiment(reviews):
    scores = {t: {"pos": 0.0, "neg": 0.0, "count": 0} for t in THEME_TOKENS}
    for r in reviews:
        tokens = set(re.findall(r"[a-z']+", r["Text"].lower()))
        present = [t for t, vocab in THEME_TOKENS.items() if tokens & vocab]
        sc = r.get("SentimentScores", {})
        p = float(sc.get("Positive", 0) or 0)
        n = float(sc.get("Negative", 0) or 0)
        for th in present:
            s = scores[th]
            s["pos"] += p
            s["neg"] += n
            s["count"] += 1
    out = {}
    for th, v in scores.items():
        if v["count"] == 0:
            continue
        if v["pos"] > v["neg"] * THEME_RATIO:
            sentiment = "positive"
        elif v["neg"] > v["pos"] * THEME_RATIO:
            sentiment = "negative"
        else:
            sentiment = "mixed"
        out[th] = {"sentiment": sentiment, "count": v["count"], "pos": v["pos"], "neg": v["neg"]}
    return out

def extract_keyphrases(reviews):
    pos_c = Counter(); neg_c = Counter()
    for r in reviews:
        text = r["Text"].lower()
        sc = r.get("SentimentScores", {})
        p = float(sc.get("Positive", 0) or 0); n = float(sc.get("Negative", 0) or 0)
        words = re.findall(r"[a-z]+", text)
        for i, w in enumerate(words):
            if w in POS_TERMS and i+1 < len(words):
                phrase = " ".join(words[i:i+2])
                pos_c[phrase] += p + 0.5
            if w in NEG_TERMS and i+1 < len(words):
                phrase = " ".join(words[i:i+2])
                neg_c[phrase] += n + 0.5
    top_pos = [w for w,_ in pos_c.most_common(PHRASE_MAX)]
    top_neg = [w for w,_ in neg_c.most_common(PHRASE_MAX)]
    return top_pos, top_neg

def build_structured_summary(reviews):
    orientation, counts = derive_orientation(reviews)
    theme_sentiment = derive_theme_sentiment(reviews)
    top_pos_ph, top_neg_ph = extract_keyphrases(reviews)
    pos_themes = [t for t, v in theme_sentiment.items() if v["sentiment"] == "positive"]
    neg_themes = [t for t, v in theme_sentiment.items() if v["sentiment"] == "negative"]
    mixed_themes = [t for t, v in theme_sentiment.items() if v["sentiment"] == "mixed"]
    return {
        "orientation": orientation,
        "review_counts": counts,
        "positive_themes": sorted(pos_themes),
        "negative_themes": sorted(neg_themes),
        "mixed_themes": sorted(mixed_themes),
        "top_positive_phrases": top_pos_ph,
        "top_negative_phrases": top_neg_ph,
        "review_count": len(reviews),
        "raw_texts": [r["Text"] for r in reviews]
    }

# ----------------- Cleaning / validation -----------------
def cleanup_text(t: str) -> str:
    if not t: return t
    t = re.sub(r'\s+', ' ', t).strip()
    t = re.sub(r'\s+([.,!?;:])', r'\1', t)
    t = re.sub(r'([.,!?;:])([A-Za-z])', r'\1 \2', t)
    return t

def compress_repeats(text: str) -> str:
    if not text: return text
    t = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", text, flags=re.I)
    t = re.sub(r"\b(\w+\s+\w+)(\s+\1\b)+", r"\1", t, flags=re.I)
    return re.sub(r"\s+", " ", t).strip()

def sentences_too_similar(a: str, b: str) -> bool:
    A = set(re.findall(r"[a-z]+", a.lower())); B = set(re.findall(r"[a-z]+", b.lower()))
    if not A or not B: return False
    return (len(A & B) / len(A | B)) >= 0.85

def validate_final(summary: str, allow_similar: bool = False) -> bool:
    if not summary: return False
    s = summary.strip()
    s = strip_instruction_sentences(s)
    if REDACTED_TOKEN.lower() in s.lower() or "|" in s: return False
    if INSTRUCTION_ECHO_RE.search(s): return False
    parts = re.split(r'(?<=[.!?])\s+', s)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) != 2: return False
    for p in parts:
        core = p.strip(' "\'')
        if len(core) < MIN_SENT_LEN or len(core) > MAX_SENT_LEN: return False
    if not allow_similar and sentences_too_similar(parts[0], parts[1]): return False
    return True

def review_overlap(summary: str, raw_texts, threshold_jaccard: float = 0.65, ngram: int = 6) -> bool:
    if not summary:
        return False
    s = re.sub(r'\s+', ' ', summary.lower()).strip()
    s_words = re.findall(r"[a-z]+", s)
    s_set = set(s_words)
    if not s_set:
        return False
    for raw in raw_texts:
        r = re.sub(r'\s+', ' ', (raw or "").lower()).strip()
        r_words = re.findall(r"[a-z]+", r)
        if not r_words:
            continue
        r_set = set(r_words)
        j = len(s_set & r_set) / max(1, len(s_set | r_set))
        if j >= threshold_jaccard:
            return True
        if len(r_words) >= ngram:
            grams = {" ".join(r_words[i:i+ngram]) for i in range(0, len(r_words) - ngram + 1)}
            for g in grams:
                if g and g in s:
                    return True
    return False

# ----------------- Style variation to reduce repetition -----------------
VARIANTS_OPENING = {
    "predominantly positive": [
        "Feedback is largely positive",
        "Overall response is positive",
        "Reviews trend positive",
        "Sentiment skews positive"
    ],
    "predominantly negative": [
        "Feedback is largely negative",
        "Overall response is negative",
        "Reviews trend negative",
        "Sentiment skews negative"
    ],
    "mixed": [
        "Overall sentiment is mixed",
        "Feedback is mixed",
        "Reviews reflect mixed sentiment",
        "Sentiment is split"
    ],
}
VARIANTS_CONCERNS = [
    "Minor concerns are noted around {x}",
    "Some drawbacks relate to {x}",
    "A few issues involve {x}",
    "Users cite concerns about {x}",
]
VARIANTS_POSPERSIST = [
    "Strengths remain in {x}",
    "Positives persist in {x}",
    "There are still positives in {x}",
    "Notable strengths include {x}",
]

DEFAULT_NEG = ["quality", "durability", "availability", "support"]
DEFAULT_POS = ["performance", "value", "ease of use", "design"]

def choose_default(pool):
    # Stable per day (no reliance on randomized hash seed)
    try:
        seed = int(today_date_str().replace("-", ""))
    except Exception:
        seed = 0
    rnd = random.Random(seed)
    return ", ".join(rnd.sample(pool, 2))

def join_unique(seq, max_items=None):
    """
    Deduplicate (case-insensitive), strip, and join with commas and 'and'.
    """
    seen = set()
    out = []
    for x in seq or []:
        if not x:
            continue
        t = str(x).strip()
        tl = t.lower()
        if tl in seen:
            continue
        seen.add(tl)
        out.append(t)
    if max_items is not None:
        out = out[:max_items]
    if not out:
        return ""
    if len(out) == 1:
        return out[0]
    if len(out) == 2:
        return f"{out[0]} and {out[1]}"
    return f"{', '.join(out[:-1])}, and {out[-1]}"

def vary_style(summary: str, orientation: str) -> str:
    if not summary:
        return summary
    parts = re.split(r'(?<=[.!?])\s+', summary.strip())
    parts = [p for p in parts if p]
    if len(parts) != 2:
        return summary
    s1, s2 = parts

    # Vary S1 opening
    if orientation in VARIANTS_OPENING:
        if re.match(r'^(sentiment is|overall sentiment is|feedback is)\s+predominantly\s+positive', s1, flags=re.I):
            s1 = re.sub(r'^(sentiment is|overall sentiment is|feedback is)\s+predominantly\s+positive',
                        random.choice(VARIANTS_OPENING["predominantly positive"]), s1, flags=re.I)
        if re.match(r'^(sentiment is|overall sentiment is|feedback is)\s+predominantly\s+negative', s1, flags=re.I):
            s1 = re.sub(r'^(sentiment is|overall sentiment is|feedback is)\s+predominantly\s+negative',
                        random.choice(VARIANTS_OPENING["predominantly negative"]), s1, flags=re.I)
        if re.match(r'^(sentiment is|overall sentiment is|feedback is)\s+mixed', s1, flags=re.I):
            s1 = re.sub(r'^(sentiment is|overall sentiment is|feedback is)\s+mixed',
                        random.choice(VARIANTS_OPENING["mixed"]), s1, flags=re.I)

    # Vary S2
    s2 = re.sub(r'^(Minor concerns remain around)\s+(.+)',
                lambda m: random.choice(VARIANTS_CONCERNS).format(x=m.group(2)), s2, flags=re.I)
    s2 = re.sub(r'^(Some strengths persist in)\s+(.+)',
                lambda m: random.choice(VARIANTS_POSPERSIST).format(x=m.group(2)), s2, flags=re.I)

    out = f"{s1.rstrip('. ')}. {s2.rstrip('. ')}."
    out = re.sub(r'\s+', ' ', out).strip()
    return out

# ----------------- SageMaker response parsing -----------------
def parse_sagemaker_response_bytes(raw_bytes):
    if raw_bytes is None: return ""
    try:
        txt = raw_bytes.decode("utf-8")
    except Exception:
        try:
            txt = raw_bytes.decode("latin-1")
        except Exception:
            return ""
    if not txt:
        return ""
    stripped = txt.strip()
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            for k in ("summary_text","generated_text","summary","text"):
                if isinstance(parsed.get(k), str): return parsed[k]
            if isinstance(parsed.get("predictions"), list) and parsed["predictions"]:
                first = parsed["predictions"][0]
                if isinstance(first, dict):
                    for k in ("summary_text","generated_text","summary","text"):
                        if isinstance(first.get(k), str): return first[k]
                elif isinstance(first, str):
                    return first
        elif isinstance(parsed, list):
            acc = []
            for el in parsed:
                if isinstance(el, dict):
                    for k in ("summary_text","generated_text","summary","text"):
                        if isinstance(el.get(k), str):
                            acc.append(el[k]); break
                elif isinstance(el, str):
                    acc.append(el)
            if acc:
                return " ".join(acc)
    except Exception:
        pass
    return stripped

# ----------------- Model Invocation -----------------
def call_sagemaker(body_text: str) -> str:
    endpoint = normalize_endpoint_name(SAGEMAKER_ENDPOINT)
    if not endpoint:
        raise RuntimeError("No endpoint configured")
    resp = sagemaker_rt.invoke_endpoint(
        EndpointName=endpoint,
        Body=body_text.encode("utf-8"),
        ContentType="application/x-text",
        Accept="*/*"
    )
    raw = resp["Body"].read()
    return parse_sagemaker_response_bytes(raw)

# ----------------- Prompt Construction -----------------
def build_generation_prompt(struct):
    # Keep instructions minimal and non-echo-prone; rely on repair/validation for exact two sentences.
    instr = "Paraphrase succinctly in neutral business English without PII or quotes."
    # Provide only sanitized snippets, no labels
    snips = []
    for raw in struct["raw_texts"][:6]:
        sn = strip_meta_instructions_and_pii(raw)
        if len(sn) > 120:
            sn = sn[:120].rsplit(' ', 1)[0]
        snips.append(sn)
    snippet_block = " ".join(snips)
    return f"{instr} {snippet_block}"

# ----------------- Repair / paraphrase -----------------
def repair_model_output(txt, orientation_hint=None):
    if not txt:
        return ""
    t = strip_meta_instructions_and_pii(txt)
    # Split to sentences
    parts = re.split(r'(?<=[.!?])\s+', t)
    # Drop instruction/context-like sentences or fragments we see echoing
    parts = [p.strip(' "\'') for p in parts if p.strip() and not INSTRUCTION_ECHO_RE.search(p) and not INSTRUCTION_LABEL_RE.search(p)]
    # Also explicitly drop known boilerplate openings
    DROP_STARTS = re.compile(r'^(focus on the overall picture|strengths such as|concerns like|themes including|use this context)\b', flags=re.I)
    parts = [p for p in parts if not DROP_STARTS.search(p)]
    if not parts:
        return ""
    if len(parts) == 1:
        hint = orientation_hint or "overall sentiment"
        parts.append(f"Further feedback reflects {hint}.")
    elif len(parts) > 2:
        parts = parts[:2]

    def dedupe_commas(s):
        # Remove duplicated tokens separated by commas (e.g., "performance, performance")
        s = re.sub(r'\b(\w+)\s*,\s*\1\b', r'\1', s, flags=re.I)
        # Collapse accidental duplicate commas
        s = re.sub(r',\s*,', ',', s)
        # Token-level dedupe for comma-separated lists
        tokens = [w.strip() for w in s.split(",")]
        seen = set()
        cleaned = []
        for tok in tokens:
            tl = tok.lower().strip()
            if tl in seen or not tok:
                continue
            seen.add(tl)
            cleaned.append(tok)
        return ", ".join(cleaned)

    norm = []
    for p in parts[:2]:
        p = re.sub(r'(?i)\breviewers?\s*:\s*', '', p)
        p = p.replace('"', '')
        p = compress_repeats(p)
        p = dedupe_commas(p)
        if not re.search(r'[.!?]$', p):
            p += '.'
        norm.append(p)
    out = " ".join(norm)
    out = strip_instruction_sentences(out)
    out = strip_meta_instructions_and_pii(out)
    out = compress_repeats(out)
    out = re.sub(r'\s+([.,!?;:])', r'\1', out)
    out = re.sub(r'([.,!?;:])([A-Za-z])', r'\1 \2', out)
    return out.strip()

def deterministic_paraphrase(struct):
    o    = struct["orientation"]
    pos  = struct["positive_themes"]
    neg  = struct["negative_themes"]
    mix  = struct["mixed_themes"]
    posp = struct["top_positive_phrases"]
    negp = struct["top_negative_phrases"]

    # Build clean text from themes/phrases
    pos_text = join_unique(pos) or join_unique(posp) or choose_default(DEFAULT_POS)
    neg_text = join_unique(neg) or join_unique(negp)  # may be empty
    mix_text = join_unique(mix)

    if o == "predominantly positive":
        s1 = f"Feedback is largely positive, highlighting {pos_text}."
        if neg_text:
            s2 = random.choice(VARIANTS_CONCERNS).format(x=neg_text) + "."
        else:
            s2 = "Few notable concerns are raised."
    elif o == "predominantly negative":
        if not neg_text:
            neg_text = choose_default(DEFAULT_NEG)
        s1 = f"Reviews trend negative, driven by issues in {neg_text}."
        if pos_text or mix_text:
            strengths = pos_text if pos_text else (mix_text or choose_default(DEFAULT_POS))
            s2 = random.choice(VARIANTS_POSPERSIST).format(x=strengths) + "."
        else:
            s2 = "Positive remarks are limited."
    else:
        s1 = f"Feedback is mixed, balancing positives in {pos_text} with concerns about {neg_text or choose_default(DEFAULT_NEG)}."
        if mix_text:
            s2 = f"Additional themes include {mix_text}."
        elif neg_text:
            s2 = random.choice(VARIANTS_CONCERNS).format(x=neg_text) + "."
        else:
            s2 = "Concerns are not concentrated on a single theme."

    return repair_model_output(f"{s1} {s2}", o)

# ----------------- Model summarization with fallback -----------------
def model_only_summary(reviews):
    struct = build_structured_summary(reviews)
    prompt = build_generation_prompt(struct)
    raw    = call_sagemaker(prompt)
    fixed  = repair_model_output(raw, struct["orientation"])
    if not review_overlap(fixed, struct["raw_texts"]) and validate_final(fixed):
        return fixed

    # Retry with shuffled raw texts altering prompt
    shuffled = list(struct["raw_texts"])
    random.shuffle(shuffled)
    struct2 = dict(struct); struct2["raw_texts"] = shuffled
    prompt2 = build_generation_prompt(struct2)
    raw2    = call_sagemaker(prompt2)
    fixed2  = repair_model_output(raw2, struct["orientation"])
    if not review_overlap(fixed2, shuffled) and validate_final(fixed2):
        return fixed2

    # Deterministic final fallback
    det = deterministic_paraphrase(struct)
    if validate_final(det, allow_similar=True):
        return det

    # Debug dump
    if DEBUG_PROMPTS and DEBUG_S3_BUCKET:
        try:
            key = f"{DEBUG_S3_PREFIX.rstrip('/')}/invalid/{datetime.utcnow().isoformat()}Z.json"
            dbg = {
                "prompt": prompt[:600], "raw": raw[:600], "fixed": fixed[:300],
                "prompt_retry": prompt2[:600], "raw_retry": raw2[:600], "fixed_retry": fixed2[:300],
                "deterministic": det[:300]
            }
            s3.put_object(Bucket=DEBUG_S3_BUCKET, Key=key, Body=json.dumps(dbg, ensure_ascii=False).encode("utf-8"), ContentType="application/json")
        except Exception:
            LOG.exception("Debug dump failed")
    raise RuntimeError("Model-only output failed validation")

# ----------------- Numeric helpers -----------------
def convert_numeric_types(obj):
    if isinstance(obj, float): return Decimal(str(obj))
    if isinstance(obj, int): return obj
    if isinstance(obj, Decimal): return obj
    if isinstance(obj, dict): return {k: convert_numeric_types(v) for k, v in obj.items()}
    if isinstance(obj, list): return [convert_numeric_types(v) for v in obj]
    return obj

# ----------------- Write Summary -----------------
def write_summary_to_table(product_id, summary_text, review_count, source_meta=None):
    now = now_iso(); date_str = today_date_str()
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
                summary_table.put_item(Item=item)
            else:
                raise
    else:
        summary_table.put_item(Item=item)

    if SUMMARY_S3_BUCKET:
        key = f"{SUMMARY_S3_PREFIX.rstrip('/')}/date={date_str}/product={product_id}.json"
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
    since_hours = int(event.get("since_hours", PROCESS_LOOKBACK_HOURS))
    since_epoch = epoch_seconds_ago(since_hours)
    product_list = event.get("product_list")
    results = []

    # Model readiness gate
    if REQUIRE_MODEL:
        ok, status = model_ready()
        if not ok:
            LOG.warning("Model required not ready: %s", status)
            return {"status": "skipped", "reason": f"model_not_ready:{status}"}

    # Discover products if not provided
    if not product_list:
        seen = set()
        scan_kwargs = {"FilterExpression": Attr("ProcessedTimestampEpoch").gte(since_epoch), "ProjectionExpression": "ProductID"}
        start = None
        while True:
            if start:
                scan_kwargs["ExclusiveStartKey"] = start
            resp = review_table.scan(**scan_kwargs)
            for it in resp.get("Items", []):
                pid = it.get("ProductID")
                if pid:
                    seen.add(pid)
            start = resp.get("LastEvaluatedKey")
            if not start:
                break
        product_list = list(seen)
        LOG.info("Discovered products: %s", product_list)

    # Per-product processing
    for product in product_list:
        try:
            reviews = query_reviews_for_product(product, since_epoch)
            if not reviews:
                results.append({"product": product, "status": "no_reviews"})
                continue

            # 1) Generate summary
            if REQUIRE_MODEL:
                summary = model_only_summary(reviews)
            else:
                summary = deterministic_paraphrase(build_structured_summary(reviews))

            # 2) Clean instruction/context echoes; fallback if needed
            cleaned_for_write = strip_instruction_sentences(summary)
            if INSTRUCTION_LABEL_RE.search(cleaned_for_write) or INSTRUCTION_ECHO_RE.search(cleaned_for_write):
                struct = build_structured_summary(reviews)
                det = deterministic_paraphrase(struct)
                if validate_final(det, allow_similar=True) and not review_overlap(det, struct["raw_texts"]):
                    summary = det
                else:
                    summary = cleaned_for_write
            else:
                summary = cleaned_for_write

            # 3) Derive orientation for style variance, vary, and re-validate
            orientation, counts = derive_orientation(reviews)
            summary = vary_style(summary, orientation)
            # Extra dedupe in case variation introduced repeats
            summary = re.sub(r'\b(\w+)\s*,\s*\1\b', r'\1', summary, flags=re.I)

            if not validate_final(summary):
                struct = build_structured_summary(reviews)
                det = deterministic_paraphrase(struct)
                if validate_final(det, allow_similar=True):
                    summary = det

            # 4) Meta + write
            theme_sentiment = derive_theme_sentiment(reviews)
            source_meta = {
                "method": "sagemaker_structured" if REQUIRE_MODEL else "deterministic_only",
                "polished": False,
                "orientation": orientation,
                "review_counts": counts,
                "theme_sentiment": theme_sentiment
            }

            write_summary_to_table(product, summary, len(reviews), source_meta)
            results.append({
                "product": product,
                "status": "summarized",
                "summary_len": len(summary),
                "reviews": len(reviews),
                "orientation": orientation
            })
        except Exception as e:
            LOG.exception("Processing error for %s: %s", product, e)
            results.append({"product": product, "status": "error", "error": str(e)})

    LOG.info("Completed summarization run. results=%s", json.dumps(results))
    return {"status": "finished", "results": results}

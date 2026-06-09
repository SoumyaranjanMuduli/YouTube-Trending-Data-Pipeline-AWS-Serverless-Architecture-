"""
Lambda: JSON Reference Data → Silver Layer (Parquet)
────────────────────────────────────────────────────
Function Name : yt-pipeline-json-to-parquet-dev
Trigger       : S3 PUT on Bronze bucket
                prefix  = youtube/raw_statistics_reference_data/
                suffix  = .json

Bronze path structure (confirmed from S3 console):
    s3://yt-data-pipeline-brozne-ap-south-soumya/
        youtube/raw_statistics_reference_data/
            region=ca/
                CA_category_id.json        ← uppercase filename, lowercase folder

Silver output:
    s3://yt-data-pipeline-silver-ap-south-soumya/
        youtube/reference_data/
            region=ca/
                <snappy parquet files>

Environment Variables (set in Lambda Console → Configuration → Environment variables):
    BRONZE_BUCKET        = yt-data-pipeline-brozne-ap-south-soumya   ← ADD THIS (missing!)
    SILVER_BUCKET        = yt-data-pipeline-silver-ap-south-soumya
    GLUE_DB_SILVER       = yt_pipeline_silver_dev
    GLUE_TABLE_REFERENCE = clean_reference_data
    SNS_ALERT_TOPIC_ARN  = arn:aws:sns:ap-south-1:937445573232:yt-pipeline-alerts-dev:a7491995-941c-4953-b578-9f8eb53ec73d

Fixes applied:
    [1] Test event was empty → added fallback direct S3 read mode
    [2] BRONZE_BUCKET env var was missing → hardcoded fallback added
    [3] Region extracted from folder path (region=ca/) not filename
    [4] Works for both S3-triggered and manually tested invocations
    [5] logger.warning() replaced with logger.warning() — standard Python
        logging (NOT Glue logger — this is Lambda, standard logging works fine)
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config from Environment Variables ────────────────────────────────────────
# FIX [1]: BRONZE_BUCKET was missing from Lambda env vars — added fallback
BRONZE_BUCKET = os.environ.get(
    "BRONZE_BUCKET",
    "yt-data-pipeline-brozne-ap-south-soumya"    # ← your actual bronze bucket
)
SILVER_BUCKET = os.environ.get(
    "SILVER_BUCKET",
    "yt-data-pipeline-silver-ap-south-soumya"    # ← your actual silver bucket
)
GLUE_DB    = os.environ.get("GLUE_DB_SILVER",       "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC  = os.environ.get("SNS_ALERT_TOPIC_ARN",  "")

# ── S3 Paths ──────────────────────────────────────────────────────────────────
BRONZE_PREFIX = "youtube/raw_statistics_reference_data/"
SILVER_PATH   = f"s3://{SILVER_BUCKET}/youtube/reference_data/"

# ── AWS Clients ───────────────────────────────────────────────────────────────
s3_client  = boto3.client("s3")
sns_client = boto3.client("sns", region_name="ap-south-1")

logger.info(f"Config loaded — Bronze: {BRONZE_BUCKET} | Silver: {SILVER_BUCKET}")
logger.info(f"Glue: {GLUE_DB}.{GLUE_TABLE}")
logger.info(f"Silver path: {SILVER_PATH}")


# ── HELPER: Extract region from S3 key ───────────────────────────────────────
def extract_region(key: str) -> str:
    """
    Extract region from S3 key path.

    Handles both formats seen in your S3 console:
      - youtube/raw_statistics_reference_data/region=ca/CA_category_id.json → 'ca'
      - youtube/raw_statistics_reference_data/CA_category_id.json           → 'ca' (from filename)
      - youtube/raw_statistics_reference_data/US_category_id.json           → 'us' (from filename)

    Always returns lowercase.
    """
    parts = key.split("/")

    # Try folder-based region first: region=ca/
    for part in parts:
        if part.lower().startswith("region="):
            return part.split("=")[1].lower().strip()

    # Fallback: extract from filename like CA_category_id.json or US_category_id.json
    filename = parts[-1]  # e.g. CA_category_id.json
    if "_category_id" in filename.lower():
        region_code = filename.split("_")[0].lower()  # 'ca', 'us', 'gb' etc.
        if len(region_code) == 2:
            return region_code

    logger.warning(f"Could not extract region from key: {key} — using 'unknown'")
    return "unknown"


# ── HELPER: Read raw JSON from S3 ────────────────────────────────────────────
def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Read and parse JSON file from S3.
    Uses raw boto3 (not awswrangler) because YouTube category JSON
    has mixed top-level types that pandas cannot normalize directly.
    """
    logger.info(f"Reading s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content  = response["Body"].read().decode("utf-8")
    return json.loads(content)


# ── HELPER: Normalize JSON to DataFrame ──────────────────────────────────────
def normalize_category_json(raw_data: dict, key: str) -> pd.DataFrame:
    """
    Normalize YouTube category JSON → flat pandas DataFrame.

    YouTube category JSON structure:
    {
      "kind": "youtube#videoCategoryListResponse",
      "etag": "...",
      "items": [
        {
          "kind": "youtube#videoCategory",
          "etag": "...",
          "id": "1",
          "snippet": { "title": "Film & Animation", "assignable": true, "channelId": "UCBR..." }
        },
        ...
      ]
    }
    """
    if "items" in raw_data and isinstance(raw_data["items"], list):
        df = pd.json_normalize(raw_data["items"])
        logger.info(f"  Extracted {len(raw_data['items'])} items from 'items' array")
    else:
        logger.warning("  No 'items' key found — normalizing entire JSON")
        df = pd.json_normalize(raw_data)

    if df.empty:
        raise ValueError(f"Empty DataFrame after normalizing {key}")

    logger.info(f"  Columns after normalize: {list(df.columns)}")
    return df


# ── HELPER: Clean DataFrame ───────────────────────────────────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names, remove duplicates, cast types.
    """
    # Flatten dot-notation column names: snippet.title → snippet_title
    df.columns = [col.replace(".", "_") for col in df.columns]

    # Deduplicate on category id
    if "id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["id"], keep="last")
        if len(df) < before:
            logger.info(f"  Removed {before - len(df)} duplicate category IDs")

    # Cast id to string (sometimes comes as int)
    if "id" in df.columns:
        df["id"] = df["id"].astype(str)

    return df


# ── HELPER: SNS Alert ─────────────────────────────────────────────────────────
def send_alert(subject: str, message: str):
    """Send SNS failure alert. Silently skips if no topic configured."""
    if not SNS_TOPIC:
        logger.warning("SNS_ALERT_TOPIC_ARN not set — skipping alert")
        return
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message,
        )
        logger.info(f"SNS alert sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {e}")


# ── HELPER: Process a single S3 file ─────────────────────────────────────────
def process_file(bucket: str, key: str) -> dict:
    """
    Full pipeline for one JSON file:
      Read → Normalize → Clean → Add metadata → Write Parquet to Silver
    Returns a summary dict.
    """
    # Step 1: Read JSON
    raw_data = read_json_from_s3(bucket, key)

    # Step 2: Normalize to DataFrame
    df = normalize_category_json(raw_data, key)

    # Step 3: Clean
    df = clean_dataframe(df)

    # Step 4: Add metadata
    region = extract_region(key)
    df["region"]               = region
    df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"]         = key

    logger.info(f"  Final shape: {df.shape} | Region: {region}")
    logger.info(f"  Sample columns: {list(df.columns)}")

    # Step 5: Write Parquet to Silver (partitioned by region)
    wr.s3.to_parquet(
        df=df,
        path=SILVER_PATH,
        dataset=True,
        database=GLUE_DB,
        table=GLUE_TABLE,
        partition_cols=["region"],
        mode="overwrite_partitions",
        schema_evolution=True,
        compression="snappy",
    )

    silver_out = f"{SILVER_PATH}region={region}/"
    logger.info(f"  ✅ Written to: {silver_out} ({len(df)} rows)")

    return {
        "source_bucket": bucket,
        "key":           key,
        "region":        region,
        "rows_written":  len(df),
        "silver_path":   silver_out,
    }


# ── MAIN HANDLER ─────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Entry point.

    Handles 3 invocation types:
      1. S3 trigger (normal operation)     → event has Records[].s3
      2. Manual test with S3 key           → event has { "bucket": "...", "key": "..." }
      3. Manual test with no payload       → scans all JSON files in Bronze prefix

    Returns:
      { "statusCode": 200/207, "processed": [...], "errors": [...] }
    """
    logger.info(f"Event received: {json.dumps(event)[:500]}")

    records   = []
    processed = []
    errors    = []

    # ── Mode 1: Real S3 trigger ───────────────────────────────────────────────
    s3_records = event.get("Records", [])
    if s3_records:
        logger.info(f"S3 trigger mode — {len(s3_records)} record(s)")
        for rec in s3_records:
            try:
                bucket = rec["s3"]["bucket"]["name"]
                key    = unquote_plus(rec["s3"]["object"]["key"])
                records.append((bucket, key))
            except KeyError as e:
                logger.error(f"Malformed S3 record: {e}")

    # ── Mode 2: Manual test with explicit bucket+key ──────────────────────────
    elif "bucket" in event and "key" in event:
        logger.info("Manual test mode — using bucket/key from event")
        records.append((event["bucket"], event["key"]))

    # ── Mode 3: No payload — scan all JSON files in Bronze prefix ─────────────
    # FIX [2]: Instead of doing nothing, scan Bronze and process all JSON files
    else:
        logger.info("No S3 records in event — scanning Bronze bucket for all JSON files...")
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX)
        for page in pages:
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".json"):
                    logger.info(f"  Found: {k}")
                    records.append((BRONZE_BUCKET, k))

        if not records:
            logger.warning(f"No JSON files found under s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}")
            return {"statusCode": 200, "processed": [], "errors": [], "message": "No files to process"}

    logger.info(f"Total files to process: {len(records)}")

    # ── Process each file ─────────────────────────────────────────────────────
    for bucket, key in records:
        try:
            result = process_file(bucket, key)
            processed.append(result)
        except Exception as e:
            logger.error(f"Failed: s3://{bucket}/{key} — {e}", exc_info=True)
            errors.append({"key": key, "error": str(e)})

    # ── Alert on errors ───────────────────────────────────────────────────────
    if errors:
        send_alert(
            subject="[YT Pipeline] Silver reference transform FAILED",
            message=json.dumps(errors, indent=2),
        )

    logger.info(f"Done — Processed: {len(processed)} | Errors: {len(errors)}")

    return {
        "statusCode": 200 if not errors else 207,
        "processed":  processed,
        "errors":     errors,
    }
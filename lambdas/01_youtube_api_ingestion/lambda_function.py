"""
Lambda: YouTube Data API Ingestion (Bronze Layer)
──────────────────────────────────────────────────
Function name : yt-pipeline-youtube-ingestion-dev
Runtime       : Python 3.12
Triggered by  : Amazon EventBridge (schedule every 6 hours)
Role          : yt-pipeline-lambda-role

What it does:
  1. Calls YouTube Data API v3 for each of 10 regions
  2. Fetches top-50 trending videos  (raw_statistics)
  3. Fetches video category mapping  (raw_statistics_reference_data)
  4. Writes both as JSON to Bronze S3 bucket with Hive partitions
  5. Sends SNS alert if any region fails

Environment Variables to set in Lambda console:
  YOUTUBE_API_KEY       → your Google API key (YouTube Data API v3 enabled)
  S3_BUCKET_BRONZE      → yt-data-pipeline-brozne-ap-south-soumya
  YOUTUBE_REGIONS       → US,GB,CA,DE,FR,IN,JP,KR,MX,RU
  SNS_ALERT_TOPIC_ARN   → arn:aws:sns:ap-south-1:937445573232:yt-pipeline-alerts-dev

S3 output paths created:
  youtube/raw_statistics/region=ca/date=2026-06-07/hour=14/20260607_141530.json
  youtube/raw_statistics_reference_data/region=ca/date=2026-06-07/ca_category_id.json
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

import boto3

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS Clients ───────────────────────────────────────────────────────────────
s3_client  = boto3.client("s3")
sns_client = boto3.client("sns")

# ── Config (from environment variables) ──────────────────────────────────────
API_KEY    = os.environ["YOUTUBE_API_KEY"]
BUCKET     = os.environ["S3_BUCKET_BRONZE"]
REGIONS    = os.environ.get("YOUTUBE_REGIONS", "US,GB,CA,DE,FR,IN,JP,KR,MX,RU").split(",")
SNS_TOPIC  = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
API_BASE   = "https://www.googleapis.com/youtube/v3"
MAX_RESULTS = 50


def fetch_trending_videos(region_code: str) -> dict:
    """Call YouTube Data API — get top 50 trending videos for a region."""
    params = urlencode({
        "part":        "snippet,statistics,contentDetails",
        "chart":       "mostPopular",
        "regionCode":  region_code,
        "maxResults":  MAX_RESULTS,
        "key":         API_KEY,
    })
    url = f"{API_BASE}/videos?{params}"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_video_categories(region_code: str) -> dict:
    """Call YouTube Data API — get category ID → name mapping for a region."""
    params = urlencode({
        "part":       "snippet",
        "regionCode": region_code,
        "key":        API_KEY,
    })
    url = f"{API_BASE}/videoCategories?{params}"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def write_to_s3(data: dict, bucket: str, key: str):
    """Write a Python dict as JSON to S3."""
    body = json.dumps(data, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "youtube_data_api_v3",
        },
    )


def send_alert(subject: str, message: str):
    """Send SNS failure alert."""
    if SNS_TOPIC:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message,
        )


def lambda_handler(event, context):
    """
    Main entry point.
    EventBridge passes an empty event {}.
    Step Functions passes {"triggered_by": "step_functions", ...}.
    Both work — the handler ignores the event payload.
    """
    now              = datetime.now(timezone.utc)
    date_partition   = now.strftime("%Y-%m-%d")
    hour_partition   = now.strftime("%H")
    ingestion_id     = now.strftime("%Y%m%d_%H%M%S")

    results = {"success": [], "failed": []}

    for region in REGIONS:
        region = region.strip().lower()
        logger.info(f"Processing region: {region}")

        # ── Step 1: fetch trending videos ─────────────────────────────────
        try:
            trending_data = fetch_trending_videos(region)
            video_count   = len(trending_data.get("items", []))

            trending_data["_pipeline_metadata"] = {
                "ingestion_id":        ingestion_id,
                "region":              region,
                "ingestion_timestamp": now.isoformat(),
                "video_count":         video_count,
                "source":              "youtube_data_api_v3",
            }

            # Hive-partitioned path:
            # youtube/raw_statistics/region=ca/date=2026-06-07/hour=14/20260607_141530.json
            s3_key = (
                f"youtube/raw_statistics/"
                f"region={region}/"
                f"date={date_partition}/"
                f"hour={hour_partition}/"
                f"{ingestion_id}.json"
            )
            write_to_s3(trending_data, BUCKET, s3_key)
            logger.info(f"  Wrote {video_count} videos → s3://{BUCKET}/{s3_key}")

        except (HTTPError, URLError) as e:
            logger.error(f"  API error for {region} trending: {e}")
            results["failed"].append({"region": region, "type": "trending", "error": str(e)})
            continue
        except Exception as e:
            logger.error(f"  Unexpected error for {region} trending: {e}")
            results["failed"].append({"region": region, "type": "trending", "error": str(e)})
            continue

        # ── Step 2: fetch category reference data ──────────────────────────
        try:
            category_data = fetch_video_categories(region)
            category_data["_pipeline_metadata"] = {
                "ingestion_id":        ingestion_id,
                "region":              region,
                "ingestion_timestamp": now.isoformat(),
                "source":              "youtube_data_api_v3",
            }

            # youtube/raw_statistics_reference_data/region=ca/date=2026-06-07/ca_category_id.json
            ref_key = (
                f"youtube/raw_statistics_reference_data/"
                f"region={region}/"
                f"date={date_partition}/"
                f"{region}_category_id.json"
            )
            write_to_s3(category_data, BUCKET, ref_key)
            logger.info(f"  Wrote categories → s3://{BUCKET}/{ref_key}")

        except (HTTPError, URLError) as e:
            logger.error(f"  API error for {region} categories: {e}")
            results["failed"].append({"region": region, "type": "categories", "error": str(e)})
            continue

        results["success"].append(region)

    # ── Summary ────────────────────────────────────────────────────────────────
    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}/{len(REGIONS)} regions. "
        f"Failed: {len(results['failed'])}."
    )
    logger.info(summary)

    if results["failed"]:
        send_alert(
            subject=f"[YT Pipeline] Ingestion partial failure — {ingestion_id}",
            message=json.dumps(results, indent=2),
        )

    return {
        "statusCode":   200,
        "ingestion_id": ingestion_id,
        "results":      results,
    }
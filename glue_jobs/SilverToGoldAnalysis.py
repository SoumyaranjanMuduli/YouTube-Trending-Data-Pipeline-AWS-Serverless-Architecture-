"""
Glue Job: Silver → Gold (Analytics Aggregations)
─────────────────────────────────────────────────
Reads cleansed statistics and reference data from Silver,
joins them, and produces business-level aggregations in Gold layer.

Resources:
  Silver Bucket : yt-data-pipeline-silver-ap-south-soumya
  Gold Bucket   : yt-data-pipeline-gold-ap-south-soumya
  Silver DB     : yt_pipeline_silver_dev
  Gold DB       : yt_pipeline_gold_dev
  SNS ARN       : arn:aws:sns:ap-south-1:937445573232:yt-pipeline-alerts-dev

Gold tables produced:
  1. trending_analytics  — Daily trending summaries per region
  2. channel_analytics   — Channel performance metrics
  3. category_analytics  — Category-level trends over time

Job Parameters (set in Glue console):
    --JOB_NAME        — Glue job name (auto-set)
    --silver_database — yt_pipeline_silver_dev
    --gold_bucket     — yt-data-pipeline-gold-ap-south-soumya
    --gold_database   — yt_pipeline_gold_dev

Fixes applied vs original:
  [1] Docstring moved to top of file (was below imports)
  [2] SNS alerting added — job failures now send alerts
  [3] Top-level try/except added — all 3 Gold writes protected
  [4] .count() after write removed — was re-triggering full Spark job
      Replaced with cached count computed BEFORE write
  [5] Silver table names driven by constants (easy to change)
  [6] logger.warn() used throughout (correct for Glue, not .warning())
"""

import sys
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Job Setup ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "gold_bucket",
    "gold_database",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# ── Config ────────────────────────────────────────────────────────────────────
SILVER_DB   = args["silver_database"]    # yt_pipeline_silver_dev
GOLD_BUCKET = args["gold_bucket"]        # yt-data-pipeline-gold-ap-south-soumya
GOLD_DB     = args["gold_database"]      # yt_pipeline_gold_dev

# FIX [5]: Silver table names as constants — easy to change in one place
SILVER_STATS_TABLE = "clean_statistics"
SILVER_REF_TABLE   = "clean_reference_data"

SNS_ARN = "arn:aws:sns:ap-south-1:937445573232:yt-pipeline-alerts-dev"

logger.info(f"Silver DB  : {SILVER_DB}")
logger.info(f"Gold DB    : {GOLD_DB}")
logger.info(f"Gold Bucket: {GOLD_BUCKET}")


# ── SNS Alert Helper ──────────────────────────────────────────────────────────
# FIX [2]: SNS alerting added — original had no alerts at all
def send_sns_alert(subject, message):
    """Send SNS failure alert. Uses logger.warn() — correct for Glue."""
    try:
        sns = boto3.client("sns", region_name="ap-south-1")
        sns.publish(TopicArn=SNS_ARN, Subject=subject, Message=message)
        logger.info(f"SNS alert sent: {subject}")
    except Exception as e:
        logger.warn(f"Failed to send SNS alert: {str(e)}")


# ── Helper: Write Gold Table ──────────────────────────────────────────────────
def write_gold_table(df, table_name, path, partition_keys=None):
    """
    Write a DataFrame to the Gold S3 layer and update Glue catalog.
    FIX [4]: row_count computed via cache BEFORE write — not after.
             Original called .count() after write which re-triggered
             the entire Spark computation unnecessarily.
    """
    if partition_keys is None:
        partition_keys = ["region"]

    # Cache and count BEFORE write — avoids re-computation
    df.cache()
    row_count = df.count()

    dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    sink = glueContext.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
    )
    sink.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName=table_name)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dyf)

    df.unpersist()
    logger.info(f"  Gold table '{table_name}': {row_count} rows → {path}")
    return row_count


# ── Main Job Logic ────────────────────────────────────────────────────────────
# FIX [3]: Full try/except wrapper — failures trigger SNS alert
try:

    # ── Read Silver: Statistics ───────────────────────────────────────────────
    logger.info(f"Reading Silver statistics: {SILVER_DB}.{SILVER_STATS_TABLE}")

    stats_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name=SILVER_STATS_TABLE,
        transformation_ctx="stats",
    )
    stats_df = stats_dyf.toDF()

    stats_count = stats_df.count()
    logger.info(f"Statistics records loaded: {stats_count}")

    if stats_count == 0:
        logger.info("No statistics data found in Silver. Nothing to aggregate.")
        job.commit()
        sys.exit(0)


    # ── Read Silver: Reference Data (category lookup) ─────────────────────────
    logger.info(f"Reading Silver reference data: {SILVER_DB}.{SILVER_REF_TABLE}")

    category_lookup = None
    try:
        ref_dyf = glueContext.create_dynamic_frame.from_catalog(
            database=SILVER_DB,
            table_name=SILVER_REF_TABLE,
            transformation_ctx="ref",
        )
        ref_df = ref_dyf.toDF()

        # Handle both possible column naming formats from Glue crawler
        # Format A: snippet_title (crawler flattened with underscore)
        # Format B: snippet.title (raw dot notation)
        if "id" in ref_df.columns and "snippet_title" in ref_df.columns:
            category_lookup = ref_df.select(
                F.col("id").cast("long").alias("category_id"),
                F.col("snippet_title").alias("category_name"),
            ).dropDuplicates(["category_id"])

        elif "id" in ref_df.columns and "snippet.title" in ref_df.columns:
            category_lookup = ref_df.select(
                F.col("id").cast("long").alias("category_id"),
                F.col("`snippet.title`").alias("category_name"),
            ).dropDuplicates(["category_id"])

        else:
            logger.warn(
                f"Cannot find category title column. "
                f"Columns in reference data: {ref_df.columns}. "
                f"Proceeding without category names."
            )

        if category_lookup is not None:
            ref_count = category_lookup.count()
            logger.info(f"Category lookup loaded: {ref_count} entries")

            # Join category names onto stats
            stats_df = stats_df.withColumn("category_id", F.col("category_id").cast("long"))
            stats_df = stats_df.join(
                F.broadcast(category_lookup),
                on="category_id",
                how="left",
            )

    except Exception as ref_err:
        logger.warn(f"Could not load reference data: {str(ref_err)}. Proceeding without category names.")

    # Always guarantee category_name column exists for all 3 Gold tables
    if "category_name" not in stats_df.columns:
        stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))
    else:
        stats_df = stats_df.fillna("Unknown", subset=["category_name"])

    # Re-cache after join
    stats_df.cache()


    # ── Gold Table 1: Trending Analytics ─────────────────────────────────────
    logger.info("Building Gold table: trending_analytics...")

    trending = stats_df.groupBy("region", "trending_date_parsed").agg(
        F.count("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("dislikes").alias("total_dislikes"),
        F.sum("comment_count").alias("total_comments"),
        F.round(F.avg("views"), 2).alias("avg_views_per_video"),
        F.round(F.avg("like_ratio"), 4).alias("avg_like_ratio"),
        F.round(F.avg("engagement_rate"), 4).alias("avg_engagement_rate"),
        F.max("views").alias("max_views"),
        F.countDistinct("channel_title").alias("unique_channels"),
        F.countDistinct("category_id").alias("unique_categories"),
    ).withColumn("_aggregated_at", F.current_timestamp())

    write_gold_table(
        df=trending,
        table_name="trending_analytics",
        path=f"s3://{GOLD_BUCKET}/youtube/trending_analytics/",
        partition_keys=["region"],
    )


    # ── Gold Table 2: Channel Analytics ──────────────────────────────────────
    logger.info("Building Gold table: channel_analytics...")

    channel = stats_df.groupBy("channel_title", "region").agg(
        F.countDistinct("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.round(F.avg("views"), 2).alias("avg_views_per_video"),
        F.round(F.avg("engagement_rate"), 4).alias("avg_engagement_rate"),
        F.max("views").alias("peak_views"),
        F.count("trending_date_parsed").alias("times_trending"),
        F.min("trending_date_parsed").alias("first_trending"),
        F.max("trending_date_parsed").alias("last_trending"),
        F.collect_set("category_name").alias("categories"),
    )

    # Rank channels by total views within each region
    window_rank = Window.partitionBy("region").orderBy(F.col("total_views").desc())
    channel = channel \
        .withColumn("rank_in_region", F.row_number().over(window_rank)) \
        .withColumn("_aggregated_at", F.current_timestamp())

    write_gold_table(
        df=channel,
        table_name="channel_analytics",
        path=f"s3://{GOLD_BUCKET}/youtube/channel_analytics/",
        partition_keys=["region"],
    )


    # ── Gold Table 3: Category Analytics ─────────────────────────────────────
    logger.info("Building Gold table: category_analytics...")

    category = stats_df.groupBy(
        "category_name", "category_id", "region", "trending_date_parsed"
    ).agg(
        F.count("video_id").alias("video_count"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.round(F.avg("engagement_rate"), 4).alias("avg_engagement_rate"),
        F.countDistinct("channel_title").alias("unique_channels"),
    )

    # Category share of total views per region per day
    window_total = Window.partitionBy("region", "trending_date_parsed")
    category = category \
        .withColumn(
            "view_share_pct",
            F.round(
                F.col("total_views") / F.sum("total_views").over(window_total) * 100, 2
            )
        ) \
        .withColumn("_aggregated_at", F.current_timestamp())

    write_gold_table(
        df=category,
        table_name="category_analytics",
        path=f"s3://{GOLD_BUCKET}/youtube/category_analytics/",
        partition_keys=["region"],
    )

    # Release cache
    stats_df.unpersist()

    logger.info("Gold layer build complete. All 3 tables written successfully.")
    job.commit()


except Exception as e:
    error_msg = (
        f"Glue Job FAILED: {args['JOB_NAME']}\n"
        f"Silver DB  : {SILVER_DB}\n"
        f"Gold DB    : {GOLD_DB}\n"
        f"Gold Bucket: {GOLD_BUCKET}\n"
        f"Error      : {str(e)}"
    )
    logger.error(error_msg)
    send_sns_alert(
        subject=f"[ALERT] Glue Job Failed: {args['JOB_NAME']}",
        message=error_msg,
    )
    raise
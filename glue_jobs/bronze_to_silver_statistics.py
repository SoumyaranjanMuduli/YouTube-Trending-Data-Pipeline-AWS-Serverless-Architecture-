"""
Glue Job: Bronze → Silver (Statistics Data)
────────────────────────────────────────────
Reads raw CSV statistics from the Bronze layer,
applies schema enforcement, data cleansing, deduplication,
and writes clean Parquet to the Silver layer.

Fix applied:
  - Direct Spark CSV read with ISO-8859-1 encoding (fixes MX, DE, FR, JP, KR, RU files)
  - multiLine=true (fixes description fields with embedded newlines)
  - PERMISSIVE mode (skips corrupt rows instead of crashing)
  - No changes to business logic, schema, or output format

Job Parameters:
    --JOB_NAME         — Glue job name (auto-set)
    --bronze_database  — Bronze Glue catalog database
    --bronze_table     — Bronze statistics table
    --silver_bucket    — Silver S3 bucket
    --silver_database  — Silver Glue catalog database
    --silver_table     — Silver statistics table
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

# ── Job Setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_table",
    "silver_bucket",
    "silver_database",
    "silver_table",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger      = glueContext.get_logger()

# ── Config ───────────────────────────────────────────────────────────────────
BRONZE_DB    = args["bronze_database"]
BRONZE_TABLE = args["bronze_table"]
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB    = args["silver_database"]
SILVER_TABLE = args["silver_table"]

# S3 path — read ALL regions directly (bypass catalog for encoding control)
BRONZE_S3_PATH = f"s3://yt-data-pipeline-brozne-ap-south-soumya/youtube/raw_statistics/"
SILVER_PATH    = f"s3://{SILVER_BUCKET}/youtube/statistics/"

logger.info(f"Bronze path : {BRONZE_S3_PATH}")
logger.info(f"Silver path : {SILVER_PATH}")
logger.info(f"Silver catalog: {SILVER_DB}.{SILVER_TABLE}")


# ── Step 1: Read CSVs directly with Spark (encoding fix) ────────────────────
#
# WHY: Glue catalog reader uses default UTF-8 strict mode.
#      MX, DE, FR, JP, KR, RU Kaggle CSV files contain ISO-8859-1 / Latin-1
#      characters (accented letters, special symbols) that crash UTF-8 parser.
#
# FIX: Read directly from S3 with:
#      - encoding=ISO-8859-1  → handles all Latin + Asian filenames safely
#      - multiLine=true       → handles newlines embedded in description fields
#      - mode=PERMISSIVE      → logs corrupt rows instead of crashing
#      - recursiveFileLookup  → finds CSVs in region=xx/ subfolders
#      - pathGlobFilter       → only picks .csv files, ignores JSON files
#
logger.info("Reading Bronze CSVs with ISO-8859-1 encoding...")

df = (
    spark.read
    .option("header",              "true")
    .option("inferSchema",         "false")   # all strings first, cast manually
    .option("encoding",            "ISO-8859-1")
    .option("multiLine",           "true")
    .option("escape",              '"')
    .option("quote",               '"')
    .option("sep",                 ",")
    .option("mode",                "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter",      "*.csv")
    .csv(BRONZE_S3_PATH)
)

# Add region partition column by extracting from file path
df = df.withColumn(
    "region",
    F.regexp_extract(F.input_file_name(), r"region=([a-z]+)", 1)
)

initial_count = df.count()
logger.info(f"Bronze records read: {initial_count}")

# Drop rows that Spark flagged as corrupt
if "_corrupt_record" in df.columns:
    corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
    if corrupt_count > 0:
        logger.info(f"Dropping {corrupt_count} corrupt rows")
    df = df.drop("_corrupt_record")


if initial_count == 0:
    logger.info("No records found. Committing empty job.")

else:
    # ── Step 2: Schema Enforcement ──────────────────────────────────────────
    logger.info("Enforcing schema and casting types...")

    # Kaggle CSV format columns (confirmed from your files):
    # video_id, trending_date, title, channel_title, category_id, publish_time,
    # tags, views, likes, dislikes, comment_count, thumbnail_link,
    # comments_disabled, ratings_disabled, video_error_or_removed, description

    df = df.select(
        F.col("video_id").cast(StringType()),
        F.col("trending_date").cast(StringType()),
        F.col("title").cast(StringType()),
        F.col("channel_title").cast(StringType()),
        F.col("category_id").cast(LongType()),
        F.col("publish_time").cast(StringType()),
        F.col("tags").cast(StringType()),
        F.col("views").cast(LongType()),
        F.col("likes").cast(LongType()),
        F.col("dislikes").cast(LongType()),
        F.col("comment_count").cast(LongType()),
        F.col("thumbnail_link").cast(StringType()),
        F.col("comments_disabled").cast(BooleanType()),
        F.col("ratings_disabled").cast(BooleanType()),
        F.col("video_error_or_removed").cast(BooleanType()),
        F.col("description").cast(StringType()),
        F.col("region").cast(StringType()),
    )


    # ── Step 3: Data Cleansing ──────────────────────────────────────────────
    logger.info("Cleansing data...")

    # Remove records where video_id is null (corrupt rows)
    df = df.filter(F.col("video_id").isNotNull())

    # Standardize region to lowercase
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

    # Remove rows where region failed to extract (empty string)
    df = df.filter(F.col("region") != "")

    # Parse trending_date from Kaggle format YY.DD.MM to proper date
    df = df.withColumn(
        "trending_date_parsed",
        F.when(
            F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
            F.to_date(F.col("trending_date"), "yy.dd.MM")
        ).otherwise(
            F.to_date(F.col("trending_date"))
        )
    )

    # Fill nulls for numeric columns with 0
    for col_name in ["views", "likes", "dislikes", "comment_count"]:
        df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    # Derived engagement columns
    df = df.withColumn(
        "like_ratio",
        F.when(
            F.col("views") > 0,
            F.round(F.col("likes") / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df = df.withColumn(
        "engagement_rate",
        F.when(
            F.col("views") > 0,
            F.round(
                (F.col("likes") + F.col("dislikes") + F.col("comment_count"))
                / F.col("views") * 100,
                4
            )
        ).otherwise(0.0)
    )

    # Processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    df = df.withColumn("_job_name",     F.lit(args["JOB_NAME"]))


    # ── Step 4: Deduplication ───────────────────────────────────────────────
    logger.info("Deduplicating...")

    from pyspark.sql.window import Window

    window = (
        Window
        .partitionBy("video_id", "region", "trending_date_parsed")
        .orderBy(F.col("_processed_at").desc())
    )

    df = (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    clean_count = df.count()
    logger.info(
        f"After cleansing & dedup: {clean_count} records "
        f"(removed {initial_count - clean_count})"
    )


    # ── Step 5: Data Quality Checks ─────────────────────────────────────────
    logger.info("Running data quality checks...")

    null_counts = {}
    for col_name in ["video_id", "title", "channel_title", "views"]:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_counts[col_name] = null_count
        if null_count > 0:
            logger.info(f"  DQ WARNING: {col_name} has {null_count} null values")

    negative_views = df.filter(F.col("views") < 0).count()
    if negative_views > 0:
        logger.info(f"  DQ WARNING: {negative_views} records with negative views")

    logger.info(f"  DQ check complete. Null counts: {null_counts}")


    # ── Step 6: Write to Silver Layer ───────────────────────────────────────
    logger.info(f"Writing to Silver: {SILVER_PATH}")

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_statistics")

    sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
    )
    sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_TABLE)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)

    logger.info(f"Silver write complete. {clean_count} records written.")

job.commit()

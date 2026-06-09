# 🎬 YouTube Trending Data Pipeline — AWS Serverless Architecture

> An end-to-end serverless data pipeline that ingests live YouTube trending data across 10 global regions, processes it through a **Bronze → Silver → Gold** medallion architecture, runs automated data quality checks, and makes it queryable via AWS Athena — fully orchestrated by AWS Step Functions.

---

## 📌 Project Overview

This pipeline automatically fetches YouTube trending videos every 6 hours using the YouTube Data API, stores and transforms the data through three S3 layers, validates data quality before promotion, and produces three analytics-ready Gold tables for business reporting.

**Built on:** AWS Lambda · AWS Glue · Step Functions · S3 · Athena · EventBridge · SNS · IAM · CloudWatch  
**Language:** Python 3.12  
**Storage Format:** CSV/JSON (Bronze) → Parquet/Snappy (Silver & Gold)  
**Region:** ap-south-1 (Mumbai)

---

## 🏗️ Architecture Diagram

```
                        ┌─────────────────────────────────────────────────────┐
                        │              AWS Step Functions Orchestration         │
                        └─────────────────────────────────────────────────────┘
                                              │
                    ┌─────────────────────────▼──────────────────────────┐
                    │  EventBridge (every 6 hrs)                          │
                    │         │                                           │
                    │         ▼                                           │
                    │  Lambda 3 — YouTube API Ingestion                  │
                    │    ├── 10 regions × 50 trending videos              │
                    │    └── Writes JSON → S3 Bronze (Hive partitioned)  │
                    │         │                                           │
                    │         ▼                                           │
                    │  ┌──────────────────────┐                          │
                    │  │   Parallel Execution  │                          │
                    │  │  ┌────────────────┐  │                          │
                    │  │  │ Lambda 1       │  │                          │
                    │  │  │ JSON→Parquet   │  │                          │
                    │  │  │ (Reference)    │  │                          │
                    │  │  └────────────────┘  │                          │
                    │  │  ┌────────────────┐  │                          │
                    │  │  │ Glue Job 1     │  │                          │
                    │  │  │ Bronze→Silver  │  │                          │
                    │  │  │ (Statistics)   │  │                          │
                    │  │  └────────────────┘  │                          │
                    │  └──────────────────────┘                          │
                    │         │                                           │
                    │         ▼                                           │
                    │  Lambda 2 — Data Quality (5 checks)                │
                    │    ├── quality_passed = true  → continue           │
                    │    └── quality_passed = false → SNS alert + STOP   │
                    │         │                                           │
                    │         ▼                                           │
                    │  Glue Job 2 — Silver → Gold                        │
                    │    └── 3 Gold analytics tables                     │
                    │         │                                           │
                    │         ▼                                           │
                    │  Athena — Query Gold Layer                         │
                    │  SNS — Success/Failure Email Alert                 │
                    └────────────────────────────────────────────────────┘
```

---

## 🗂️ Medallion Architecture — S3 Layer Design

| Layer | S3 Bucket | Format | Purpose |
|-------|-----------|--------|---------|
| 🥉 Bronze | `yt-data-pipeline-brozne-ap-south-soumya` | CSV / JSON | Raw ingested data — never modified |
| 🥈 Silver | `yt-data-pipeline-silver-ap-south-soumya` | Parquet (Snappy) | Cleansed, typed, deduplicated |
| 🥇 Gold | `yt-data-pipeline-gold-ap-south-soumya` | Parquet (Snappy) | Business aggregations for analytics |
| 📜 Scripts | `yt-data-pipeline-script-ap-south-soumya` | Python `.py` | Glue job scripts |
| 🔍 Athena | `yt-data-pipeline-glue-athena-queryresult-soumya` | CSV | Athena query results |

---

## ⚙️ AWS Services & Resources

### Lambda Functions

| # | Function Name | Purpose |
|---|---------------|---------|
| Lambda 3 | `yt-data-pipline-youtube-integration-dev` | Fetch live data from YouTube API for 10 regions |
| Lambda 1 | `yt-pipeline-json-to-parquet-dev` | Convert JSON reference data → Parquet |
| Lambda 2 | `yt-pipeline-data-quality-dev` | Run 5 automated data quality checks via Athena |

### Glue ETL Jobs

| Job | Name | What It Does |
|-----|------|--------------|
| Job 1 | `yt-pipeline-bronze-to-silver-dev` | Cleanse & type raw CSV statistics → Silver Parquet |
| Job 2 | `yt-pipeline-silver-to-gold-dev` | Aggregate Silver → 3 Gold analytics tables |

### Other Services

| Service | Resource Name | Purpose |
|---------|---------------|---------|
| Step Functions | `yt-pipeline-orchestration-dev` | Orchestrate all pipeline steps in order |
| EventBridge | `yt-pipeline-ingestion-schedule` | Trigger Lambda 3 every 6 hours automatically |
| SNS | `yt-pipeline-alerts-dev` | Email alerts on pipeline success or failure |
| Athena DB | `yt_pipeline_gold_dev` | SQL query layer on Gold tables |

---

## 🔄 Pipeline Execution Flow (Step Functions)

```
Step 1 → IngestFromYouTubeAPI       Lambda 3 fetches trending data for all 10 regions
Step 2 → WaitForS3Consistency       10-second wait for S3 propagation
Step 3 → ProcessInParallel          Two branches run simultaneously:
            Branch A → Lambda 1     JSON reference data → Parquet
            Branch B → Glue Job 1   Bronze CSV statistics → Silver Parquet
Step 4 → RunDataQualityChecks       Lambda 2 runs 5 DQ checks, returns quality_passed bool
Step 5 → EvaluateDataQuality        quality_passed = true → continue | false → SNS alert + STOP
Step 6 → RunSilverToGoldGlueJob     Glue Job 2 creates 3 Gold analytics tables
Step 7 → NotifySuccess              SNS email: "Pipeline completed successfully"
```

---

## ✅ Data Quality Checks (Lambda 2)

| Check | Validates | Passes When |
|-------|-----------|-------------|
| 1. Row Count | Enough data exists | Table has at least 10 rows |
| 2. Null % | Critical columns populated | `video_id`, `title`, `views`, `region` null % under 5% |
| 3. Schema | Expected columns present | All 5 required columns exist |
| 4. Value Range | Numeric values are sane | No negative views, no views over 50 billion |
| 5. Freshness | Data is recent | Latest `_processed_at` within last 48 hours |

> If any check fails → `quality_passed: false` → Step Functions stops pipeline and sends SNS failure alert. **Gold layer only receives clean, validated data.**

---

## 🥇 Gold Layer Analytics Tables

| Table | Contains |
|-------|----------|
| `trending_analytics` | Daily trending summaries per region — top videos, avg views, avg engagement |
| `channel_analytics` | Per-channel metrics — total trending appearances, avg views, dominant category |
| `category_analytics` | Per-category trends over time — which categories dominate per region |

---

## 📊 Sample Athena Queries

```sql
-- Top trending videos in Canada
SELECT * FROM trending_analytics
WHERE region = 'ca'
ORDER BY avg_views DESC
LIMIT 10;

-- Top channels by total views globally
SELECT channel_title, SUM(views) AS total_views
FROM trending_analytics
GROUP BY channel_title
ORDER BY total_views DESC
LIMIT 20;

-- Most trending categories per region
SELECT category_name, region, COUNT(*) AS trending_count
FROM category_analytics
GROUP BY category_name, region
ORDER BY trending_count DESC;
```

---

## 📁 Repository Structure

```
youtube-aws-pipeline/
│
├── lambda/
│   ├── yt_pipeline_youtube_api_lambda.py   # Lambda 3 — YouTube API ingestion
│   ├── JsonToParquet.py                    # Lambda 1 — JSON to Parquet conversion
│   └── dq_lambda.py                        # Lambda 2 — Data quality checks
│
├── glue/
│   ├── bronze_to_silver_statistics.py      # Glue Job 1 — Bronze → Silver
│   └── SilverToGoldAnalysis.py             # Glue Job 2 — Silver → Gold
│
├── step_functions/
│   └── pipeline_orchestration.json         # Step Functions state machine definition
│
├── iam/
│   ├── glue_role_policy.json               # IAM policy for Glue role
│   ├── lambda_role_policy.json             # IAM policy for Lambda role
│   └── sfn_role_policy.json                # IAM policy for Step Functions role
│
├── screenshots/
│   ├── step_functions_execution.png        # Step Functions execution graph
│   ├── glue_job_runs.png                   # Glue job run history
│   ├── athena_query_results.png            # Athena Gold layer query output
│   ├── s3_bucket_structure.png             # S3 medallion layer structure
│   └── lambda_functions.png               # Lambda console overview
│
├── architecture_diagram.png                # Full pipeline architecture diagram
└── README.md
```

---

## 🚀 Setup Guide (Quick Reference)

> Full step-by-step setup with error analysis and corrected code is documented separately.

**High-level setup order:**
1. Create S3 buckets (Bronze, Silver, Gold, Scripts, Athena)
2. Create IAM roles — Glue, Lambda, Step Functions
3. Create SNS topic and confirm email subscription
4. Create Glue databases (use underscores, not hyphens)
5. Run Glue Crawlers on Bronze bucket
6. Deploy 3 Lambda functions with environment variables
7. Upload Glue scripts to S3, create 2 Glue ETL jobs
8. Create Step Functions state machine from JSON
9. Set EventBridge schedule (every 6 hours)
10. Query Gold tables via Athena

> ⚠️ **Common pitfall:** Glue database names must use underscores (`yt_pipeline_bronze_dev`), not hyphens. Hyphens cause `EntityNotFoundException` in Glue ETL jobs.

---

## 🌍 Regions Covered

`US` `GB` `CA` `DE` `FR` `IN` `JP` `KR` `MX` `RU`

---

## 🛠️ Tech Stack

![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20Glue%20%7C%20S3%20%7C%20Athena%20%7C%20Step%20Functions-orange?logo=amazonaws)
![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.3-red?logo=apachespark)
![Parquet](https://img.shields.io/badge/Storage-Parquet%20%2F%20Snappy-green)

---

## 👤 Author

**Soumya Ranjan Muduli**  
[LinkedIn](https://linkedin.com/in/soumyaranjan2003) · [GitHub](https://github.com/SoumyaranjanMuduli) · [Portfolio](https://soumya-ranjan-nu.vercel.app)

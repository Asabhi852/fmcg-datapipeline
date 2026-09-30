🚀 End-to-End Data Engineering Project: FMCG Domain (Atlon & Sports Bar Integration)

📌 Business Scenario & Problem Statement

Imagine Atlon, a mature, global sports equipment manufacturer operating with predictable ERP systems and a well-established data architecture (Bronze, Silver, and Gold layers on Databricks).

Recently, Atlon acquired Sports Bar, a fast-growing startup in the energy bars and athletic nutrition space. While Sports Bar experienced rapid expansion, its data management was chaotic—spread across scattered spreadsheets, cloud drives, WhatsApp exports, and ad-hoc API logs.

Post-acquisition, unifying supply chain forecasting, inventory planning, and financial reporting resulted in data discrepancies, conflicting sales metrics, and missing records. This project builds a robust, scalable data engineering pipeline to ingest, clean, transform, and unify Sports Bar's fragmented data into Atlon’s existing data model, empowering leadership with unified analytics.

🏗️ Architecture & Tech Stack

Cloud Storage: AWS S3 (sportsbar-dp) serving as the cloud landing zone.

Processing & Computation: Databricks Free Edition, Apache Spark, PySpark, SQL.

Storage Format & Architecture: Delta Lake implementing the Medallion Architecture (Bronze ➔ Silver ➔ Gold layers).

Orchestration: Databricks Workflows (Cron-scheduled tasks with email alerting).

Serving & BI: Databricks Dashboards, Databricks Genie (Natural Language AI Querying), and Denormalized Analytical Views.

📊 Data Model (Star Schema)

The project unifies data into a centralized star schema structure:

dim_customers: Customer metadata (Code, Name, Market, Platform, Channel, City).

dim_products: Product hierarchy (ID, Division, Category, Product Details, Variant extraction).

gross_price: Yearly pricing information mapped using window functions and historical ranking.

fact_orders: Granular monthly and daily transactional data detailing orders, quantities, and revenue.

dim_date: Programmatically generated calendar dimension table (Jan 1, 2024 – Dec 31, 2025).

⚙️ Step-by-Step Implementation Workflow

[AWS S3 Landing Zone] ➔ (Bronze Ingestion & Metadata) ➔ (Silver Cleaning & Transformation) ➔ (Gold Layer Upserts / MERGE) ➔ [Unified BI Dashboards & Genie AI]



Environment Setup: Configured a unified FMCG catalog in Databricks with dedicated schemas for bronze, silver, and gold.

AWS S3 Integration: Connected external cloud storage buckets to ingest full historical dumps (5-month backfill from July to Nov) and incremental daily drops (starting Dec 1).

Data Transformation & Cleansing (Silver Layer):

Customers: Standardized city names, trimmed trailing/leading spaces, handled null values with business rules, and concatenated city names to eliminate duplicate identifiers.

Products: Fixed spelling errors, extracted product weights/variants via Regex, and generated secure surrogate keys using sha() hashing.

Gross Pricing: Normalized non-uniform dates using try_to_date() and coalesce(), handled negative/null prices, and applied Window functions (ROW_NUMBER) to capture correct yearly pricing.

Orders: Processed historical full loads and staging-based daily incremental loads, archiving files post-ingestion via dbutils.fs.mv().

Gold Layer Integration: Performed idempotent MERGE INTO (Upsert) operations to synchronize child startup records seamlessly into the parent Atlon Gold tables.

Workflow Orchestration: Scheduled end-to-end dependency-managed pipeline jobs with automated failure notifications.

Serving & Analytics: Built interactive executive dashboards (Atlon BI 360) with drill-down filters and integrated Databricks Genie for conversational natural language data queries.

📂 Repository Structure

├── notebooks/
│   ├── 01_bronze_ingestion.py        # Ingestion scripts from S3 to Bronze Delta tables
│   ├── 02_silver_transformation.py   # Data cleansing, regex extraction, and standardizations
│   ├── 03_gold_upsert_model.py       # MERGE operations into Star Schema dimensions & facts
│   └── 04_incremental_pipeline.py    # Daily incremental load handling & file archiving
├── sql/
│   ├── create_catalog_schemas.sql    # Catalog and medallion schema DDLs
│   └── analytical_views.sql          # Denormalized views for BI and reporting
├── workflows/
│   └── databricks_job_config.json    # Workflow configuration and dependency mapping
└── README.md



🚀 Key Learnings & Engineering Highlights

Idempotent Pipelines: Implemented Delta Lake MERGE statements ensuring safe, repeatable runs without data duplication.

Handling Data Drift & Quality Issues: Resolved messy startup data issues including typos, mixed date formats, missing fields, and unstandardized categories.

Cost-Efficient Engineering: Fully developed and tested utilizing Databricks Free Edition and AWS free-tier cloud primitives.

👤 Author

Built with 💡 and ☕ as part of an end-to-end data engineering portfolio project. Feel free to star ⭐ this repo if you find it helpful!

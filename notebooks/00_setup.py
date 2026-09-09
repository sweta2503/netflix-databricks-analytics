# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Install dependencies and create the medallion database schemas.
# MAGIC Run this once before anything else.

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %pip install groq==0.13.0 dbt-databricks==1.9.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Cell 4
# Create catalog and medallion databases (Unity Catalog 3-part naming)
spark.sql("CREATE CATALOG IF NOT EXISTS netflix")
print("✅ netflix catalog")

for db in ["netflix_bronze", "netflix_silver", "netflix_gold", "netflix_ai"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS netflix.{db}")
    print(f"✅ netflix.{db}")

# COMMAND ----------

# DBTITLE 1,Cell 5
# Shared path constants — imported by every notebook
VOLUME_ROOT    = "/Volumes/netflix/default/aidataset/"
RAW_CSV        = f"{VOLUME_ROOT}netflix_titles.csv"

BRONZE_TABLE   = "netflix.netflix_bronze.raw_titles"
SILVER_DB      = "netflix.netflix_silver"
GOLD_DB        = "netflix.netflix_gold"
AI_DB          = "netflix.netflix_ai"

print("📁 Path constants ready")
print(f"   CSV path : {RAW_CSV}")
print()
print("Next steps:")
print("  1. Confirm netflix_titles.csv is in your Volume at the path above")
print("  2. Run 01_bronze_ingestion")
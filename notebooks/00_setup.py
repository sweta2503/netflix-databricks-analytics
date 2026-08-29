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

# MAGIC %pip install anthropic==0.40.0 dbt-databricks==1.9.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Medallion databases
for db in ["netflix_bronze", "netflix_silver", "netflix_gold", "netflix_ai"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"✅ {db}")

# COMMAND ----------

# Shared path constants — imported by every notebook
VOLUME_ROOT    = "/Volumes/netflix/default/aidataset/"
RAW_CSV        = f"{VOLUME_ROOT}netflix_titles.csv"

BRONZE_TABLE   = "netflix_bronze.raw_titles"
SILVER_DB      = "netflix_silver"
GOLD_DB        = "netflix_gold"
AI_DB          = "netflix_ai"

print("📁 Path constants ready")
print(f"   CSV path : {RAW_CSV}")
print()
print("Next steps:")
print("  1. Confirm netflix_titles.csv is in your Volume at the path above")
print("  2. Run 01_bronze_ingestion")

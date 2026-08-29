# Databricks notebook source
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
DBFS_ROOT      = "dbfs:/FileStore/netflix"
RAW_CSV        = f"{DBFS_ROOT}/netflix_titles.csv"

BRONZE_TABLE   = "netflix_bronze.raw_titles"
SILVER_DB      = "netflix_silver"
GOLD_DB        = "netflix_gold"
AI_DB          = "netflix_ai"

print("📁 Path constants ready")
print(f"   Upload your CSV to: {RAW_CSV}")
print()
print("Next steps:")
print("  1. Upload netflix_titles.csv → Data > Add Data > DBFS > FileStore/netflix/")
print("  2. Run 01_bronze_ingestion")

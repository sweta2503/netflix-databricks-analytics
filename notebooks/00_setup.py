# Databricks notebook source
# MAGIC %md
# MAGIC # Netflix Analytics — Setup
# MAGIC Run this notebook first. It creates the database and installs dependencies.

# COMMAND ----------

# MAGIC %pip install anthropic==0.40.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Create database schemas for medallion architecture
spark.sql("CREATE DATABASE IF NOT EXISTS netflix")
spark.sql("CREATE DATABASE IF NOT EXISTS netflix_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS netflix_gold")
spark.sql("CREATE DATABASE IF NOT EXISTS netflix_ai")

print("✅ Databases created: netflix (bronze), netflix_silver, netflix_gold, netflix_ai")

# COMMAND ----------

# Define DBFS paths used across all notebooks
BRONZE_PATH = "dbfs:/user/hive/warehouse/netflix_bronze"
SILVER_PATH  = "dbfs:/user/hive/warehouse/netflix_silver"
GOLD_PATH    = "dbfs:/user/hive/warehouse/netflix_gold"
AI_PATH      = "dbfs:/user/hive/warehouse/netflix_ai"
RAW_CSV_PATH = "dbfs:/FileStore/netflix/netflix_titles.csv"

print("📁 Paths configured")
print(f"  Raw CSV  : {RAW_CSV_PATH}")
print(f"  Bronze   : {BRONZE_PATH}")
print(f"  Silver   : {SILVER_PATH}")
print(f"  Gold     : {GOLD_PATH}")
print(f"  AI       : {AI_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC Upload `netflix_titles.csv` from Kaggle to DBFS:
# MAGIC 1. Download from https://www.kaggle.com/datasets/shivamb/netflix-shows
# MAGIC 2. In Databricks: **Data → Add Data → DBFS → FileStore/netflix/**
# MAGIC 3. Then run notebook **01_bronze_ingestion**

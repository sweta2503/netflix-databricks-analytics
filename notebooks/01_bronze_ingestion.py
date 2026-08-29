# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Raw Ingestion
# MAGIC Reads raw Netflix CSV from DBFS. Writes as-is to a Delta table.
# MAGIC Bronze = source of truth. Zero transformations.

# COMMAND ----------

RAW_CSV      = "/Volumes/netflix/default/aidataset/netflix_titles.csv"
BRONZE_TABLE = "netflix_bronze.raw_titles"

# COMMAND ----------

# MAGIC %md ## Ingest

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .csv(RAW_CSV)
)

row_count = df_raw.count()
print(f"📥 Raw rows  : {row_count:,}")
print(f"📋 Columns   : {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

display(df_raw.limit(5))

# COMMAND ----------

# MAGIC %md ## Write Bronze Delta table

# COMMAND ----------

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
print(f"✅ Bronze table: {BRONZE_TABLE}  ({row_count:,} rows)")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

from pyspark.sql.functions import col, isnull

df_b = spark.table(BRONZE_TABLE)
total = df_b.count()

print(f"{'Column':<22} {'Nulls':>7} {'%':>6}")
print("-" * 38)
for c in df_b.columns:
    nulls = df_b.filter(isnull(col(c)) | (col(c) == "")).count()
    pct   = nulls / total * 100
    flag  = "⚠️ " if pct > 10 else "   "
    print(f"{flag}{c:<20} {nulls:>7,} {pct:>5.1f}%")

# COMMAND ----------

# MAGIC %md ## Delta time travel — already available

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY netflix_bronze.raw_titles;

# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 1 — Bronze (Raw Ingestion)
# MAGIC Reads the raw Netflix CSV from DBFS and writes it as a Delta table.
# MAGIC No transformations — bronze is the source of truth.

# COMMAND ----------

RAW_CSV_PATH = "dbfs:/FileStore/netflix/netflix_titles.csv"
BRONZE_TABLE = "netflix.raw_titles"
BRONZE_PATH  = "dbfs:/user/hive/warehouse/netflix_bronze/raw_titles"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw CSV

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")   # some descriptions span multiple lines
    .option("escape", '"')
    .csv(RAW_CSV_PATH)
)

print(f"📥 Rows loaded: {df_raw.count():,}")
print(f"📋 Columns   : {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview raw data

# COMMAND ----------

display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta table

# COMMAND ----------

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Bronze table written: {BRONZE_TABLE}")
print(f"   Rows: {spark.table(BRONZE_TABLE).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze data quality check

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan, isnull

df_bronze = spark.table(BRONZE_TABLE)
total = df_bronze.count()

print("=== NULL / MISSING VALUE REPORT ===")
for column in df_bronze.columns:
    null_count = df_bronze.filter(isnull(col(column)) | (col(column) == "")).count()
    pct = round(null_count / total * 100, 1)
    flag = "⚠️ " if pct > 10 else "✅ "
    print(f"{flag} {column:<20} nulls: {null_count:>5,}  ({pct}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta table history (time travel available from first write)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY netflix.raw_titles

# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 2 — Silver (Clean & Normalize)
# MAGIC Reads Bronze Delta table. Cleans, types, and normalizes into structured Silver tables.
# MAGIC Rules: no business logic here — only data quality fixes.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType

BRONZE_TABLE = "netflix.raw_titles"
SILVER_TABLE  = "netflix_silver.titles"

# COMMAND ----------

df = spark.table(BRONZE_TABLE)
print(f"📥 Bronze rows: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

df_silver = (
    df
    # ── Cast types ───────────────────────────────────────────────────────────
    .withColumn("release_year",  col("release_year").cast(IntegerType()))
    .withColumn("date_added",
        F.to_date(F.trim(col("date_added")), "MMMM d, yyyy")
    )

    # ── Parse duration into a numeric + unit ─────────────────────────────────
    .withColumn("duration_value",
        F.regexp_extract(col("duration"), r"(\d+)", 1).cast(IntegerType())
    )
    .withColumn("duration_unit",
        F.when(col("duration").contains("Season"), "Seasons")
         .when(col("duration").contains("min"), "Minutes")
         .otherwise(None)
    )

    # ── Genres: comma-separated string → array ───────────────────────────────
    .withColumn("genres", F.split(F.trim(col("listed_in")), r",\s*"))

    # ── Primary country (first in comma-separated list) ──────────────────────
    .withColumn("primary_country",
        F.trim(F.split(col("country"), ",").getItem(0))
    )

    # ── Null handling ────────────────────────────────────────────────────────
    .withColumn("director",
        F.when(col("director") == "", None).otherwise(col("director"))
    )
    .withColumn("cast",
        F.when(col("cast") == "", None).otherwise(col("cast"))
    )
    .withColumn("country",
        F.when(col("country") == "", None).otherwise(col("country"))
    )
    .withColumn("primary_country",
        F.when(col("primary_country") == "", None).otherwise(col("primary_country"))
    )
    .withColumn("rating",
        F.when(col("rating") == "", None).otherwise(col("rating"))
    )

    # ── Derived flags ─────────────────────────────────────────────────────────
    .withColumn("is_movie",  F.col("type") == "Movie")
    .withColumn("added_year", F.year(col("date_added")))

    # ── Select and rename for clean schema ────────────────────────────────────
    .select(
        col("show_id"),
        col("type"),
        col("title"),
        col("director"),
        col("cast"),
        col("country"),
        col("primary_country"),
        col("date_added"),
        col("added_year"),
        col("release_year"),
        col("rating"),
        col("duration"),
        col("duration_value"),
        col("duration_unit"),
        col("listed_in").alias("genres_raw"),
        col("genres"),
        col("description"),
        col("is_movie"),
    )
    .dropDuplicates(["show_id"])
    .filter(col("title").isNotNull())
)

print(f"✅ Silver rows: {df_silver.count():,}")
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview cleaned data

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Delta table

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"✅ Silver table written: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

df_check = spark.table(SILVER_TABLE)

movies   = df_check.filter(col("type") == "Movie").count()
shows    = df_check.filter(col("type") == "TV Show").count()
no_date  = df_check.filter(col("date_added").isNull()).count()
no_country = df_check.filter(col("primary_country").isNull()).count()

print("=== SILVER VALIDATION ===")
print(f"  Movies        : {movies:,}")
print(f"  TV Shows      : {shows:,}")
print(f"  Missing date  : {no_date:,}")
print(f"  Missing country: {no_country:,}")
print("✅ Silver layer ready")

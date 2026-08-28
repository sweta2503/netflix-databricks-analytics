# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 3 — Gold (Business Analytics Tables)
# MAGIC Reads Silver. Produces 6 Gold tables — one per business question.
# MAGIC These are what the AI layer and dashboard read.

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TABLE = "netflix_silver.titles"

df = spark.table(SILVER_TABLE)
print(f"📥 Silver rows: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1 — Content type split by year

# COMMAND ----------

gold_type_by_year = (
    df
    .filter(F.col("added_year").isNotNull())
    .groupBy("added_year", "type")
    .agg(F.count("*").alias("titles_count"))
    .orderBy("added_year", "type")
)

gold_type_by_year.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("netflix_gold.type_by_year")
print("✅ gold.type_by_year")
display(gold_type_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2 — Top countries by content volume

# COMMAND ----------

gold_top_countries = (
    df
    .filter(F.col("primary_country").isNotNull())
    .groupBy("primary_country")
    .agg(
        F.count("*").alias("total_titles"),
        F.sum(F.col("is_movie").cast("int")).alias("movies"),
        (F.count("*") - F.sum(F.col("is_movie").cast("int"))).alias("tv_shows"),
    )
    .orderBy(F.col("total_titles").desc())
    .limit(20)
)

gold_top_countries.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("netflix_gold.top_countries")
print("✅ gold.top_countries")
display(gold_top_countries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3 — Genre distribution (exploded)

# COMMAND ----------

gold_genres = (
    df
    .select(F.explode(F.col("genres")).alias("genre"), "type")
    .withColumn("genre", F.trim(F.col("genre")))
    .filter(F.col("genre") != "")
    .groupBy("genre", "type")
    .agg(F.count("*").alias("count"))
    .orderBy(F.col("count").desc())
)

gold_genres.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("netflix_gold.genre_distribution")
print("✅ gold.genre_distribution")
display(gold_genres.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 4 — Rating distribution

# COMMAND ----------

gold_ratings = (
    df
    .filter(F.col("rating").isNotNull())
    .groupBy("rating", "type")
    .agg(F.count("*").alias("count"))
    .orderBy(F.col("count").desc())
)

gold_ratings.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("netflix_gold.rating_distribution")
print("✅ gold.rating_distribution")
display(gold_ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 5 — Top directors (movies only)

# COMMAND ----------

gold_directors = (
    df
    .filter(F.col("director").isNotNull() & (F.col("type") == "Movie"))
    .groupBy("director")
    .agg(F.count("*").alias("movie_count"))
    .orderBy(F.col("movie_count").desc())
    .limit(15)
)

gold_directors.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("netflix_gold.top_directors")
print("✅ gold.top_directors")
display(gold_directors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 6 — International content growth (non-US)

# COMMAND ----------

gold_intl_growth = (
    df
    .filter(F.col("added_year").isNotNull() & F.col("primary_country").isNotNull())
    .withColumn("is_us", F.col("primary_country") == "United States")
    .groupBy("added_year")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("is_us").cast("int")).alias("us_titles"),
        (F.count("*") - F.sum(F.col("is_us").cast("int"))).alias("international_titles"),
    )
    .withColumn("intl_pct", F.round(F.col("international_titles") / F.col("total") * 100, 1))
    .orderBy("added_year")
)

gold_intl_growth.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("netflix_gold.international_growth")
print("✅ gold.international_growth")
display(gold_intl_growth)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary stats (passed to AI layer)

# COMMAND ----------

total        = df.count()
movies       = df.filter(F.col("type") == "Movie").count()
shows        = df.filter(F.col("type") == "TV Show").count()
countries    = df.filter(F.col("primary_country").isNotNull()).select("primary_country").distinct().count()
year_min     = df.agg(F.min("added_year")).collect()[0][0]
year_max     = df.agg(F.max("added_year")).collect()[0][0]

print("=== GOLD SUMMARY ===")
print(f"  Total titles       : {total:,}")
print(f"  Movies             : {movies:,}  ({round(movies/total*100,1)}%)")
print(f"  TV Shows           : {shows:,}  ({round(shows/total*100,1)}%)")
print(f"  Countries covered  : {countries}")
print(f"  Year range added   : {year_min} – {year_max}")
print("\n✅ All 6 Gold tables ready")

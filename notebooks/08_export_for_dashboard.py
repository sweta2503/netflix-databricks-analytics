# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Export Gold + AI tables for Streamlit Dashboard
# MAGIC Saves all Gold and AI tables as CSV to DBFS.
# MAGIC Download them locally, then run: streamlit run streamlit/app.py

# COMMAND ----------

import os

EXPORT_PATH = "dbfs:/FileStore/netflix/exports"
dbutils.fs.mkdirs(EXPORT_PATH)

tables = {
    "content_by_year":       "netflix.netflix_gold.content_by_year",
    "genre_distribution":    "netflix.netflix_gold.genre_distribution",
    "country_analysis":      "netflix.netflix_gold.country_analysis",
    "rating_distribution":   "netflix.netflix_gold.rating_distribution",
    "top_directors":         "netflix.netflix_gold.top_directors",
    "international_growth":  "netflix.netflix_gold.international_growth",
    "enriched_titles":       "netflix.netflix_ai.enriched_titles",
    "strategic_insights":    "netflix.netflix_ai.strategic_insights",
}

for name, table in tables.items():
    out = f"{EXPORT_PATH}/{name}"
    try:
        df = spark.table(table)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
        print(f"✅ Exported {table} → {out}  ({df.count():,} rows)")
    except Exception as e:
        print(f"❌ {table}: {e}")

# COMMAND ----------

print("""
=== DOWNLOAD INSTRUCTIONS ===

In Databricks: Data → DBFS → FileStore/netflix/exports/

For each folder (e.g. content_by_year/):
  - Open the folder
  - Download the file named part-00000-*.csv
  - Rename it to the folder name (e.g. content_by_year.csv)
  - Place it in your local: streamlit/data/

Then run:
  pip install -r requirements.txt
  streamlit run streamlit/app.py
""")

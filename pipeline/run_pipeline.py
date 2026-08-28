# Databricks notebook source
# MAGIC %md
# MAGIC # Master Pipeline — Run All Layers
# MAGIC Runs all 5 notebooks in sequence. Use this as your single entry point.
# MAGIC In Databricks Community Edition, run this notebook to execute the full pipeline.

# COMMAND ----------

import time

def run_notebook(path: str, timeout: int = 600):
    print(f"\n▶  Running: {path}")
    t0 = time.time()
    dbutils.notebook.run(path, timeout_seconds=timeout)
    elapsed = round(time.time() - t0, 1)
    print(f"✅ Done: {path}  ({elapsed}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run pipeline

# COMMAND ----------

start = time.time()

print("=" * 55)
print("  Netflix Analytics Pipeline — Starting")
print("=" * 55)

run_notebook("../notebooks/01_bronze_ingestion")
run_notebook("../notebooks/02_silver_transform")
run_notebook("../notebooks/03_gold_analytics")
run_notebook("../notebooks/04_ai_insights")
run_notebook("../notebooks/05_dashboard")

total = round(time.time() - start, 1)

print("\n" + "=" * 55)
print(f"  Pipeline complete in {total}s")
print("=" * 55)
print("\n  Tables created:")
print("    netflix.raw_titles          (Bronze)")
print("    netflix_silver.titles       (Silver)")
print("    netflix_gold.type_by_year")
print("    netflix_gold.top_countries")
print("    netflix_gold.genre_distribution")
print("    netflix_gold.rating_distribution")
print("    netflix_gold.top_directors")
print("    netflix_gold.international_growth")
print("    netflix_ai.insights         (Claude AI)")

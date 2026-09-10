# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Master Pipeline Orchestrator
# MAGIC Runs the full pipeline end-to-end using dbutils.notebook.run().
# MAGIC This is Databricks-native orchestration — no Airflow needed.
# MAGIC
# MAGIC Pipeline:
# MAGIC   Bronze → dbt Silver → Gold → AI Enrichment → AI Insights
# MAGIC
# MAGIC Note: This pipeline runs the core data transformation and AI enrichment steps.
# MAGIC Notebooks 05 (text-to-SQL) and 06 (prompt-based analysis) are exploratory tools, not part of the production pipeline.
All outputs are written to Unity Catalog Delta tables.

# COMMAND ----------

import time

dbutils.widgets.text("GROQ_API_KEY", "", "Groq API Key")
dbutils.widgets.text("DATABRICKS_HOST",   "", "Databricks Host")
dbutils.widgets.text("DATABRICKS_HTTP_PATH", "", "Databricks HTTP Path")
dbutils.widgets.text("DATABRICKS_TOKEN",  "", "Databricks Token")

API_KEY   = dbutils.widgets.get("GROQ_API_KEY")
DB_HOST   = dbutils.widgets.get("DATABRICKS_HOST")
DB_PATH   = dbutils.widgets.get("DATABRICKS_HTTP_PATH")
DB_TOKEN  = dbutils.widgets.get("DATABRICKS_TOKEN")

# COMMAND ----------

NOTEBOOKS_DIR = "."  # relative to this notebook's location in Repos

pipeline = [
    ("01 Bronze Ingestion",   "01_bronze_ingestion",    {},                                300),
    ("02 dbt Transform",      "02_dbt_transform",       {
        "DATABRICKS_HOST": DB_HOST,
        "DATABRICKS_HTTP_PATH": DB_PATH,
        "DATABRICKS_TOKEN": DB_TOKEN,
    }, 600),
    ("03 Gold Analytics",     "03_gold_analytics",      {
        "DATABRICKS_HOST": DB_HOST,
        "DATABRICKS_HTTP_PATH": DB_PATH,
        "DATABRICKS_TOKEN": DB_TOKEN,
    }, 600),
    ("04 AI Enrichment",      "04_ai_enrichment",       {"GROQ_API_KEY": API_KEY}, 900),
    ("07 AI Insights",        "07_ai_insights",         {"GROQ_API_KEY": API_KEY}, 600),
]

# COMMAND ----------

results = []
total_start = time.time()

for step_name, notebook, params, timeout in pipeline:
    print(f"\n{'─'*60}")
    print(f"▶  {step_name}")
    print(f"{'─'*60}")
    start = time.time()
    try:
        result = dbutils.notebook.run(
            f"{NOTEBOOKS_DIR}/{notebook}",
            timeout_seconds=timeout,
            arguments=params,
        )
        elapsed = round(time.time() - start, 1)
        results.append({"step": step_name, "status": "✅ SUCCESS", "seconds": elapsed})
        print(f"✅ {step_name} — {elapsed}s")
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        results.append({"step": step_name, "status": f"❌ FAILED: {str(e)[:80]}", "seconds": elapsed})
        print(f"❌ {step_name} failed after {elapsed}s: {e}")
        break

# COMMAND ----------

total_elapsed = round(time.time() - total_start, 1)

print(f"\n{'═'*60}")
print(f"  PIPELINE COMPLETE — {total_elapsed}s total")
print(f"{'═'*60}")
for r in results:
    print(f"  {r['status']:<14} {r['step']:<35} {r['seconds']}s")
print(f"{'═'*60}")
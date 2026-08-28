# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 4 — AI Insights (Claude API)
# MAGIC Reads Gold Delta tables. Sends structured data to Claude API.
# MAGIC Returns analyst-level written insights saved back to Delta.

# COMMAND ----------

# MAGIC %pip install anthropic==0.40.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import anthropic
import json
from pyspark.sql import functions as F

# ── API key — add yours here ──────────────────────────────────────────────────
# In production: use dbutils.secrets.get(scope="netflix", key="ANTHROPIC_API_KEY")
# For Community Edition: paste your key directly (do NOT commit to git)
dbutils.widgets.text("ANTHROPIC_API_KEY", "", "Anthropic API Key")
ANTHROPIC_API_KEY = dbutils.widgets.get("ANTHROPIC_API_KEY")

if not ANTHROPIC_API_KEY:
    raise ValueError("Set your Anthropic API key in the widget above.")

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
print("✅ Claude client ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold tables into Python dicts for Claude

# COMMAND ----------

def df_to_dict(table_name, limit=50):
    return spark.table(table_name).limit(limit).toPandas().to_dict(orient="records")

type_by_year     = df_to_dict("netflix_gold.type_by_year")
top_countries    = df_to_dict("netflix_gold.top_countries", limit=15)
genres           = df_to_dict("netflix_gold.genre_distribution", limit=20)
ratings          = df_to_dict("netflix_gold.rating_distribution")
intl_growth      = df_to_dict("netflix_gold.international_growth")

# Summary stats
df_silver = spark.table("netflix_silver.titles")
total     = df_silver.count()
movies    = df_silver.filter(F.col("type") == "Movie").count()
shows     = df_silver.filter(F.col("type") == "TV Show").count()
countries = df_silver.filter(F.col("primary_country").isNotNull()).select("primary_country").distinct().count()

summary = {
    "total_titles": total,
    "movies": movies,
    "tv_shows": shows,
    "countries_covered": countries,
}

print("✅ Gold data loaded for Claude")
print(f"   Type-by-year rows : {len(type_by_year)}")
print(f"   Top countries     : {len(top_countries)}")
print(f"   Genre rows        : {len(genres)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claude Insight 1 — Content Strategy Analysis

# COMMAND ----------

def ask_claude(prompt: str, system: str = None) -> str:
    kwargs = {"model": "claude-haiku-4-5-20251001", "max_tokens": 1024, "messages": [{"role": "user", "content": prompt}]}
    if system:
        kwargs["system"] = system
    response = client.messages.create(**kwargs)
    return response.content[0].text

SYSTEM = (
    "You are a senior data analyst specializing in streaming media strategy. "
    "You receive structured data from a Netflix content database and produce "
    "concise, insight-driven analysis. Write in confident, professional prose. "
    "Cite specific numbers from the data. Do not use filler phrases."
)

# COMMAND ----------

prompt_strategy = f"""
Here is Netflix's content addition data by year and type:
{json.dumps(type_by_year, indent=2)}

And their international vs US content growth:
{json.dumps(intl_growth, indent=2)}

Overall summary: {json.dumps(summary)}

Write a 3-paragraph content strategy analysis covering:
1. How Netflix's content mix (movies vs TV shows) shifted over time
2. Their international expansion strategy — when it accelerated and what the data shows
3. One strategic insight a data analyst would highlight that is not obvious from the raw numbers
"""

insight_strategy = ask_claude(prompt_strategy, SYSTEM)
print("=== CONTENT STRATEGY INSIGHT ===")
print(insight_strategy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claude Insight 2 — Genre & Audience Analysis

# COMMAND ----------

prompt_genre = f"""
Here is Netflix's genre distribution across Movies and TV Shows:
{json.dumps(genres, indent=2)}

And rating distribution:
{json.dumps(ratings, indent=2)}

Write a 2-paragraph analysis covering:
1. Which genres dominate and what this tells us about Netflix's target audience
2. What the rating distribution reveals about content maturity strategy (family vs adult content)
"""

insight_genre = ask_claude(prompt_genre, SYSTEM)
print("=== GENRE & AUDIENCE INSIGHT ===")
print(insight_genre)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claude Insight 3 — Executive Summary

# COMMAND ----------

prompt_exec = f"""
Based on this Netflix content library data:
- Summary: {json.dumps(summary)}
- Top producing countries: {json.dumps(top_countries[:8])}
- Content added by year: {json.dumps(type_by_year)}

Write a 5-sentence executive summary that a business stakeholder could read in 30 seconds.
Include: total scale, geographic reach, content mix, key trend, and one forward-looking observation.
"""

insight_exec = ask_claude(prompt_exec, SYSTEM)
print("=== EXECUTIVE SUMMARY ===")
print(insight_exec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save AI insights to Delta table

# COMMAND ----------

import datetime

insights_data = [
    ("content_strategy", insight_strategy, datetime.datetime.now().isoformat()),
    ("genre_audience",   insight_genre,    datetime.datetime.now().isoformat()),
    ("executive_summary", insight_exec,    datetime.datetime.now().isoformat()),
]

df_insights = spark.createDataFrame(insights_data, ["insight_type", "insight_text", "generated_at"])

(
    df_insights.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("netflix_ai.insights")
)

print("✅ AI insights saved to netflix_ai.insights")
print(f"   Insights generated: {len(insights_data)}")

# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 07 — AI Layer 4: Strategic Analyst (Groq + Llama 70B)
# MAGIC Reads all Gold tables and AI-enriched data.
# MAGIC Produces: content strategy, trend analysis, gap analysis, executive summary.
# MAGIC All saved to netflix.netflix_ai.strategic_insights Delta table.

# COMMAND ----------

# MAGIC %pip install groq==0.13.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json, datetime
from groq import Groq
from pyspark.sql import functions as F

dbutils.widgets.text("GROQ_API_KEY", "", "Groq API Key")
API_KEY = dbutils.widgets.get("GROQ_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Groq API key. Get it free at console.groq.com")

client = Groq(api_key=API_KEY)
MODEL  = "llama3-70b-8192"

SYSTEM = """You are a senior data analyst specializing in streaming media strategy.
You receive structured data from a Netflix content database.
Write in confident, professional prose. Cite specific numbers.
Do not use filler phrases. Produce sharp analysis a business stakeholder would act on."""

def ask_groq(prompt: str, max_tokens: int = 1024) -> str:
    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user",   "content": prompt},
        ],
    )
    return response.choices[0].message.content.strip()

print(f"✅ Groq client ready — model: {MODEL}")

# COMMAND ----------

# MAGIC %md ## Load all Gold + AI data

# COMMAND ----------

def to_dict(table, limit=100):
    return spark.table(table).limit(limit).toPandas().to_dict(orient="records")

type_by_year  = to_dict("netflix.netflix_gold.content_by_year")
top_countries = to_dict("netflix.netflix_gold.country_analysis", limit=15)
genres        = to_dict("netflix.netflix_gold.genre_distribution", limit=20)
ratings       = to_dict("netflix.netflix_gold.rating_distribution")
intl_growth   = to_dict("netflix.netflix_gold.international_growth")

df_ai         = spark.table("netflix.netflix_ai.enriched_titles")
mood_dist     = df_ai.groupBy("mood").count().orderBy(F.col("count").desc()).toPandas().to_dict("records")
audience_dist = df_ai.groupBy("target_audience").count().orderBy(F.col("count").desc()).toPandas().to_dict("records")

# Combined data for gap analysis: country x mood x audience
# This allows AI to identify specific combinations that are underrepresented
country_mood_audience = (
    df_ai.filter(
        F.col("primary_country").isNotNull() & 
        F.col("mood").isNotNull() & 
        F.col("target_audience").isNotNull()
    )
    .groupBy("primary_country", "mood", "target_audience")
    .count()
    .orderBy(F.col("count").desc())
    .limit(100)
    .toPandas()
    .to_dict("records")
)

df_s    = spark.table("netflix.netflix_silver.stg_titles")
total   = df_s.count()
movies  = df_s.filter(F.col("type") == "Movie").count()
shows   = df_s.filter(F.col("type") == "TV Show").count()
ctries  = df_s.filter(F.col("primary_country").isNotNull()).select("primary_country").distinct().count()
summary = {"total_titles": total, "movies": movies, "tv_shows": shows, "countries": ctries}

print("✅ All data loaded")

# COMMAND ----------

# MAGIC %md ## Insight 1 — Content Strategy

# COMMAND ----------

insight_strategy = ask_groq(f"""
Netflix content by year and type:
{json.dumps(type_by_year, indent=2)}

International vs US growth:
{json.dumps(intl_growth, indent=2)}

Overall: {json.dumps(summary)}

Write a 3-paragraph content strategy analysis:
1. How Netflix's content mix shifted from 2015 to 2021 and what drove it
2. The international expansion — when it inflected, which regions led, what the numbers prove
3. One non-obvious strategic insight a senior analyst would surface
""", max_tokens=800)

print("=== CONTENT STRATEGY ===\n")
print(insight_strategy)

# COMMAND ----------

# MAGIC %md ## Insight 2 — Genre & Audience Intelligence

# COMMAND ----------

insight_genre = ask_groq(f"""
Genre distribution:
{json.dumps(genres, indent=2)}

Rating distribution:
{json.dumps(ratings, indent=2)}

Mood distribution (AI-enriched):
{json.dumps(mood_dist, indent=2)}

Audience distribution (AI-enriched):
{json.dumps(audience_dist, indent=2)}

Write 2 paragraphs:
1. Which genres and moods dominate and what this reveals about Netflix's actual target demographic
2. What the rating and audience data shows about maturity split — is Netflix moving toward or away from family content?
""", max_tokens=600)

print("=== GENRE & AUDIENCE ===\n")
print(insight_genre)

# COMMAND ----------

# MAGIC %md ## Insight 3 — Content Gap Analysis

# COMMAND ----------

insight_gaps = ask_groq(f"""
Combined content distribution by country, mood, and target audience:
{json.dumps(country_mood_audience, indent=2)}

Top producing countries overall:
{json.dumps(top_countries[:10], indent=2)}

Identify 3 specific content gaps in Netflix's catalog.
Look for combinations of (country, mood, audience) that have low or zero count but would be strategic.
For each gap:
- State the gap precisely (region, mood, audience combo)
- Reference the actual counts from the combined data above
- Explain why filling it would be strategically valuable

IMPORTANT: Only cite gaps you can verify from the combined data table.
""", max_tokens=700)

print("=== CONTENT GAP ANALYSIS ===\n")
print(insight_gaps)

# COMMAND ----------

# MAGIC %md ## Insight 4 — Executive Summary

# COMMAND ----------

insight_exec = ask_groq(f"""
Write a 5-sentence executive summary of Netflix's content library:
- {total:,} total titles across {ctries} countries
- {movies:,} movies ({round(movies/total*100,1)}%) and {shows:,} TV Shows
- Top countries: {[c['country'] for c in top_countries[:5]]}
- International growth: {json.dumps(intl_growth[-5:])}
- Key genres: {[g['genre'] for g in genres[:8]]}

One sentence each: scale, geographic reach, content mix, key trend, forward-looking observation.
A board member reads this in 30 seconds and knows everything important.
""", max_tokens=400)

print("=== EXECUTIVE SUMMARY ===\n")
print(insight_exec)

# COMMAND ----------

# MAGIC %md ## Save all insights to Delta

# COMMAND ----------

now = datetime.datetime.now().isoformat()
rows = [
    ("content_strategy",   insight_strategy, now),
    ("genre_audience",     insight_genre,    now),
    ("content_gap",        insight_gaps,     now),
    ("executive_summary",  insight_exec,     now),
]

df_insights = spark.createDataFrame(rows, ["insight_type", "insight_text", "generated_at"])
(
    df_insights.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("netflix.netflix_ai.strategic_insights")
)

print(f"✅ netflix.netflix_ai.strategic_insights — {len(rows)} insights saved")
display(df_insights.select("insight_type", "generated_at"))
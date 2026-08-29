# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — AI Layer 4: Strategic Analyst (Claude)
# MAGIC Claude acts as a senior data analyst.
# MAGIC Reads all Gold tables and AI-enriched data.
# MAGIC Produces: content strategy analysis, trend report, content gap analysis, executive summary.
# MAGIC All insights saved to netflix_ai.strategic_insights Delta table.

# COMMAND ----------

# MAGIC %pip install anthropic==0.40.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import anthropic, json, datetime
from pyspark.sql import functions as F

dbutils.widgets.text("ANTHROPIC_API_KEY", "", "Anthropic API Key")
API_KEY = dbutils.widgets.get("ANTHROPIC_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Anthropic API key in the widget above.")

client = anthropic.Anthropic(api_key=API_KEY)

SYSTEM = """You are a senior data analyst specializing in streaming media strategy.
You receive structured data from a Netflix content database.
Write in confident, professional prose. Cite specific numbers.
Do not use filler phrases like 'it is worth noting' or 'interestingly'.
Produce sharp, direct analysis a business stakeholder would act on."""

def ask_claude(prompt: str, max_tokens: int = 1024) -> str:
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=max_tokens,
        system=SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()

# COMMAND ----------

# MAGIC %md ## Load all Gold and AI data

# COMMAND ----------

def to_dict(table, limit=100):
    return spark.table(table).limit(limit).toPandas().to_dict(orient="records")

type_by_year   = to_dict("netflix_gold.content_by_year")
top_countries  = to_dict("netflix_gold.country_analysis", limit=15)
genres         = to_dict("netflix_gold.genre_distribution", limit=20)
ratings        = to_dict("netflix_gold.rating_distribution")
intl_growth    = to_dict("netflix_gold.international_growth")
top_directors  = to_dict("netflix_gold.top_directors", limit=10)

# AI enrichment stats
df_ai = spark.table("netflix_ai.enriched_titles")
mood_dist      = df_ai.groupBy("mood").count().orderBy(F.col("count").desc()).toPandas().to_dict("records")
audience_dist  = df_ai.groupBy("target_audience").count().orderBy(F.col("count").desc()).toPandas().to_dict("records")

# Summary
df_s     = spark.table("netflix_silver.stg_titles")
total    = df_s.count()
movies   = df_s.filter(F.col("type") == "Movie").count()
shows    = df_s.filter(F.col("type") == "TV Show").count()
ctries   = df_s.filter(F.col("primary_country").isNotNull()).select("primary_country").distinct().count()

summary = {"total_titles": total, "movies": movies, "tv_shows": shows, "countries": ctries}
print("✅ All data loaded for Claude")

# COMMAND ----------

# MAGIC %md ## Insight 1 — Content Strategy & Mix

# COMMAND ----------

insight_strategy = ask_claude(f"""
Netflix content data by year and type:
{json.dumps(type_by_year, indent=2)}

International vs US growth:
{json.dumps(intl_growth, indent=2)}

Overall: {json.dumps(summary)}

Write a 3-paragraph content strategy analysis:
1. How Netflix's content mix (movies vs TV shows) shifted from 2015 to 2021 and what drove it
2. The international expansion — when it inflected, which regions led it, what the numbers prove
3. One non-obvious strategic insight a senior analyst would surface from this data
""", max_tokens=800)

print("=== CONTENT STRATEGY ===\n")
print(insight_strategy)

# COMMAND ----------

# MAGIC %md ## Insight 2 — Genre & Audience Intelligence

# COMMAND ----------

insight_genre = ask_claude(f"""
Netflix genre distribution:
{json.dumps(genres, indent=2)}

Rating distribution:
{json.dumps(ratings, indent=2)}

AI-enriched mood distribution:
{json.dumps(mood_dist, indent=2)}

AI-enriched audience distribution:
{json.dumps(audience_dist, indent=2)}

Write 2 paragraphs:
1. Which genres and moods dominate and what this reveals about Netflix's actual target demographic (not their marketing claim)
2. What the rating and audience data shows about the maturity split — and whether Netflix is moving toward or away from family content
""", max_tokens=600)

print("=== GENRE & AUDIENCE ===\n")
print(insight_genre)

# COMMAND ----------

# MAGIC %md ## Insight 3 — Content Gap Analysis

# COMMAND ----------

insight_gaps = ask_claude(f"""
Top producing countries:
{json.dumps(top_countries[:10], indent=2)}

Genre distribution:
{json.dumps(genres, indent=2)}

Mood distribution from AI enrichment:
{json.dumps(mood_dist, indent=2)}

Audience distribution:
{json.dumps(audience_dist, indent=2)}

Identify 3 specific content gaps in Netflix's catalog — areas that are underrepresented given their scale.
For each gap:
- State the gap precisely (e.g. a specific region, mood, audience combo)
- Back it with numbers from the data
- Explain why filling it would be strategically valuable
""", max_tokens=700)

print("=== CONTENT GAP ANALYSIS ===\n")
print(insight_gaps)

# COMMAND ----------

# MAGIC %md ## Insight 4 — Executive Summary

# COMMAND ----------

insight_exec = ask_claude(f"""
Write a 5-sentence executive summary of Netflix's content library based on:
- {total:,} total titles across {ctries} countries
- {movies:,} movies ({round(movies/total*100,1)}%) and {shows:,} TV Shows ({round(shows/total*100,1)}%)
- Top countries: {json.dumps([c['country'] for c in top_countries[:5]])}
- International growth trend: {json.dumps(intl_growth[-5:])}
- Key genres: {json.dumps([g['genre'] for g in genres[:8]])}

One sentence each: scale, geographic reach, content mix, key trend, forward-looking observation.
A board member should read this and know everything important in 30 seconds.
""", max_tokens=400)

print("=== EXECUTIVE SUMMARY ===\n")
print(insight_exec)

# COMMAND ----------

# MAGIC %md ## Save all insights to Delta

# COMMAND ----------

now = datetime.datetime.now().isoformat()

rows = [
    ("content_strategy",  insight_strategy, now),
    ("genre_audience",    insight_genre,    now),
    ("content_gap",       insight_gaps,     now),
    ("executive_summary", insight_exec,     now),
]

df_insights = spark.createDataFrame(rows, ["insight_type", "insight_text", "generated_at"])

(
    df_insights.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("netflix_ai.strategic_insights")
)

print(f"✅ netflix_ai.strategic_insights — {len(rows)} insights saved")
display(df_insights.select("insight_type", "generated_at"))

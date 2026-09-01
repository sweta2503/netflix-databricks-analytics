# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — AI Layer 1: Content Enrichment (Groq + Llama)
# MAGIC Groq reads each Netflix title's description and generates structured metadata:
# MAGIC mood, themes, target_audience, content_tags, decade_feel.
# MAGIC Output written back to Delta as netflix.netflix_ai.enriched_titles.

# COMMAND ----------

# MAGIC %pip install groq==0.13.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json, time
from groq import Groq
from pyspark.sql import functions as F

dbutils.widgets.text("GROQ_API_KEY", "", "Groq API Key")
API_KEY = dbutils.widgets.get("GROQ_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Groq API key in the widget above. Get it free at console.groq.com")

client = Groq(api_key=API_KEY)
MODEL  = "llama-3.1-8b-instant"   # fast + free, ideal for high-volume enrichment
print(f"✅ Groq client ready — model: {MODEL}")

# COMMAND ----------

# MAGIC %md ## Load Silver titles

# COMMAND ----------

df_silver = spark.table("netflix.netflix_silver.stg_titles")

SAMPLE_SIZE = 500
df_sample = df_silver.filter(
    F.col("description").isNotNull() & (F.col("description") != "")
).limit(SAMPLE_SIZE)

titles = df_sample.select("show_id", "title", "type", "genres_raw", "description").toPandas()
print(f"📥 Titles to enrich: {len(titles):,}")

# COMMAND ----------

# MAGIC %md ## Enrichment with Groq (batched, with retry)

# COMMAND ----------

SYSTEM_PROMPT = """You are a content metadata specialist for a streaming platform.
Given a title's description, return ONLY valid JSON with these exact keys:
- mood: one of [Dark, Uplifting, Tense, Lighthearted, Emotional, Mysterious, Inspiring, Funny]
- themes: array of 2-4 theme strings (e.g. ["Family", "Redemption", "Crime"])
- target_audience: one of [Kids, Teens, Young Adults, Adults, All Ages]
- content_tags: array of 3-5 descriptive tags (e.g. ["based on true story", "ensemble cast"])
- decade_feel: one of [Classic, 80s, 90s, 2000s, Modern, Timeless]
Return only the JSON object. No explanation. No markdown fences."""

def enrich_title(show_id, title, content_type, genres, description):
    prompt = f"""Title: {title}
Type: {content_type}
Genres: {genres}
Description: {description}

Return the JSON metadata."""

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                max_tokens=256,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return {"show_id": show_id, "error": None, **json.loads(raw.strip())}
        except json.JSONDecodeError:
            return {"show_id": show_id, "error": "invalid_json", "mood": None,
                    "themes": [], "target_audience": None, "content_tags": [], "decade_feel": None}
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                return {"show_id": show_id, "error": str(e), "mood": None,
                        "themes": [], "target_audience": None, "content_tags": [], "decade_feel": None}

# COMMAND ----------

# MAGIC %md ## Run enrichment

# COMMAND ----------

results = []
errors  = 0

for i, row in titles.iterrows():
    result = enrich_title(
        row["show_id"], row["title"], row["type"],
        row["genres_raw"], row["description"]
    )
    results.append(result)
    if result.get("error"):
        errors += 1
    if (i + 1) % 50 == 0:
        print(f"  processed {i+1}/{len(titles)} | errors: {errors}")

print(f"\n✅ Enrichment complete — {len(results):,} titles | {errors} errors")

# COMMAND ----------

# MAGIC %md ## Save to Delta

# COMMAND ----------

import pandas as pd

df_enriched = pd.DataFrame(results)
df_enriched["themes"]       = df_enriched["themes"].apply(lambda x: x if isinstance(x, list) else [])
df_enriched["content_tags"] = df_enriched["content_tags"].apply(lambda x: x if isinstance(x, list) else [])

df_silver_pd = df_sample.select(
    "show_id", "title", "type", "genres_raw", "primary_country", "release_year", "description"
).toPandas()

df_final = df_silver_pd.merge(df_enriched, on="show_id", how="left")
df_spark  = spark.createDataFrame(df_final)

(
    df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("netflix.netflix_ai.enriched_titles")
)

print(f"✅ netflix.netflix_ai.enriched_titles — {df_spark.count():,} rows")

# COMMAND ----------

# MAGIC %md ## New queries unlocked by AI enrichment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT mood, count(*) as count
# MAGIC FROM netflix.netflix_ai.enriched_titles
# MAGIC WHERE mood IS NOT NULL
# MAGIC GROUP BY mood ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT target_audience, type, count(*) as count
# MAGIC FROM netflix.netflix_ai.enriched_titles
# MAGIC WHERE target_audience IS NOT NULL
# MAGIC GROUP BY target_audience, type ORDER BY count DESC

# COMMAND ----------

display(spark.table("netflix.netflix_ai.enriched_titles").select(
    "title", "type", "mood", "themes", "target_audience", "decade_feel", "content_tags"
).limit(10))

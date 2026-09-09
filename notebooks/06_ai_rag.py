# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 — AI Layer 3: Content Discovery via LLM (Groq + Llama 70B)
# MAGIC **Note**: This is NOT true RAG (Retrieval-Augmented Generation). 
# MAGIC No embeddings or vector search — it sends up to 500 enriched catalog records directly in the LLM prompt.
# MAGIC
# MAGIC User describes what they want to watch in plain English.
# MAGIC Groq analyzes the enriched catalog and recommends the best matching titles.

# COMMAND ----------

# MAGIC %pip install groq==0.13.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from groq import Groq
from pyspark.sql import functions as F

dbutils.widgets.text("GROQ_API_KEY", "", "Groq API Key")
API_KEY = dbutils.widgets.get("GROQ_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Groq API key. Get it free at console.groq.com")

client = Groq(api_key=API_KEY)
MODEL  = "llama3-70b-8192"
print(f"✅ Groq client ready — model: {MODEL}")

# COMMAND ----------

# MAGIC %md ## Build catalog context

# COMMAND ----------

df_catalog = (
    spark.table("netflix.netflix_ai.enriched_titles")
    .select("show_id", "title", "type", "genres_raw", "primary_country",
            "release_year", "mood", "target_audience", "decade_feel", "themes", "description")
    .filter(F.col("description").isNotNull())
)

catalog_rows = df_catalog.limit(500).toPandas().to_dict(orient="records")
print(f"📚 Catalog loaded: {len(catalog_rows)} titles")

# COMMAND ----------

def build_catalog_context(rows):
    lines = []
    for r in rows:
        themes = ", ".join(r.get("themes") or [])
        lines.append(
            f"[{r['show_id']}] {r['title']} ({r['type']}, {r['release_year']}) | "
            f"Mood: {r.get('mood')} | Audience: {r.get('target_audience')} | "
            f"Genres: {r['genres_raw']} | Themes: {themes} | "
            f"Desc: {str(r['description'])[:200]}"
        )
    return "\n".join(lines)

CATALOG_CONTEXT = build_catalog_context(catalog_rows)

SYSTEM_PROMPT = f"""You are a Netflix content recommendation engine.
You have access to this Netflix catalog:

{CATALOG_CONTEXT}

When a user describes what they want to watch, find the 5 best matches.
For each match state:
- Title and type
- Why it matches (1 sentence citing specific description details)
- Mood, audience, genres

Only recommend titles that exist in the catalog above.
Ground every recommendation in the actual description provided."""

# COMMAND ----------

def semantic_search(query: str):
    print(f"\n🔎 Query: \"{query}\"")
    print("-" * 60)
    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=1024,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Find me: {query}"},
        ],
    )
    print(response.choices[0].message.content)

# COMMAND ----------

# MAGIC %md ## Live demos

# COMMAND ----------

semantic_search("a dark psychological thriller set in Europe with a complex female lead")

# COMMAND ----------

semantic_search("something funny for the whole family, not too long, feel-good ending")

# COMMAND ----------

semantic_search("a documentary about crime or corruption in a real-world setting")

# COMMAND ----------

semantic_search("a gripping limited series I can finish in one weekend, emotional and intense")

# COMMAND ----------

# MAGIC %md ## Try your own

# COMMAND ----------

dbutils.widgets.text("search_query", "an inspiring true story about someone overcoming impossible odds", "What do you want to watch?")
q = dbutils.widgets.get("search_query")
if q:
    semantic_search(q)
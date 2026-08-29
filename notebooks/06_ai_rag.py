# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — AI Layer 3: Semantic Content Search (RAG)
# MAGIC User describes what they want to watch in plain English.
# MAGIC Claude finds the best matching Netflix titles grounded in real descriptions.
# MAGIC No vector database needed — uses Claude's context window directly.

# COMMAND ----------

# MAGIC %pip install anthropic==0.40.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import anthropic, json
from pyspark.sql import functions as F

dbutils.widgets.text("ANTHROPIC_API_KEY", "", "Anthropic API Key")
API_KEY = dbutils.widgets.get("ANTHROPIC_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Anthropic API key in the widget above.")

client = anthropic.Anthropic(api_key=API_KEY)

# COMMAND ----------

# MAGIC %md ## Load enriched catalog

# COMMAND ----------

# Build a lightweight catalog Claude can search through
df_catalog = (
    spark.table("netflix_ai.enriched_titles")
    .select(
        "show_id", "title", "type", "genres_raw",
        "primary_country", "release_year",
        "mood", "target_audience", "decade_feel", "themes",
        "description"
    )
    .filter(F.col("description").isNotNull())
)

catalog_rows = df_catalog.limit(500).toPandas().to_dict(orient="records")
print(f"📚 Catalog loaded: {len(catalog_rows)} titles")

# COMMAND ----------

# MAGIC %md ## RAG search function

# COMMAND ----------

def build_catalog_context(rows: list) -> str:
    lines = []
    for r in rows:
        themes = ", ".join(r.get("themes") or [])
        lines.append(
            f"[{r['show_id']}] {r['title']} ({r['type']}, {r['release_year']}) | "
            f"Mood: {r.get('mood')} | Audience: {r.get('target_audience')} | "
            f"Genres: {r['genres_raw']} | Themes: {themes} | "
            f"Description: {r['description'][:200]}"
        )
    return "\n".join(lines)


CATALOG_CONTEXT = build_catalog_context(catalog_rows)

SYSTEM_PROMPT = f"""You are a Netflix content recommendation engine.
You have access to the following Netflix catalog:

{CATALOG_CONTEXT}

When a user describes what they want to watch, find the 5 best matches from the catalog above.
For each match return:
- Title and type
- Why it matches (1 sentence, citing specific description details)
- Mood, audience, genres

Be specific. Only recommend titles that actually exist in the catalog above.
Ground every recommendation in the actual description provided.
"""

def semantic_search(query: str):
    print(f"\n🔎 Query: \"{query}\"")
    print("-" * 60)
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"Find me: {query}"}],
    )
    print(response.content[0].text)

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

# MAGIC %md ## Interactive widget

# COMMAND ----------

dbutils.widgets.text(
    "search_query",
    "an inspiring true story about someone overcoming impossible odds",
    "What do you want to watch?"
)
user_query = dbutils.widgets.get("search_query")
if user_query:
    semantic_search(user_query)

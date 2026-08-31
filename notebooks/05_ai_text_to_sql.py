# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — AI Layer 2: Text-to-SQL (Groq + Llama 70B)
# MAGIC User asks a plain-English question about Netflix data.
# MAGIC Groq writes Spark SQL. Query executes live on Gold Delta tables.

# COMMAND ----------

# MAGIC %pip install groq==0.13.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from groq import Groq

dbutils.widgets.text("GROQ_API_KEY", "", "Groq API Key")
API_KEY = dbutils.widgets.get("GROQ_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Groq API key. Get it free at console.groq.com")

client = Groq(api_key=API_KEY)
MODEL  = "llama3-70b-8192"   # 70B for better SQL reasoning
print(f"✅ Groq client ready — model: {MODEL}")

# COMMAND ----------

SCHEMA_CONTEXT = """
You have access to these Databricks Delta tables. Use only Spark SQL syntax.

TABLE: netflix_gold.content_by_year
  added_year INT, type STRING, titles_count INT

TABLE: netflix_gold.genre_distribution
  genre STRING, type STRING, title_count INT

TABLE: netflix_gold.country_analysis
  country STRING, total_titles INT, movies INT, tv_shows INT, movie_pct DOUBLE

TABLE: netflix_gold.rating_distribution
  rating STRING, type STRING, count INT, pct_within_type DOUBLE

TABLE: netflix_gold.top_directors
  director STRING, total_titles INT, movies INT, tv_shows INT, earliest_year INT, latest_year INT

TABLE: netflix_gold.international_growth
  added_year INT, total_titles INT, us_titles INT, international_titles INT, international_pct DOUBLE

TABLE: netflix_ai.enriched_titles
  show_id STRING, title STRING, type STRING, genres_raw STRING,
  primary_country STRING, release_year INT, description STRING,
  mood STRING, themes ARRAY<STRING>, target_audience STRING,
  content_tags ARRAY<STRING>, decade_feel STRING

Rules:
- Spark SQL only (no PostgreSQL/Snowflake syntax)
- Always add LIMIT 50 unless asking for totals or counts
- Return ONLY the SQL query — no explanation, no markdown fences
"""

# COMMAND ----------

def text_to_sql(question: str) -> str:
    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=512,
        messages=[
            {"role": "system", "content": SCHEMA_CONTEXT},
            {"role": "user",   "content": f"Question: {question}\n\nSQL:"},
        ],
    )
    sql = response.choices[0].message.content.strip()
    if sql.startswith("```"):
        sql = "\n".join(sql.split("\n")[1:-1])
    return sql.strip()


def ask(question: str):
    print(f"\n❓ {question}")
    sql = text_to_sql(question)
    print(f"\n🔍 SQL:\n{sql}")
    try:
        result = spark.sql(sql)
        print(f"\n📊 {result.count()} rows returned")
        display(result)
        return result
    except Exception as e:
        print(f"❌ SQL error: {e}")

# COMMAND ----------

# MAGIC %md ## Live demos

# COMMAND ----------

ask("Which year did Netflix add the most international content?")

# COMMAND ----------

ask("What are the top 5 genres for TV Shows on Netflix?")

# COMMAND ----------

ask("Which countries produce the most Dark-mood content according to AI enrichment?")

# COMMAND ----------

ask("How many Kids titles were added after 2018?")

# COMMAND ----------

ask("Which director has the widest year range — earliest to latest title?")

# COMMAND ----------

# MAGIC %md ## Try your own question

# COMMAND ----------

dbutils.widgets.text("your_question", "What percentage of Netflix content is international in 2020?", "Ask a question")
q = dbutils.widgets.get("your_question")
if q:
    ask(q)

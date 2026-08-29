# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — AI Layer 2: Text-to-SQL (Claude)
# MAGIC User asks a plain-English question about Netflix data.
# MAGIC Claude writes Spark SQL. Query executes live on Gold Delta tables.
# MAGIC Results returned instantly.

# COMMAND ----------

# MAGIC %pip install anthropic==0.40.0 --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import anthropic
from pyspark.sql import functions as F

dbutils.widgets.text("ANTHROPIC_API_KEY", "", "Anthropic API Key")
API_KEY = dbutils.widgets.get("ANTHROPIC_API_KEY")
if not API_KEY:
    raise ValueError("Paste your Anthropic API key in the widget above.")

client = anthropic.Anthropic(api_key=API_KEY)

# COMMAND ----------

# MAGIC %md ## Gold table schemas — what Claude knows about

# COMMAND ----------

SCHEMA_CONTEXT = """
You have access to these Databricks Delta tables. Use only Spark SQL syntax.

TABLE: netflix_gold.content_by_year
  added_year     INT      -- year title was added to Netflix
  type           STRING   -- 'Movie' or 'TV Show'
  titles_count   INT      -- number of titles added that year

TABLE: netflix_gold.genre_distribution
  genre          STRING   -- genre name (e.g. 'Dramas', 'Comedies')
  type           STRING   -- 'Movie' or 'TV Show'
  title_count    INT      -- number of titles in that genre

TABLE: netflix_gold.country_analysis
  country        STRING   -- producing country
  total_titles   INT
  movies         INT
  tv_shows       INT
  movie_pct      DOUBLE   -- % of that country's content that is movies

TABLE: netflix_gold.rating_distribution
  rating         STRING   -- e.g. 'TV-MA', 'PG-13', 'R'
  type           STRING
  count          INT
  pct_within_type DOUBLE  -- % within Movie or TV Show category

TABLE: netflix_gold.top_directors
  director       STRING
  total_titles   INT
  movies         INT
  tv_shows       INT
  earliest_year  INT
  latest_year    INT

TABLE: netflix_gold.international_growth
  added_year          INT
  total_titles        INT
  us_titles           INT
  international_titles INT
  international_pct   DOUBLE

TABLE: netflix_ai.enriched_titles
  show_id        STRING
  title          STRING
  type           STRING
  genres_raw     STRING
  primary_country STRING
  release_year   INT
  description    STRING
  mood           STRING   -- Dark, Uplifting, Tense, Lighthearted, Emotional, Mysterious, Inspiring, Funny
  themes         ARRAY<STRING>
  target_audience STRING  -- Kids, Teens, Young Adults, Adults, All Ages
  content_tags   ARRAY<STRING>
  decade_feel    STRING   -- Classic, 80s, 90s, 2000s, Modern, Timeless

Rules:
- Use Spark SQL only (not Snowflake or PostgreSQL syntax)
- Always add LIMIT 50 unless the question asks for totals
- Return ONLY the SQL query, no explanation, no markdown fences
- Use table aliases for readability
"""

# COMMAND ----------

def text_to_sql(question: str) -> str:
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=SCHEMA_CONTEXT,
        messages=[{"role": "user", "content": f"Question: {question}\n\nSQL:"}],
    )
    sql = response.content[0].text.strip()
    # Strip markdown fences if Claude added them
    if sql.startswith("```"):
        sql = "\n".join(sql.split("\n")[1:-1])
    return sql.strip()


def ask(question: str):
    print(f"\n❓ Question: {question}")
    sql = text_to_sql(question)
    print(f"\n🔍 Generated SQL:\n{sql}")
    try:
        result = spark.sql(sql)
        print(f"\n📊 Result ({result.count()} rows):")
        display(result)
        return result
    except Exception as e:
        print(f"❌ SQL error: {e}")
        return None

# COMMAND ----------

# MAGIC %md ## Live demos — ask real questions

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

# MAGIC %md ## Interactive widget — try your own question

# COMMAND ----------

dbutils.widgets.text("your_question", "What percentage of Netflix content is international in 2020?", "Ask a question")
custom_q = dbutils.widgets.get("your_question")
if custom_q:
    ask(custom_q)

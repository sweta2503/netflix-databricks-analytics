# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Business Analytics Tables
# MAGIC Reads Silver. Builds 6 dbt mart models as Delta tables.
# MAGIC These are the final, query-ready tables for AI and dashboard.

# COMMAND ----------

import subprocess, os

DBT_ROOT = "/tmp/netflix_dbt"

# Write all mart SQL models
marts = {}

marts["content_by_year"] = """
with base as (
    select * from {{ ref('stg_titles') }}
    where added_year is not null
)
select
    added_year,
    type,
    count(*) as titles_count
from base
group by added_year, type
order by added_year, type
"""

marts["genre_distribution"] = """
with exploded as (
    select
        explode(genres) as genre,
        type
    from {{ ref('stg_titles') }}
),
cleaned as (
    select trim(genre) as genre, type
    from exploded
    where trim(genre) != ''
)
select
    genre,
    type,
    count(*) as title_count
from cleaned
group by genre, type
order by title_count desc
"""

marts["country_analysis"] = """
with base as (
    select * from {{ ref('stg_titles') }}
    where primary_country is not null
      and trim(primary_country) != ''
)
select
    trim(primary_country) as country,
    count(*) as total_titles,
    sum(case when is_movie then 1 else 0 end) as movies,
    sum(case when not is_movie then 1 else 0 end) as tv_shows,
    round(
        sum(case when is_movie then 1 else 0 end) * 100.0 / count(*), 1
    ) as movie_pct
from base
group by trim(primary_country)
order by total_titles desc
limit 25
"""

marts["rating_distribution"] = """
select
    rating,
    type,
    count(*) as count,
    round(count(*) * 100.0 / sum(count(*)) over (partition by type), 1) as pct_within_type
from {{ ref('stg_titles') }}
where rating is not null
group by rating, type
order by type, count desc
"""

marts["top_directors"] = """
select
    director,
    count(*) as total_titles,
    sum(case when type = 'Movie'   then 1 else 0 end) as movies,
    sum(case when type = 'TV Show' then 1 else 0 end) as tv_shows,
    min(release_year) as earliest_year,
    max(release_year) as latest_year
from {{ ref('stg_titles') }}
where director is not null
group by director
having count(*) >= 2
order by total_titles desc
limit 20
"""

marts["international_growth"] = """
with base as (
    select
        added_year,
        primary_country,
        (trim(primary_country) = 'United States') as is_us
    from {{ ref('stg_titles') }}
    where added_year is not null
      and primary_country is not null
)
select
    added_year,
    count(*) as total_titles,
    sum(case when is_us then 1 else 0 end) as us_titles,
    sum(case when not is_us then 1 else 0 end) as international_titles,
    round(
        sum(case when not is_us then 1 else 0 end) * 100.0 / count(*), 1
    ) as international_pct
from base
group by added_year
order by added_year
"""

# Write mart SQL files
for name, sql in marts.items():
    with open(f"{DBT_ROOT}/models/marts/{name}.sql", "w") as f:
        f.write(sql.strip())
    print(f"  wrote models/marts/{name}.sql")

# COMMAND ----------

# Write mart schema.yml with tests
schema_yml = """
version: 2
models:
  - name: content_by_year
    columns:
      - name: added_year
        tests: [not_null]
      - name: titles_count
        tests: [not_null]

  - name: country_analysis
    columns:
      - name: country
        tests: [not_null, unique]

  - name: genre_distribution
    columns:
      - name: genre
        tests: [not_null]

  - name: rating_distribution
    columns:
      - name: rating
        tests: [not_null]

  - name: top_directors
    columns:
      - name: director
        tests: [not_null, unique]

  - name: international_growth
    columns:
      - name: added_year
        tests: [not_null, unique]
"""

with open(f"{DBT_ROOT}/models/marts/schema.yml", "w") as f:
    f.write(schema_yml)

# COMMAND ----------

def run_dbt(command):
    result = subprocess.run(
        f"cd {DBT_ROOT} && dbt {command} --profiles-dir /root/.dbt",
        shell=True, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise RuntimeError(f"dbt {command} failed")

# COMMAND ----------

print("Running: dbt run --select marts")
run_dbt("run --select marts")

# COMMAND ----------

print("Running: dbt test --select marts")
run_dbt("test --select marts")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

gold_tables = [
    "netflix_gold.content_by_year",
    "netflix_gold.genre_distribution",
    "netflix_gold.country_analysis",
    "netflix_gold.rating_distribution",
    "netflix_gold.top_directors",
    "netflix_gold.international_growth",
]

print("=== GOLD LAYER READY ===")
for t in gold_tables:
    try:
        count = spark.table(t).count()
        print(f"  ✅ {t:<45} {count:>5,} rows")
    except Exception as e:
        print(f"  ❌ {t} — {e}")

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: dbt Transformations
# MAGIC Runs the dbt project against Databricks.
# MAGIC dbt builds staging views and Silver Delta tables from Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Configure dbt profile
# MAGIC dbt needs your Databricks host, HTTP path, and token.
# MAGIC Get these from: User Settings → Access Tokens (in Databricks).

# COMMAND ----------

import os

dbutils.widgets.text("DATABRICKS_HOST",      "", "Databricks Host (e.g. adb-xxx.azuredatabricks.net)")
dbutils.widgets.text("DATABRICKS_HTTP_PATH", "", "HTTP Path (e.g. /sql/1.0/warehouses/xxx or cluster http path)")
dbutils.widgets.text("DATABRICKS_TOKEN",     "", "Databricks Token")

HOST      = dbutils.widgets.get("DATABRICKS_HOST")
HTTP_PATH = dbutils.widgets.get("DATABRICKS_HTTP_PATH")
TOKEN     = dbutils.widgets.get("DATABRICKS_TOKEN")

# Write dbt profiles.yml dynamically
profiles_content = f"""
netflix_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: {HOST}
      http_path: {HTTP_PATH}
      token: {TOKEN}
      catalog: hive_metastore
      schema: netflix_silver
      threads: 4
"""

profiles_dir = "/root/.dbt"
os.makedirs(profiles_dir, exist_ok=True)
with open(f"{profiles_dir}/profiles.yml", "w") as f:
    f.write(profiles_content)

print("✅ dbt profiles.yml written")

# COMMAND ----------

# MAGIC %md ## Step 2 — Write dbt project files to driver

# COMMAND ----------

import os, json

DBT_ROOT = "/tmp/netflix_dbt"
os.makedirs(f"{DBT_ROOT}/models/staging", exist_ok=True)
os.makedirs(f"{DBT_ROOT}/models/marts",   exist_ok=True)

# dbt_project.yml
with open(f"{DBT_ROOT}/dbt_project.yml", "w") as f:
    f.write("""
name: netflix_project
version: '1.0.0'
config-version: 2
profile: netflix_project
model-paths: ["models"]
models:
  netflix_project:
    staging:
      +materialized: view
      +schema: netflix_silver
    marts:
      +materialized: table
      +file_format: delta
      +schema: netflix_gold
""")

# sources.yml
with open(f"{DBT_ROOT}/models/staging/sources.yml", "w") as f:
    f.write("""
version: 2
sources:
  - name: bronze
    database: hive_metastore
    schema: netflix_bronze
    tables:
      - name: raw_titles
""")

# stg_titles.sql
with open(f"{DBT_ROOT}/models/staging/stg_titles.sql", "w") as f:
    f.write("""
with source as (
    select * from {{ source('bronze', 'raw_titles') }}
),
cleaned as (
    select
        show_id,
        type,
        title,
        nullif(trim(director), '')          as director,
        nullif(trim(cast), '')              as cast_members,
        nullif(trim(country), '')           as country,
        split(nullif(trim(country), ''), ',')[0] as primary_country,
        try_cast(release_year as int)       as release_year,
        try_to_timestamp(
            trim(date_added), 'MMMM d, yyyy'
        )                                   as date_added,
        year(try_to_timestamp(
            trim(date_added), 'MMMM d, yyyy'
        ))                                  as added_year,
        nullif(trim(rating), '')            as rating,
        duration,
        regexp_extract(duration, '(\\\\d+)', 1)::int as duration_value,
        case
            when duration like '%Season%' then 'Seasons'
            when duration like '%min%'    then 'Minutes'
            else null
        end                                 as duration_unit,
        trim(listed_in)                     as genres_raw,
        split(trim(listed_in), ', ')        as genres,
        description,
        (type = 'Movie')                    as is_movie
    from source
    where title is not null
)
select * from cleaned
""")

print("✅ dbt project files written to /tmp/netflix_dbt")

# COMMAND ----------

# MAGIC %md ## Step 3 — Run dbt

# COMMAND ----------

import subprocess

def run_dbt(command):
    result = subprocess.run(
        f"cd {DBT_ROOT} && dbt {command} --profiles-dir /root/.dbt",
        shell=True, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise RuntimeError(f"dbt {command} failed")
    return result.stdout

# COMMAND ----------

print("Running: dbt debug")
run_dbt("debug")

# COMMAND ----------

print("Running: dbt run --select staging")
run_dbt("run --select staging")

# COMMAND ----------

print("Running: dbt test --select staging")
run_dbt("test --select staging")

# COMMAND ----------

# Verify
df_silver = spark.table("netflix_silver.stg_titles")
print(f"✅ Silver view ready — {df_silver.count():,} rows")
display(df_silver.limit(5))

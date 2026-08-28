# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 5 — Dashboard
# MAGIC Reads Gold + AI tables. Renders charts and Claude's written insights side by side.
# MAGIC This is the final output of the pipeline — what you'd show a stakeholder.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from pyspark.sql import functions as F

plt.rcParams["figure.dpi"] = 130
plt.rcParams["axes.spines.top"] = False
plt.rcParams["axes.spines.right"] = False
NETFLIX_RED = "#E50914"
DARK_BG     = "#141414"
LIGHT_TEXT  = "#FFFFFF"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 1 — Content Added by Year (Movies vs TV Shows)

# COMMAND ----------

df_year = spark.table("netflix_gold.type_by_year").toPandas()
df_pivot = df_year.pivot(index="added_year", columns="type", values="titles_count").fillna(0).reset_index()

fig, ax = plt.subplots(figsize=(12, 5))
ax.set_facecolor("#1a1a1a")
fig.patch.set_facecolor(DARK_BG)

width = 0.4
years = df_pivot["added_year"]
x = range(len(years))

bars1 = ax.bar([i - width/2 for i in x], df_pivot.get("Movie", 0), width=width, label="Movie", color=NETFLIX_RED, alpha=0.9)
bars2 = ax.bar([i + width/2 for i in x], df_pivot.get("TV Show", 0), width=width, label="TV Show", color="#564d4d", alpha=0.9)

ax.set_xticks(list(x))
ax.set_xticklabels(years, rotation=45, color=LIGHT_TEXT)
ax.tick_params(colors=LIGHT_TEXT)
ax.set_title("Netflix Content Added by Year", color=LIGHT_TEXT, fontsize=14, pad=12)
ax.set_xlabel("Year Added", color=LIGHT_TEXT)
ax.set_ylabel("Titles Added", color=LIGHT_TEXT)
ax.legend(facecolor="#2a2a2a", labelcolor=LIGHT_TEXT)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
for spine in ax.spines.values():
    spine.set_edgecolor("#333")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 2 — Top 10 Countries by Content Volume

# COMMAND ----------

df_countries = spark.table("netflix_gold.top_countries").toPandas().head(10)

fig, ax = plt.subplots(figsize=(10, 6))
ax.set_facecolor("#1a1a1a")
fig.patch.set_facecolor(DARK_BG)

bars = ax.barh(df_countries["primary_country"][::-1], df_countries["total_titles"][::-1],
               color=NETFLIX_RED, alpha=0.85)

for bar, val in zip(bars, df_countries["total_titles"][::-1]):
    ax.text(bar.get_width() + 10, bar.get_y() + bar.get_height()/2,
            f"{int(val):,}", va="center", color=LIGHT_TEXT, fontsize=9)

ax.set_title("Top 10 Countries by Netflix Content", color=LIGHT_TEXT, fontsize=14, pad=12)
ax.set_xlabel("Total Titles", color=LIGHT_TEXT)
ax.tick_params(colors=LIGHT_TEXT)
for spine in ax.spines.values():
    spine.set_edgecolor("#333")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 3 — Top 15 Genres

# COMMAND ----------

df_genres = spark.table("netflix_gold.genre_distribution").toPandas()
df_top_genres = df_genres.groupby("genre")["count"].sum().reset_index().sort_values("count", ascending=False).head(15)

fig, ax = plt.subplots(figsize=(10, 6))
ax.set_facecolor("#1a1a1a")
fig.patch.set_facecolor(DARK_BG)

colors = [NETFLIX_RED if i < 5 else "#564d4d" for i in range(len(df_top_genres))]
ax.barh(df_top_genres["genre"][::-1], df_top_genres["count"][::-1], color=colors[::-1], alpha=0.85)

ax.set_title("Top 15 Genres on Netflix", color=LIGHT_TEXT, fontsize=14, pad=12)
ax.set_xlabel("Number of Titles", color=LIGHT_TEXT)
ax.tick_params(colors=LIGHT_TEXT)
for spine in ax.spines.values():
    spine.set_edgecolor("#333")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 4 — International vs US Content Growth

# COMMAND ----------

df_intl = spark.table("netflix_gold.international_growth").toPandas()

fig, ax = plt.subplots(figsize=(12, 5))
ax.set_facecolor("#1a1a1a")
fig.patch.set_facecolor(DARK_BG)

ax.fill_between(df_intl["added_year"], df_intl["intl_pct"], alpha=0.3, color=NETFLIX_RED)
ax.plot(df_intl["added_year"], df_intl["intl_pct"], color=NETFLIX_RED, linewidth=2.5, marker="o")

ax.axhline(50, color="#555", linestyle="--", linewidth=1, label="50% threshold")
ax.set_title("International Content % of Total Netflix Additions", color=LIGHT_TEXT, fontsize=14, pad=12)
ax.set_xlabel("Year", color=LIGHT_TEXT)
ax.set_ylabel("International %", color=LIGHT_TEXT)
ax.tick_params(colors=LIGHT_TEXT)
ax.legend(facecolor="#2a2a2a", labelcolor=LIGHT_TEXT)
for spine in ax.spines.values():
    spine.set_edgecolor("#333")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claude AI Insights

# COMMAND ----------

df_insights = spark.table("netflix_ai.insights").toPandas()

for _, row in df_insights.iterrows():
    label = row["insight_type"].replace("_", " ").title()
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(row["insight_text"])
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline complete
# MAGIC
# MAGIC | Layer | Table | Status |
# MAGIC |---|---|---|
# MAGIC | Bronze | `netflix.raw_titles` | ✅ |
# MAGIC | Silver | `netflix_silver.titles` | ✅ |
# MAGIC | Gold | `netflix_gold.*` (6 tables) | ✅ |
# MAGIC | AI | `netflix_ai.insights` | ✅ |

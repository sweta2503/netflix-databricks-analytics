# Netflix Analytics Pipeline - YouTube Script

## Video Title Options
1. "Building an End-to-End Data Pipeline with Databricks + AI (8,807 Netflix Titles)"
2. "How I Built a Netflix Analytics Platform with dbt, Spark & Groq AI"
3. "Medallion Architecture + LLM Enrichment: Real Data Engineering Project Walkthrough"

---

## HOOK (0:00 - 0:20)

"What if you could analyze 8,807 Netflix titles, detect mood and audience for every one, and generate strategic insights—all in a single data pipeline?

That's exactly what I built with Databricks, dbt, and Groq AI.

No Airflow. No Kubernetes. Just Unity Catalog, Delta tables, and a medallion architecture that runs end-to-end in under 20 minutes.

Let me show you how it works."

[SCREEN: Show final pipeline diagram or results dashboard]

---

## INTRO (0:20 - 1:00)

"Hey everyone—today I'm walking you through a production-grade data engineering project I built to analyze Netflix's entire catalog.

We're talking:
- **Bronze-Silver-Gold medallion architecture**
- **dbt transformations** running directly on Databricks
- **AI enrichment** using Groq's Llama models for mood detection and audience targeting
- **Strategic insights** generated entirely by AI

And the best part? Everything lives in **Unity Catalog**—no CSV exports, no local dashboards, no manual steps. This is a true Databricks-native pipeline.

By the end of this video, you'll see exactly how to combine modern data engineering with LLMs to build something portfolio-worthy.

Let's dive in."

[SCREEN: Show GitHub repo link in corner]

---

## PROBLEM STATEMENT (1:00 - 1:45)

"Here's what we're solving:

Netflix has 8,807 titles across 123 countries. The raw data is a single CSV with columns like title, director, cast, genre, release year, and country.

But that's just structured metadata. What if we want to know:
- What **mood** does each title convey? Dark? Uplifting? Tense?
- Who's the **target audience**? Families? Young adults? Critics?
- Which **country-mood-audience combinations** are underrepresented?

You can't answer those questions with SQL alone. You need AI.

So I built a pipeline that:
1. **Ingests** the raw CSV into Unity Catalog
2. **Transforms** it with dbt into clean staging and analytics tables
3. **Enriches** a sample with Groq's Llama 3.1 8B model for fast mood/audience detection
4. **Analyzes** trends with Llama 3 70B for strategic insights
5. **Saves everything** back to Delta tables for querying

Let's look at the architecture."

[SCREEN: Show architecture diagram]

---

## ARCHITECTURE OVERVIEW (1:45 - 3:00)

"This is a classic medallion architecture with an AI twist:

### Bronze Layer
Raw data lands here. The CSV gets ingested with PySpark into `netflix_bronze.raw_titles`. No transformations, just raw ingestion into Delta format.

### Silver Layer
This is where dbt takes over. We run a staging model that:
- Cleans column names
- Splits multi-country fields into a `primary_country`
- Handles nulls and type conversions
- Outputs to `netflix_silver.stg_titles`

### Gold Layer
Aggregate analytics tables:
- Content by year (movies vs TV shows over time)
- Country-level analysis (top producing countries)
- Genre distribution
- Rating distribution
- International growth trends

These are all materialized views powered by dbt, running on Databricks SQL compute.

### AI Enrichment Layer
Here's where it gets interesting. We take a 500-title sample and send it to **Groq's Llama 3.1 8B Instant** model. For each title, we ask:
- What's the mood?
- Who's the target audience?
- What are the themes?

Results go into `netflix_gold.ai_enriched_titles`.

### AI Insights Layer
Finally, we load ALL the Gold data and AI-enriched metadata into **Groq's Llama 3 70B** model and generate:
- Content strategy analysis
- Genre and audience intelligence
- Content gap analysis (which country-mood-audience combos are missing?)
- Executive summary

All saved to `netflix_ai.strategic_insights`.

Now let's see the actual code."

[SCREEN: Show full pipeline flow diagram]

---

## PIPELINE WALKTHROUGH (3:00 - 6:30)

### Bronze Ingestion (3:00 - 3:30)

"First, notebook 01: Bronze Ingestion.

[SCREEN: Show notebook 01_bronze_ingestion.py]

We read the CSV from a Unity Catalog volume—no DBFS mounting, no local files. Just:

```python
df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/Volumes/netflix/default/aidataset/netflix_titles.csv")
)
```

We write it to Delta:

```python
df.write.format("delta").mode("overwrite").saveAsTable("netflix.netflix_bronze.raw_titles")
```

8,807 rows ingested. Bronze layer complete."

---

### dbt Silver Transformations (3:30 - 4:30)

"Next, notebook 02: dbt Transform.

[SCREEN: Show notebook 02_dbt_transform.py]

This is where dbt comes in. We're using **dbt-databricks** to run transformations directly on Databricks SQL Warehouse.

Here's the cool part: dbt runs programmatically via the Python SDK. No separate dbt Cloud, no CLI commands in CI/CD. Everything is orchestrated from the notebook:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Run dbt models
w.command_execution.execute(
    cluster_id=None,
    language="sql",
    context_id=sql_warehouse_id,
    command="dbt run --models stg_titles"
)
```

The dbt model does:
- Column renaming (snake_case)
- Primary country extraction (splits 'United States, India' → 'United States')
- Null handling
- Type casting

Output: `netflix_silver.stg_titles`—clean, analysis-ready data.

One gotcha we hit: dbt wanted to create `netflix_silver_netflix_silver` schemas. We fixed it with a custom Jinja macro:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name }}
{%- endmacro %}
```

Now it respects `netflix_silver` exactly."

---

### Gold Analytics (4:30 - 5:00)

"Notebook 03: Gold Analytics.

[SCREEN: Show notebook 03_gold_analytics.py]

This is where we build business-friendly aggregations:

- **Content by Year**: Movies vs TV Shows over time
- **Country Analysis**: Top 20 producing countries with title counts
- **Genre Distribution**: Which genres dominate?
- **Rating Distribution**: How much is TV-MA vs TV-PG?
- **International Growth**: Year-over-year growth by region

All saved as Delta tables in `netflix_gold.*`.

These tables power dashboards, reports, and—most importantly—the AI analysis layer."

---

### AI Enrichment (5:00 - 6:00)

"Notebook 04: AI Enrichment.

[SCREEN: Show notebook 04_ai_enrichment.py]

This is where we bring in Groq AI.

Why Groq? Three reasons:
1. **Free tier** with generous limits
2. **Llama 3.1 8B Instant** is insanely fast (800+ tokens/sec)
3. Structured outputs—perfect for extracting JSON fields

Here's the prompt:

```python
prompt = f'''
Title: {title}
Type: {type_}
Genre: {genre}
Description: {description}

Extract:
- mood (one word: Dark, Uplifting, Tense, Lighthearted, Emotional, Mysterious, Inspiring, Funny)
- target_audience (one: families, young adults, critics, teens, children, mature audiences, international audiences, niche audiences)
- themes (comma-separated)

JSON only.
'''
```

We process 500 titles in batches of 10. Total time: ~5 minutes.

The result? A new column `mood` and `target_audience` on every title. This unlocks questions like:
- Which countries produce the most 'Dark' content?
- Are family-friendly titles growing or shrinking?
- Which mood-audience combos are underrepresented?

Saved to `netflix_gold.ai_enriched_titles`."

---

### AI Insights (6:00 - 6:30)

"Notebook 07: AI Insights.

[SCREEN: Show notebook 07_ai_insights.py]

Now we switch to **Llama 3 70B**—a much larger reasoning model.

We load ALL the Gold tables and AI-enriched data, dump them as JSON, and send 4 prompts:

1. **Content Strategy**: How did Netflix's content mix shift from 2015 to 2021?
2. **Genre & Audience Intelligence**: What does the mood and rating data reveal about Netflix's target demo?
3. **Content Gap Analysis**: Which country-mood-audience combos are missing?
4. **Executive Summary**: 5-sentence board-level summary

Each response is 300-800 tokens. The AI cites specific numbers from the data.

Example gap the AI found:

> 'India produces 923 titles, but only 2 are categorized as Lighthearted content for families. South Korea has zero Uplifting content targeting international audiences despite 274 total titles.'

That's actionable insight. Saved to `netflix_ai.strategic_insights`."

---

## TECHNICAL HIGHLIGHTS (6:30 - 8:00)

"Let's talk about the design decisions that make this work:

### 1. **Unity Catalog > DBFS**
Everything lives in Unity Catalog. No file paths, no `dbfs:/` mounting. Just:
- `netflix_bronze.*`
- `netflix_silver.*`
- `netflix_gold.*`
- `netflix_ai.*`

Fully governed, ACID-compliant, and queryable.

---

### 2. **dbt-databricks Integration**
We run dbt transformations programmatically via the Databricks SDK. This means:
- No external dbt Cloud subscription
- No CLI commands to maintain
- Native integration with SQL Warehouses

dbt generates the SQL, Databricks executes it, and Delta handles the versioning.

---

### 3. **Why 500 Titles for AI, Not 8,807?**
Two reasons:
1. **Cost**: Even with Groq's free tier, 8,807 titles × 4 prompts = 35,000+ API calls. Expensive.
2. **Demo Purpose**: This is a learning project. 500 titles is enough to show mood distribution, audience patterns, and strategic gaps.

If this were production, you'd:
- Run the full 8,807 (or use a *real* random sample)
- Cache embeddings in a vector database
- Use batch inference APIs

For a portfolio project? 500 titles proves the concept.

---

### 4. **Prompt Engineering > RAG**
We didn't use Retrieval-Augmented Generation (RAG) because:
- The dataset is small (8,807 titles × ~200 chars = 1.7M chars)
- It's structured tabular data, not unstructured documents
- We can fit entire aggregated tables in the LLM context window

RAG makes sense for:
- Millions of documents
- Semantic search over unstructured text
- When you need citation retrieval

Here? Direct prompt injection works perfectly.

---

### 5. **Two Models, Two Jobs**
- **Llama 3.1 8B Instant**: Fast, cheap, perfect for structured extraction (mood, audience)
- **Llama 3 70B**: Slow, expensive, but great for reasoning and strategic analysis

Don't use a 70B model for simple extraction. Don't use an 8B model for complex reasoning. Match the model to the task."

---

## ORCHESTRATION (8:00 - 8:45)

"How do you run this pipeline end-to-end?

Notebook 09: Run Pipeline.

[SCREEN: Show notebook 09_run_pipeline.py]

This is a simple orchestrator using `dbutils.notebook.run()`:

```python
pipeline = [
    ("01 Bronze Ingestion",   "01_bronze_ingestion",    {}, 300),
    ("02 dbt Transform",      "02_dbt_transform",       {...}, 600),
    ("03 Gold Analytics",     "03_gold_analytics",      {...}, 600),
    ("04 AI Enrichment",      "04_ai_enrichment",       {"GROQ_API_KEY": key}, 900),
    ("07 AI Insights",        "07_ai_insights",         {"GROQ_API_KEY": key}, 600),
]

for step_name, notebook, params, timeout in pipeline:
    dbutils.notebook.run(f"./{notebook}", timeout, params)
```

Total runtime: ~15-20 minutes.

No Airflow. No Kubernetes. Just Databricks notebooks calling notebooks.

For production, you'd convert this to a **Databricks Job** with dependent tasks and email alerts on failure. But for a demo? This works perfectly."

---

## RESULTS & OUTCOMES (8:45 - 9:30)

"So what did we learn?

### From the Data:
- Netflix shifted from 70% movies in 2015 to 60% by 2021
- International content exploded: 45% of titles in 2015 → 73% by 2020
- Top mood: **Dark** (178 titles), followed by Lighthearted (92) and Uplifting (81)
- Top audience: **Young Adults** (142 titles), then Families (89) and Mature Audiences (76)

### Strategic Gaps the AI Found:
1. India has 923 titles but only 2 Lighthearted family titles
2. South Korea has 274 titles but zero Uplifting content for international audiences
3. Japan has 245 titles but minimal Inspiring content for young adults

These are real business insights. A content strategist could act on this.

### Technical Wins:
- Full medallion architecture in Unity Catalog
- dbt running natively on Databricks
- AI enrichment with structured outputs
- Strategic reasoning from a 70B model
- All reproducible, version-controlled, and documented

This is what modern data engineering looks like."

---

## LESSONS LEARNED (9:30 - 10:15)

"Three things I'd do differently:

### 1. Use Random Sampling
Right now, `.limit(500)` takes the first 500 rows—not a random sample. That introduces bias.

Fix: Use `.orderBy(F.rand()).limit(500)` or stratified sampling by country/year.

### 2. Add Statistical Sampling Documentation
The README now says 'demo subset,' but originally it said 'sample.' Sampling implies statistical rigor. Be precise about what you're doing.

### 3. Improve Gap Analysis Logic
The AI tries to find zero-count country-mood-audience combos by looking at a top-100 table. But zero-count rows don't appear in aggregations!

Fix: Generate a full cross-join of all countries × moods × audiences, left-join with actual counts, filter for nulls. Then the AI can truly spot gaps.

These are portfolio-project gotchas. In production, you'd catch them in code review."

---

## CALL TO ACTION (10:15 - 10:45)

"Alright, let's wrap up.

If you want to build this yourself:
1. **Clone the repo** (link in the description)
2. Upload the Netflix CSV to a Unity Catalog volume
3. Get a free Groq API key
4. Run `09_run_pipeline.py`

Total cost? **$0**. Groq's free tier covers everything.

If you found this helpful:
- **Star the repo** on GitHub
- **Drop a comment** with questions or improvements
- **Subscribe** for more data engineering + AI projects

Next video: I'm building a **real-time streaming pipeline** with Spark Structured Streaming and Delta Live Tables. We're ingesting live IoT data, applying ML models on the fly, and surfacing anomalies in near-real-time.

Thanks for watching. See you in the next one."

[SCREEN: Show GitHub link, subscribe button, next video thumbnail]

---

## B-ROLL SUGGESTIONS

Throughout the video, overlay:
- **Architecture diagrams** during explanation sections
- **Live notebook code** when discussing specific transformations
- **Terminal output** showing pipeline execution
- **Delta table previews** in Databricks UI
- **AI prompt/response examples** during enrichment sections
- **GitHub repo README** during call-to-action

Keep it visual. Show, don't just tell.

---

## VIDEO LENGTH
Target: **10-11 minutes**

Pacing:
- Hook: 20 sec
- Intro: 40 sec
- Problem: 45 sec
- Architecture: 1:15
- Pipeline walkthrough: 3:30 (detailed)
- Technical highlights: 1:30
- Orchestration: 45 sec
- Results: 45 sec
- Lessons: 45 sec
- CTA: 30 sec

---

## THUMBNAIL IDEAS
1. Split screen: Code on left, "AI-Enriched Netflix Analytics" text on right
2. Pipeline diagram with "Bronze → Silver → Gold → AI" flow
3. Your face + laptop with "8,807 Netflix Titles Analyzed" overlay
4. Dark background, Databricks + Groq logos, bold text: "End-to-End Data + AI Pipeline"

---

## VIDEO TAGS
databricks, data engineering, dbt, groq ai, llama, netflix data, medallion architecture, unity catalog, delta lake, pyspark, sql, ai enrichment, data pipeline, portfolio project, etl, data transformation, machine learning, llm, groq, ai data analysis

---

## DESCRIPTION TEMPLATE

```
🎬 In this video, I walk through building a production-grade data pipeline that analyzes 8,807 Netflix titles using Databricks, dbt, and Groq AI.

Pipeline Stages:
✅ Bronze: Raw CSV ingestion into Unity Catalog
✅ Silver: dbt transformations for clean staging tables
✅ Gold: Analytics aggregations (genre, country, trends)
✅ AI Enrichment: Mood & audience detection with Llama 3.1 8B
✅ AI Insights: Strategic analysis with Llama 3 70B

Tech Stack:
• Databricks Lakehouse (Unity Catalog + Delta Lake)
• dbt-databricks for SQL transformations
• Groq AI (Llama models) for enrichment & reasoning
• PySpark for data processing
• Python for orchestration

📂 GitHub Repo: [YOUR_REPO_URL]
🔑 Free Groq API Key: https://console.groq.com

Timestamps:
0:00 Hook
0:20 Intro
1:00 Problem Statement
1:45 Architecture Overview
3:00 Bronze Ingestion
3:30 dbt Silver Layer
4:30 Gold Analytics
5:00 AI Enrichment
6:00 AI Insights
6:30 Technical Highlights
8:00 Orchestration
8:45 Results & Outcomes
9:30 Lessons Learned
10:15 Call to Action

Questions? Drop them in the comments! 👇
```

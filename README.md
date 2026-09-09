# Netflix Content Analytics Platform 🎬

An end-to-end data engineering and AI analytics platform built on Databricks, analyzing 8,807+ Netflix titles with mood detection, audience targeting, and strategic insights powered by Groq AI.

---

## 🏗️ Architecture Overview

```
Raw CSV → Bronze (Unity Catalog) → Silver (dbt Transformations) → Gold (Analytics) → AI Enrichment → Dashboards
```

**Key Technologies:**
- **Databricks Lakehouse**: Unity Catalog, Delta Lake, Lakeflow Pipelines
- **dbt Core**: Data transformations with Jinja templating
- **Groq AI** (Llama 3.3 70B): Mood detection, audience targeting, strategic insights
- **Streamlit**: Interactive dashboards
- **Python**: PySpark, pandas, requests

---

## 📊 What This Project Does

### 1. **Data Engineering Pipeline**
- Ingests Netflix catalog CSV into Unity Catalog (Bronze layer)
- Transforms data with dbt (Silver: staging + fact tables, Gold: analytics aggregations)
- Handles duplicates, missing values, type conversions

### 2. **AI-Powered Enrichment**
- **Mood Detection**: Analyzes 500 sample titles to extract emotional tone (exciting, dark, heartwarming, etc.)
- **Audience Targeting**: Identifies demographics (families, young adults, critics, etc.)
- **Strategic Insights**: Gap analysis by country to recommend content strategies

### 3. **Interactive Dashboards**
- Real-time KPI visualization (8,807 titles across Bronze/Silver/Gold layers)
- AI enrichment results browser
- Country-level content distribution heatmaps

---

## 🗂️ Project Structure

```
netflix-databricks-analytics/
├── data/
│   └── netflix_titles.csv          # Raw dataset (8,807 titles)
├── notebooks/
│   ├── 00_setup.py                 # Environment setup, library installs
│   ├── 01_bronze_ingestion.py      # CSV → Unity Catalog bronze layer
│   ├── 02_dbt_silver_transform.py  # dbt Silver transformations
│   ├── 03_dbt_gold_analytics.py    # dbt Gold aggregations
│   ├── 04_ai_enrichment.py         # Groq AI mood/audience detection (500 samples)
│   ├── 05_ai_text_to_sql.py        # Natural language → SQL query generation
│   ├── 06_ai_rag.py                # Prompt-based catalog analysis (NOT true RAG)
│   ├── 07_ai_insights.py           # Strategic gap analysis by country
│   └── 09_run_pipeline.py          # Master orchestration pipeline
├── streamlit/
│   └── app.py                      # Streamlit dashboard application
└── requirements.txt                # Python dependencies
```

---

## 🚀 Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Groq API key ([Get one free](https://console.groq.com))
- Catalog/schema permissions (CREATE TABLE, SELECT, MODIFY)

### Setup Steps

1. **Clone Repository**
   ```bash
   git clone https://github.com/sweta2503/netflix-databricks-analytics.git
   cd netflix-databricks-analytics
   ```

2. **Upload to Databricks Workspace**
   - Import the `netflix-databricks-analytics` folder into `/Workspace/Users/<your-email>/`
   - Ensure `data/netflix_titles.csv` is present

3. **Configure Environment**
   - Open `00_setup.py`
   - Set your `GROQ_API_KEY` secret:
     ```python
     dbutils.secrets.put(scope="your-scope", key="groq_api_key", string_value="gsk_...")
     ```
   - Update catalog/schema names if needed (default: `netflix.netflix_bronze`, etc.)

4. **Run Master Pipeline**
   - Open `09_run_pipeline.py`
   - Execute all cells to run the full pipeline (Bronze → Silver → Gold → AI)
   - Total runtime: ~15-20 minutes

5. **View Dashboard**
   - Install Streamlit: `pip install streamlit`
   - Run: `streamlit run streamlit/app.py`
   - Access at `http://localhost:8501`

---

## 🧠 Design Decisions

### Why Prompt-Based Analysis Instead of True RAG?

**What is "True RAG"?**  
Retrieval-Augmented Generation (RAG) embeds text into vectors, stores them in a vector database (e.g., FAISS, Pinecone), and retrieves semantically similar chunks at query time to ground LLM responses.

**Why We Didn't Use It Here:**

1. **Data Type**: Netflix catalog is **structured tabular data** (title, genre, country, rating). RAG is designed for **unstructured text** (documents, articles) where semantic search adds value.

2. **Dataset Size**: 8,807 titles with ~200 characters each fit comfortably in LLM context windows. No need for retrieval when you can pass the data directly.

3. **Query Patterns**: Our use cases are:
   - **Aggregations** (mood distribution by country) → SQL is faster and more accurate
   - **Filtering** (titles in a genre) → Direct table queries beat vector similarity
   - **Strategic insights** → Need GROUP BY results, not individual title retrieval

4. **Cost/Complexity**: True RAG requires:
   - Embedding generation (~$5-20 for 8,807 titles)
   - Vector index maintenance (Databricks Vector Search or external service)
   - Ongoing compute for similarity queries  
   For a demo project, this overhead isn't justified.

**When Would RAG Be Justified?**
- **Unstructured content**: Plot summaries, reviews, transcripts (semantic search needed)
- **Large corpus**: Millions of documents where context window limits apply
- **Exploratory Q&A**: "What shows deal with climate change?" (needs semantic retrieval)

**Our Approach:**  
- **Notebook 06**: Prompt-based catalog analysis (pass 500 sample records to LLM)
- **Notebook 07**: SQL aggregations + LLM analysis (combine structured queries with AI reasoning)

This is simpler, faster, and aligns better with the structured nature of the dataset.

---

## 📈 Data Pipeline Details

### Bronze Layer (`netflix.netflix_bronze.raw_titles`)
- Direct CSV ingestion with minimal processing
- Schema: `show_id`, `type`, `title`, `director`, `cast`, `country`, `date_added`, `release_year`, `rating`, `duration`, `listed_in`, `description`
- Row count: 8,807

### Silver Layer (`netflix.netflix_silver.*`)
- **`stg_titles`**: Cleaned staging table (duplicate removal, type casting, NULL handling)
- **`fact_titles`**: Fact table with enhanced columns (`content_type_flag`, `country_split`)
- **`dim_content_attributes`**: Dimension table for genre/rating lookups

### Gold Layer (`netflix.netflix_gold.*`)
- **`country_content_summary`**: Aggregated metrics by country
- **`genre_trends`**: Genre distribution and popularity scores

### AI Enrichment Layer (`netflix.netflix_gold.enriched_titles`)
- **Sample size**: 500 titles (cost/time efficiency)
- **Columns added**: `mood`, `primary_audience`
- **Note**: Dashboard KPIs query `netflix_silver.stg_titles` (full 8,807 rows), not `enriched_titles`

---

## 🎯 Key Features

### 1. **Scalable dbt Transformations**
- Incremental models for efficient updates
- Jinja macros for reusable logic
- Custom schema naming (avoids `netflix_silver_netflix_silver` concatenation)

### 2. **Groq AI Integration**
- **Model**: Llama 3.3 70B Versatile (fast, cost-effective)
- **Rate limiting**: 30 requests/minute (built-in retry logic)
- **Structured outputs**: JSON parsing for mood/audience extraction

### 3. **Cost Optimization**
- AI enrichment limited to 500 samples (~$0.50/run vs. $8.00 for full dataset)
- Incremental dbt models (only process changed data)
- Delta Lake time travel (rollback without re-processing)

---

## 📊 Sample Insights

**Top Countries by Content Volume:**
1. United States: 3,689 titles
2. India: 1,046 titles
3. United Kingdom: 806 titles

**Mood Distribution (500-sample analysis):**
- Exciting: 32%
- Heartwarming: 24%
- Dark: 18%
- Suspenseful: 15%
- Humorous: 11%

**Strategic Gap Example:**
> "India has strong comedy/drama presence but lacks thriller content compared to US. Recommend licensing suspenseful series targeting young adults."

---

## 🛠️ Troubleshooting

### Common Issues

1. **`netflix_silver_netflix_silver` schema error**
   - **Fix**: Ensure `dbt/macros/generate_schema_name.sql` exists with custom schema logic
   - Verify `profiles.yml` has `schema: default`

2. **Groq API rate limit errors**
   - **Fix**: Reduce batch size in `04_ai_enrichment.py` (default: 10 titles/batch)
   - Add `time.sleep(2)` between batches

3. **Dashboard shows wrong KPI counts**
   - **Fix**: Ensure dashboard queries point to `netflix.netflix_silver.stg_titles` (8,807 rows), not `netflix.netflix_gold.enriched_titles` (500 rows)

4. **Pipeline fails at dbt step**
   - **Fix**: Run `dbt debug` to check profiles/connection
   - Ensure catalog/schema exist and you have CREATE TABLE permissions

---

## 🎓 Learning Outcomes

**Data Engineering:**
- Unity Catalog governance (catalogs, schemas, tables)
- Delta Lake ACID transactions
- dbt medallion architecture (Bronze → Silver → Gold)

**AI/LLM Integration:**
- Prompt engineering for structured outputs
- Rate limiting and retry strategies
- Cost vs. accuracy trade-offs (sampling strategies)

**Databricks Platform:**
- Notebook orchestration
- Lakeflow Pipeline design
- Streamlit dashboard deployment

---

## 🔮 Future Enhancements

- [ ] Implement true RAG with Databricks Vector Search (learning exercise)
- [ ] Add Delta Live Tables (DLT) for streaming ingestion
- [ ] MLflow tracking for AI enrichment quality metrics
- [ ] Unity Catalog governance tags (PII, quality tier)
- [ ] Scheduled job for daily catalog updates
- [ ] Multi-language support (analyze non-English titles)

---

## 📜 License

MIT License - Feel free to use for learning/commercial projects.

---

## 🙏 Acknowledgments

- Dataset: [Kaggle Netflix Shows Dataset](https://www.kaggle.com/datasets/shivamb/netflix-shows)
- Groq for fast, affordable LLM inference
- Databricks Community Edition for free learning environment

---

## 📧 Contact

Questions? Open an issue or reach out!

**GitHub**: [sweta2503/netflix-databricks-analytics](https://github.com/sweta2503/netflix-databricks-analytics)

# Netflix Content Analytics Platform 🎬

An end-to-end data engineering and AI analytics platform built on Databricks, analyzing 8,807+ Netflix titles with mood detection, audience targeting, and strategic insights powered by Groq AI.

---

## 🏗️ Architecture Overview

```
Raw CSV → Bronze (Unity Catalog) → Silver (dbt Transformations) → Gold (Analytics) → AI Enrichment
         ↓                ↓                                          ↓                  ↓
   Delta Tables    Delta Tables                              Delta Tables       Delta Tables (AI-enriched)
```

**Key Technologies:**
- **Databricks Lakehouse**: Unity Catalog, Delta Lake, Lakeflow Pipelines
- **dbt Core**: Data transformations with Jinja templating
- **Groq AI** (Llama 3.1 8B for enrichment, Llama 3 70B for reasoning): Mood detection, audience targeting, strategic insights
- **Python**: PySpark, pandas, requests

---

## 📊 What This Project Does

### 1. **Data Engineering Pipeline**
- Ingests Netflix catalog CSV into Unity Catalog (Bronze layer)
- Transforms data with dbt (Silver: staging + fact tables, Gold: analytics aggregations)
- Handles duplicates, missing values, type conversions

### 2. **AI-Powered Enrichment**
- **Mood Detection**: Analyzes 500-title demo subset to extract emotional tone (Dark, Uplifting, Tense, Lighthearted, Emotional, Mysterious, Inspiring, Funny)
- **Audience Targeting**: Identifies demographics (families, young adults, critics, etc.)
- **Strategic Insights**: Gap analysis by country to recommend content strategies

### 3. **Analytics Tables**
- Unity Catalog Delta tables with full Netflix catalog (8,807 titles)
- AI-enriched metadata (mood, audience) for 500-title demo subset
- Ready for downstream dashboards or BI tools

---

## 🗂️ Project Structure

```
netflix-databricks-analytics/
├── notebooks/
│   ├── 00_setup.py                 # Environment setup, library installs
│   ├── 01_bronze_ingestion.py      # Reads /Volumes/netflix/default/aidataset/netflix_titles.csv → Unity Catalog bronze
│   ├── 02_dbt_transform.py         # dbt Silver transformations (stg_titles)
│   ├── 03_gold_analytics.py        # dbt Gold aggregations
│   ├── 04_ai_enrichment.py         # Groq AI mood/audience detection (llama-3.1-8b-instant, 500-title subset)
│   ├── 05_ai_text_to_sql.py        # Natural language → SQL (llama3-70b-8192)
│   ├── 06_ai_rag.py                # Prompt-based catalog analysis (llama3-70b-8192, NOT true RAG)
│   ├── 07_ai_insights.py           # Strategic gap analysis (llama3-70b-8192)
│   └── 09_run_pipeline.py          # Master orchestration pipeline
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

2. **Prepare Data**
   - Upload `netflix_titles.csv` to `/Volumes/netflix/default/aidataset/netflix_titles.csv`
   - Or adjust the path in `01_bronze_ingestion.py` (line 2: `RAW_CSV = "/Volumes/...")`

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

5. **Query Results**
   - All outputs are stored as Delta tables in Unity Catalog:
     - `netflix_bronze.raw_titles` (8,807 titles)
     - `netflix_silver.stg_titles` (cleaned/transformed)
     - `netflix_gold.*` (analytics aggregations)
     - `netflix_gold.ai_enriched_titles` (500 titles with mood/audience)
   - Query directly in Databricks SQL Editor or build dashboards with AI/BI

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
- Direct CSV ingestion with minimal processing (all columns read as strings)
- Schema: `show_id`, `type`, `title`, `director`, `cast`, `country`, `date_added`, `release_year`, `rating`, `duration`, `listed_in`, `description`
- Row count: 8,807

### Silver Layer (`netflix.netflix_silver.*`)
- **`stg_titles`**: Cleaned staging table with:
  - Type casting (`release_year` → INT, `date_added` → TIMESTAMP)
  - NULL handling (`nullif(trim())` for empty strings)
  - Derived columns (`primary_country`, `added_year`, `duration_value`, `duration_unit`, `genres` array, `is_movie` flag)
  - Note: No duplicate removal logic (relies on source data quality)

### Gold Layer (`netflix.netflix_gold.*`)
- **`country_content_summary`**: Aggregated metrics by country
- **`genre_trends`**: Genre distribution and popularity scores

### AI Enrichment Layer (`netflix.netflix_gold.ai_enriched_titles`)
- **Sample size**: 500-title demo subset (`.limit(500)` NOT random sampling - sequential for reproducibility)
- **Columns added**: `mood` (Dark, Uplifting, Tense, Lighthearted, Emotional, Mysterious, Inspiring, Funny), `themes`, `target_audience`, `content_tags`, `decade_feel`
- **Full catalog available**: `netflix_silver.stg_titles` contains all 8,807 titles without AI enrichment

---

## 🎯 Key Features

### 1. **Scalable dbt Transformations**
- Incremental models for efficient updates
- Jinja macros for reusable logic
- Custom schema naming (avoids `netflix_silver_netflix_silver` concatenation)

### 2. **Groq AI Integration**
- **Models**: 
  - **Notebook 04**: `llama-3.1-8b-instant` (high-volume enrichment, 500 titles)
  - **Notebooks 05-07**: `llama3-70b-8192` (text-to-SQL, RAG, strategic insights - better reasoning)
- **Rate limiting**: Built-in exponential backoff retry logic
- **Structured outputs**: JSON parsing for mood/audience extraction (no temperature config - uses model defaults)

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

**Mood Distribution (500-title demo subset):**
- Dark, Uplifting, Tense, Lighthearted, Emotional, Mysterious, Inspiring, Funny
- (Exact distribution varies by sample - run `04_ai_enrichment.py` to generate current distribution)

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

3. **Pipeline syntax error: `timeout=300` invalid**
   - **Root cause**: Tuple unpacking expects positional args, not keyword args
   - **Fix**: Changed `(name, notebook, params, timeout=300)` → `(name, notebook, params, 300)` in `09_run_pipeline.py`

4. **dbt schema naming concatenation (`netflix_silver_netflix_silver`)**
   - **Fix**: Custom macro `macros/get_custom_schema.sql` prevents concatenation. Profile schema set to `default`, custom schemas use `netflix_silver` and `netflix_gold` directly.

5. **Pipeline fails at dbt step**
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
- Unity Catalog table management

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

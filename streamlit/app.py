"""
Netflix Analytics Dashboard — reads exported CSVs from Databricks pipeline.
Run: streamlit run streamlit/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(
    page_title="Netflix Content Intelligence",
    page_icon="🎬",
    layout="wide",
)

DATA_DIR = Path(__file__).parent / "data"

# ── Load data ─────────────────────────────────────────────────────────────────

@st.cache_data
def load(filename):
    path = DATA_DIR / filename
    if not path.exists():
        return None
    return pd.read_csv(path)

content_by_year  = load("content_by_year.csv")
genre_dist       = load("genre_distribution.csv")
country_analysis = load("country_analysis.csv")
rating_dist      = load("rating_distribution.csv")
top_directors    = load("top_directors.csv")
intl_growth      = load("international_growth.csv")
enriched         = load("enriched_titles.csv")
insights         = load("strategic_insights.csv")

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("""
<h1 style='color:#E50914; font-family:Georgia; margin-bottom:0'>
  🎬 Netflix Content Intelligence
</h1>
<p style='color:#888; margin-top:4px; font-size:14px'>
  End-to-End AI Data Engineering Pipeline · Databricks + Delta Lake + Groq (Llama)
</p>
<hr style='border-color:#333; margin: 12px 0 24px'>
""", unsafe_allow_html=True)

# ── KPI row ───────────────────────────────────────────────────────────────────

if enriched is not None:
    total    = len(enriched)
    movies   = len(enriched[enriched["type"] == "Movie"])
    shows    = total - movies
    ctries   = enriched["primary_country"].nunique()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Titles",  f"{total:,}")
    k2.metric("Movies",        f"{movies:,}", f"{round(movies/total*100,1)}%")
    k3.metric("TV Shows",      f"{shows:,}",  f"{round(shows/total*100,1)}%")
    k4.metric("Countries",     f"{ctries}")

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Content Trends", "🌍 Geography", "🎭 Genres & Mood", "🤖 AI Insights", "🔍 Content Search"
])

# ── Tab 1: Content Trends ─────────────────────────────────────────────────────

with tab1:
    st.subheader("Content Added by Year")
    if content_by_year is not None:
        fig = px.bar(
            content_by_year, x="added_year", y="titles_count", color="type",
            color_discrete_map={"Movie": "#E50914", "TV Show": "#564d4d"},
            barmode="group", labels={"added_year": "Year", "titles_count": "Titles Added", "type": "Type"},
        )
        fig.update_layout(plot_bgcolor="#0d0d0d", paper_bgcolor="#0d0d0d",
                          font_color="white", legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("International vs US Content Growth")
    if intl_growth is not None:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=intl_growth["added_year"], y=intl_growth["international_pct"],
            mode="lines+markers", name="International %",
            line=dict(color="#E50914", width=3), marker=dict(size=8),
        ))
        fig2.update_layout(
            plot_bgcolor="#0d0d0d", paper_bgcolor="#0d0d0d",
            font_color="white", yaxis_title="International Content %",
            xaxis_title="Year", showlegend=False,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ── Tab 2: Geography ──────────────────────────────────────────────────────────

with tab2:
    st.subheader("Top Content-Producing Countries")
    if country_analysis is not None:
        top20 = country_analysis.head(20)
        fig = px.bar(
            top20, x="total_titles", y="country", orientation="h",
            color="movie_pct",
            color_continuous_scale=["#564d4d", "#E50914"],
            labels={"total_titles": "Total Titles", "country": "", "movie_pct": "Movie %"},
        )
        fig.update_layout(plot_bgcolor="#0d0d0d", paper_bgcolor="#0d0d0d",
                          font_color="white", yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 3: Genres & Mood ──────────────────────────────────────────────────────

with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Genres")
        if genre_dist is not None:
            top_genres = (
                genre_dist.groupby("genre")["title_count"]
                .sum().reset_index()
                .sort_values("title_count", ascending=False)
                .head(12)
            )
            fig = px.bar(
                top_genres, x="title_count", y="genre", orientation="h",
                color_discrete_sequence=["#E50914"],
            )
            fig.update_layout(plot_bgcolor="#0d0d0d", paper_bgcolor="#0d0d0d",
                              font_color="white", yaxis=dict(autorange="reversed"),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Mood Distribution (Groq AI)")
        if enriched is not None and "mood" in enriched.columns:
            mood_counts = enriched["mood"].value_counts().reset_index()
            mood_counts.columns = ["mood", "count"]
            fig = px.pie(
                mood_counts, names="mood", values="count",
                color_discrete_sequence=px.colors.sequential.Reds_r,
                hole=0.4,
            )
            fig.update_layout(plot_bgcolor="#0d0d0d", paper_bgcolor="#0d0d0d",
                              font_color="white")
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Target Audience Split (Groq AI Enrichment)")
    if enriched is not None and "target_audience" in enriched.columns:
        aud = enriched.groupby(["target_audience", "type"]).size().reset_index(name="count")
        fig = px.bar(
            aud, x="target_audience", y="count", color="type",
            color_discrete_map={"Movie": "#E50914", "TV Show": "#564d4d"},
            barmode="group",
        )
        fig.update_layout(plot_bgcolor="#0d0d0d", paper_bgcolor="#0d0d0d",
                          font_color="white", legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 4: AI Insights ────────────────────────────────────────────────────────

with tab4:
    st.subheader("Strategic Insights — Generated by Groq AI (Llama)")

    if insights is not None:
        insight_map = {
            "content_strategy":  ("📊 Content Strategy Analysis", "content_strategy"),
            "genre_audience":    ("🎭 Genre & Audience Intelligence", "genre_audience"),
            "content_gap":       ("🔍 Content Gap Analysis", "content_gap"),
            "executive_summary": ("📋 Executive Summary", "executive_summary"),
        }
        for key, (label, itype) in insight_map.items():
            row = insights[insights["insight_type"] == itype]
            if not row.empty:
                with st.expander(label, expanded=(key == "executive_summary")):
                    st.markdown(row.iloc[0]["insight_text"])
    else:
        st.info("Run notebook 07_ai_insights and export the results first.")

# ── Tab 5: Content Search ─────────────────────────────────────────────────────

with tab5:
    st.subheader("🔍 Find What to Watch")
    st.caption("Filter the enriched catalog by mood, audience, and type.")

    if enriched is not None and "mood" in enriched.columns:
        col1, col2, col3 = st.columns(3)
        moods     = ["All"] + sorted(enriched["mood"].dropna().unique().tolist())
        audiences = ["All"] + sorted(enriched["target_audience"].dropna().unique().tolist())
        types     = ["All", "Movie", "TV Show"]

        sel_mood     = col1.selectbox("Mood",     moods)
        sel_audience = col2.selectbox("Audience", audiences)
        sel_type     = col3.selectbox("Type",     types)

        df_filtered = enriched.copy()
        if sel_mood     != "All": df_filtered = df_filtered[df_filtered["mood"] == sel_mood]
        if sel_audience != "All": df_filtered = df_filtered[df_filtered["target_audience"] == sel_audience]
        if sel_type     != "All": df_filtered = df_filtered[df_filtered["type"] == sel_type]

        st.markdown(f"**{len(df_filtered):,} titles match**")

        display_cols = ["title", "type", "mood", "target_audience", "decade_feel", "primary_country", "release_year", "description"]
        available = [c for c in display_cols if c in df_filtered.columns]
        st.dataframe(df_filtered[available].head(50), use_container_width=True, hide_index=True)
    else:
        st.info("Run notebooks 04_ai_enrichment and 08_export first.")

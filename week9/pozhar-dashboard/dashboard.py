import streamlit as st
import pandas as pd
import sqlite3
import json
import plotly.express as px
import subprocess
import sys
import re

st.set_page_config(page_title="Data Engineering Pipeline Dashboard", layout="wide")

st.title("🔥 Data Engineering Pipeline Dashboard")
st.caption("Built by Pozhar — Weeks 4-8 combined into one live application")

section = st.sidebar.radio("Navigate to:", [
    "🏠 Overview",
    "📥 Week 4 — Crypto ETL",
    "✅ Week 5 — Data Quality",
    "📸 Week 6 — CDC Tracker",
    "⭐ Week 7-8 — Star Schema"
])

if section == "🏠 Overview":
    st.header("Pipeline Overview")
    st.markdown("""
    This dashboard combines 5 weeks of data engineering work into one live application:
    
    1. **Extract & Load** — Pull live crypto data from an API (Week 4)
    2. **Validate** — Check data quality automatically (Week 5)
    3. **Track Changes** — Monitor database changes in real-time (Week 6)
    4. **Analyze** — Query a star schema data warehouse (Week 7-8)
    
    Use the sidebar to explore each stage of the pipeline.
    """)

elif section == "📥 Week 4 — Crypto ETL":
    st.header("Crypto ETL Pipeline")
    st.markdown("Extracts live cryptocurrency prices, transforms and loads them into SQLite.")

    conn = sqlite3.connect("crypto.db")
    df = pd.read_sql("SELECT * FROM coin_prices ORDER BY ingested_at DESC LIMIT 250", conn)
    conn.close()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Snapshots", len(df))
    col2.metric("Unique Coins", df['id'].nunique())
    col3.metric("Last Updated", df['ingested_at'].max())

    st.subheader("Top 10 Coins by Market Cap")
    top10 = df.sort_values('market_cap', ascending=False).head(10)
    fig = px.bar(top10, x='name', y='market_cap', color='name', title="Top 10 by Market Cap")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw Data")
    st.dataframe(df.head(20))

elif section == "✅ Week 5 — Data Quality":
    st.header("Data Quality Checks")
    st.markdown("Runs YAML-defined validation rules against a dataset.")

    tab1, tab2 = st.tabs(["📁 Upload Your Own CSV", "🪙 Crypto Dataset (Built-in)"])

    with tab1:
        st.subheader("Upload a CSV to validate")
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            st.success(f"Loaded {len(df_upload):,} rows, {len(df_upload.columns)} columns")
            st.dataframe(df_upload.head(10))

            st.subheader("Configure Checks")
            selected_col = st.selectbox("Select a column to check", df_upload.columns)
            check_type = st.selectbox("Check type", ["not_null", "unique", "range", "regex_match"])

            run_check = st.button("Run Check")

            if run_check:
                col_data = df_upload[selected_col]
                if check_type == "not_null":
                    failures = col_data.isnull().sum()
                    st.write(f"**Result:** {failures} null values found")
                    if failures == 0:
                        st.success("✓ Passed — no nulls found")
                    else:
                        st.error(f"✗ Failed — {failures} nulls found")

                elif check_type == "unique":
                    failures = col_data.duplicated().sum()
                    st.write(f"**Result:** {failures} duplicate values found")
                    if failures == 0:
                        st.success("✓ Passed — all values unique")
                    else:
                        st.error(f"✗ Failed — {failures} duplicates found")

                elif check_type == "range":
                    min_val = st.number_input("Min value", value=0.0)
                    max_val = st.number_input("Max value", value=1000000.0)
                    try:
                        numeric_col = pd.to_numeric(col_data, errors='coerce')
                        failures = ((numeric_col < min_val) | (numeric_col > max_val)).sum()
                        st.write(f"**Result:** {failures} values out of range")
                        if failures == 0:
                            st.success("✓ Passed — all values in range")
                        else:
                            st.error(f"✗ Failed — {failures} out of range")
                    except Exception as e:
                        st.error(f"Could not run range check: {e}")

                elif check_type == "regex_match":
                    pattern = st.text_input("Regex pattern", value="^[a-z0-9]+$")
                    try:
                        failures = (~col_data.astype(str).str.match(pattern)).sum()
                        st.write(f"**Result:** {failures} values failed the pattern")
                        if failures == 0:
                            st.success("✓ Passed — all values match pattern")
                        else:
                            st.error(f"✗ Failed — {failures} did not match")
                            sample = col_data[~col_data.astype(str).str.match(pattern)].head(5).tolist()
                            st.write(f"Sample failing values: {sample}")
                    except Exception as e:
                        st.error(f"Could not run regex check: {e}")
        else:
            st.info("Upload a CSV file above to run data quality checks on your own data")

    with tab2:
        st.markdown("Runs YAML-defined validation rules against the built-in crypto dataset.")

        if st.button("Run Data Quality Checks", key="crypto_dq"):
            result = subprocess.run([sys.executable, "dq.py", "coin_prices.yml"], capture_output=True, text=True)
            st.code(result.stdout, language="text")

        st.subheader("Known Issue Found")
        st.warning("🐛 Bug found: The `symbol` column contains Chinese characters (e.g. 币安人生) that fail the regex pattern `^[a-z0-9]+$`. This was caught automatically by the data quality checker.")

elif section == "📸 Week 6 — CDC Tracker":
    st.header("Change Data Capture (CDC) Activity")
    st.markdown("Real-time log of INSERT, UPDATE and DELETE operations on the customers table.")

    events = []
    with open("events.jsonl") as f:
        for line in f:
            events.append(json.loads(line))

    df_events = pd.DataFrame(events)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events", len(df_events))
    col2.metric("Inserts", len(df_events[df_events['op'] == 'INSERT']))
    col3.metric("Updates/Deletes", len(df_events[df_events['op'].isin(['UPDATE', 'DELETE'])]))

    st.subheader("Event Type Breakdown")
    op_counts = df_events['op'].value_counts().reset_index()
    op_counts.columns = ['Operation', 'Count']
    fig = px.pie(op_counts, names='Operation', values='Count', title="CDC Operations")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Recent Events")
    st.dataframe(df_events.tail(10))

elif section == "⭐ Week 7-8 — Star Schema":
    st.header("GitHub Events Star Schema")
    st.markdown("Analytics on 836,573 real GitHub events loaded into a dimensional model.")

    st.info("📊 Showing cached results from local Postgres analysis")

    event_type_data = pd.DataFrame({
        'event_type_name': ['PushEvent', 'CreateEvent', 'PullRequestEvent', 'IssueCommentEvent', 
                             'WatchEvent', 'DeleteEvent', 'PullRequestReviewEvent', 'IssuesEvent',
                             'PullRequestReviewCommentEvent', 'ForkEvent'],
        'count': [200257, 26381, 15886, 10144, 8669, 6747, 5996, 3666, 3482, 2083]
    })

    st.subheader("Event Types Distribution")
    fig = px.bar(event_type_data, x='event_type_name', y='count', title="GitHub Event Types (836,573 total events)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 10 Actors by Issues Created")
    top_actors = pd.DataFrame({
        'login': ['github-actions[bot]', 'kinjalsoftnoesis', 'poornima-krishnasamy', 'SAPDocs', 
                   'linkspreed', 'juzeppej0stko', 'zspz', 'lisabenedetti', 'maxokon', 'softvision-raul-bucata'],
        'issues_created': [227, 74, 65, 64, 43, 37, 35, 32, 23, 23]
    })
    fig2 = px.bar(top_actors, x='login', y='issues_created', title="Top 10 Issue Creators")
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(event_type_data)

st.sidebar.markdown("---")
st.sidebar.caption("BuyPower Data Engineering Internship — Week 9 Capstone By Pozhar")

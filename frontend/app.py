# app.py
from marshmallow import pprint
import streamlit as st
import pandas as pd
import sqlite3
import json
import warnings
import sys
import os
sys.path.append(os.path.abspath(".."))
warnings.filterwarnings("ignore")
# Import the functions, not the variables
from backend.llm_sql_pipeline import get_schema, get_sql_chain, get_final_analysis, parentdir

st.set_page_config(page_title="NBA Stats Analyst")
st.title("🏀 NBA Gemini Data Pipeline")

# Initialize the chain once and store it in Streamlit's session state
if 'nba_chain' not in st.session_state:
    st.session_state.nba_chain = get_sql_chain()
st.markdown("This app allows you to ask question about NBA Stats. It will generate an SQL query based on the question, execute it against the database, and provide an analysis of the results.")
st.markdown("The data available in the database includes REGULAR season player per game stats (2000-2025), draft history, missing seasons, and general player info. Please refrain from asking questions that require data outside of this scope.")
question = st.text_input("Ask about NBA stats:")

if st.button("Analyze") and question:
    with st.spinner("Processing..."):
        # 1. Use the function to get schema
        schema = get_schema()
        
        # 2. Use the chain stored in session state
        generated_sql = st.session_state.nba_chain.invoke({
            "schema": schema, 
            "question": question
        })
        
        # 3. Execute
        conn = sqlite3.connect(os.path.join(parentdir(), "data", "nba_stats.db"))
        df_result = pd.read_sql_query(generated_sql.query, conn)
        conn.close()
        
        # 4. Use the function for the final response
        evidence_json = json.dumps(df_result.to_dict(orient="records"), indent=2)
        analysis = get_final_analysis(question, evidence_json)
        with open(os.path.join(parentdir(), "temp_data", "chat_output.txt"), 'w') as f:
            f.write(analysis)
        st.markdown(analysis)
        
        with open(os.path.join(parentdir(), "data", "db_schema.txt"), "r") as f:
            schema = f.read()
        st.markdown("Database Schema:")
        st.code(schema, language="sql")
        st.markdown("Query:")
        st.code(generated_sql.query, language="sql")

#python -m streamlit run app.py


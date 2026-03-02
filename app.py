# app.py
import streamlit as st
import pandas as pd
import sqlite3
import json
import warnings
warnings.filterwarnings("ignore")
# Import the functions, not the variables
from tester import get_schema, get_sql_chain, get_final_analysis

st.set_page_config(page_title="NBA Stats Analyst")
st.title("🏀 NBA Gemini Data Pipeline")

# Initialize the chain once and store it in Streamlit's session state
if 'nba_chain' not in st.session_state:
    st.session_state.nba_chain = get_sql_chain()

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
        conn = sqlite3.connect("data/nba_stats.db")
        df_result = pd.read_sql_query(generated_sql.query, conn)
        conn.close()
        
        # 4. Use the function for the final response
        evidence_json = json.dumps(df_result.to_dict(orient="records"), indent=2)
        analysis = get_final_analysis(question, evidence_json)
        
        st.markdown(analysis)
        st.markdown(f"Schema used: {schema} ")
        st.markdown(f"Generated SQL: {generated_sql.query}")
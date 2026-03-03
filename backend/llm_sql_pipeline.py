import os
import json
import sqlite3
import pickle
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
import warnings
warnings.filterwarnings("ignore")
# --- 0. SQL Query Template ---
sql_template = """
    You are a SQLite expert. Given the following database schema, write an SQL query that answers the user's question about basketball. Your response should ACCURATELY reflect the column NAMES and STRUCTURE in the schema. do NOT deviate from this
    List of things to keep in mind when generating the SQL query:
    1. Use context clues from the question to infer any filters that should be applied. Example; someone asking about "KD" should be filtered to "Kevin Durant" in the player_name column, as that is his real name and should be used
    2. Output ONLY the SQL code, no preamble.
    3. Stats that the user asks about (points, rebounds, assists, etc.) should be in the form of "pts", "reb", "ast", etc. as they are in the database, and should be assumed to be in their base form (per game, not per 36 minutes) unless otherwise specified. Also, ppg, rpg, apg, spg, bpg should be translated to pts, reb, ast, stl, blk respectively, and the SQL query should be written accordingly. For example, if the user asks "players who averaged at least 20 ppg", the SQL query should filter for pts >= 19.99 (subtracting 0.01 to account for rounding issues in the data), not ppg >= 20.
    4. Use outside information to transform any nicknames or abbreviations into the correct values in the database.
    5. Additionally, seasons should be in the format "2023-24" for the 2024 season, "2013-14" for the 2014 season, etc. the ONLY exception being if asked about draft YEAR, if someone asks for players drafted in 2020, the draft_year column should be filtered to 2020, not 2019-20.
    6. Percentage stats should be in decimal form, so 50% should be 0.5, 45.3% should be 0.453, etc.
    7. Counting stats are all in their per game form, if a user asks for totals on the season, multiply those stats by games played
    8. When matching players between tables, always match on player_id if possible, as that is the most accurate way to match players. Only match on player_name if player_id is not available in both tables.
    9. Use context to infer any necessary JOINS between tables, for example questions about rookies or draft classes should likely involve using the draft_history table, questions about missing seasons should involve using the missing_seasons table, etc. When in doubt, use JOINs to combine tables and get the most comprehensive data possible to answer the question.
    10. IMPORTANT: Always subtract 0.01 from any counting stats, so if the user asks for a specific counting stats, such as "who averaged >=24 ppg", write in the SQL >=23.99 instead of >=24, to account for any rounding issues in the data. Mind this should only be applied to counting stats like points, rebounds, assists, steals and blocks, not percentage stats like shooting percentages, or advanced stats like PER, win shares, etc. 

    Finally, here's the order I'd like you to give me the information
    1. The table name that is most relevant to the user's question, based on the schema and the question. Only output one table name, and it should be the most relevant one to the user's question. This will be used to determine which table to query from.
    2. The SQL query (self explanatory)
    3. The file name for the resulting query output (should match the information you gave, so if you were asked to compare lebron and kd, it would be something like "lebron_kd_comparison")

    Schema:
    {schema}

    Question: {question}

    SQL Query:"""

def parentdir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# --- 1. Data Models ---
class SQLQueryResult(BaseModel):
    table_name: str = Field(description="The name of the table used in the query")
    query: str = Field(description="The executable SQLite query")
    file_name: str = Field(description="The name of the file used in the query")

# --- 2. Configuration & Setup ---
def setup_environment():
    load_dotenv()
    os.environ['LANGCHAIN_TRACING_V2'] = 'true'
    os.environ['LANGCHAIN_ENDPOINT'] = 'https://api.smith.langchain.com'
    os.environ['LANGCHAIN_API_KEY'] = os.getenv("LANGCHAIN_API_KEY")
    os.environ["OPENAI_API_KEY"] = os.getenv("OPEN_AI_API_KEY")

# --- 3. Database Functions ---
def get_schema(db_path=os.path.join(parentdir(), "data", "nba_stats.db")):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
    schema = "\n".join([row[0] for row in cursor.fetchall() if row[0]])
    conn.close()
    with open(os.path.join(parentdir(), "data", "db_schema.txt"), "w") as f:
        f.write(schema)
    return schema
# Add this to your tester.py
def get_sql_chain(sql_template=sql_template):
    setup_environment() # Ensure API keys are loaded
    schema = get_schema()
    
    # Define your template exactly as it was
    sql_template = sql_template
    
    llm = ChatOpenAI(model_name="gpt-5.2", temperature=0)
    structured_llm = llm.with_structured_output(SQLQueryResult)
    sql_prompt = ChatPromptTemplate.from_template(sql_template)
    
    return sql_prompt | structured_llm
def execute_query(query, db_path=os.path.join(parentdir(), "data", "nba_stats.db")):
    print(db_path)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- 4. LLM Operations ---
def generate_sql(question, schema, sql_template=sql_template):
    llm = ChatOpenAI(model_name="gpt-5.2", temperature=0)
    structured_llm = llm.with_structured_output(SQLQueryResult)
    
    # Your full, unabbreviated template
    sql_template =sql_template
    
    sql_prompt = ChatPromptTemplate.from_template(sql_template)
    sql_chain = sql_prompt | structured_llm
    return sql_chain.invoke({"schema": schema, "question": question})

def get_final_analysis(question, evidence_json):
    llm_answer = ChatOpenAI(model_name="gpt-5.2", temperature=0)
    
    SYSTEM_RULES = """You are an NBA stats analyst.
    You MUST answer using ONLY the evidence provided.
    Do NOT use outside knowledge.
    Do NOT guess or invent numbers.

    Keep the answer factual.
    """

    # Your full, unabbreviated prompt
    USER_PROMPT = f"""User Question:
    {{question}}

    Evidence:
    {{evidence_json}}

    Return your answer in EXACTLY this format:

    Short Answer: The short, concise answer to the user's question, based ONLY on the evidence provided.
    Full Dataset: Answer in the form of a table with appropriate rows and columns, based ONLY on the evidence provided. Pretty much, just format the json file in a readable table format, listing EVERY single row NO SKIPPING (IMPORTANT), don't summarize the data. Do NOT use outside knowledge or guess at any numbers. 
    Summary: A comprehensive summary of what the data says in relation to the user's question, based ONLY on the evidence provided. Do NOT use outside knowledge or guess at any numbers.
    Source: NBAstats.com

    IMPORTANT: Before answering "Not Enough Data", check to make sure that the evidence provided is actually insufficient to answer the question. If the data is relevant but just doesn't directly answer the question, you can still use it to provide a short answer and summary, but just say "Not enough data for a comprehensive answer" or something like that in the summary section. Only say "Not enough data" if there is truly no relevant information in the evidence to even partially answer the question.
    """
    
    # We use .replace and .format to handle the double braces needed for f-strings containing literal braces
    final_prompt = USER_PROMPT.format(question=question, evidence_json=evidence_json)

    response = llm_answer.invoke([
        {"role": "system", "content": SYSTEM_RULES},
        {"role": "user", "content": final_prompt}
    ])
    return response.content

# --- 5. Main Execution ---
def main(question):
    setup_environment()
    db_path = os.path.join(parentdir(), "data", "nba_stats.db")
    
    schema = get_schema(db_path)
    question = question
    
    generated_sql = generate_sql(question, schema)
    print(generated_sql)
    df_result = execute_query(generated_sql.query, db_path)
    
    evidence_packet = {
        "question": question,
        "row_count": len(df_result),
        "data": df_result.to_dict(orient="records")
    }
    df = pd.DataFrame(evidence_packet["data"])
    with open(os.path.join(parentdir(), "temp_data", generated_sql.table_name + ".csv"), "wb") as f:
        pickle.dump(evidence_packet, f)
    
    evidence_json = json.dumps(evidence_packet, ensure_ascii=False, indent=2)
    analysis = get_final_analysis(question, evidence_json)
    print(analysis)

if __name__ == "__main__":
    question = "Players drafted in 2024 to average at least 10 ppg in their rookie season"
    main(question)
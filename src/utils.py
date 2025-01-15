from langchain_community.utilities.sql_database import SQLDatabase
from dotenv import load_dotenv
import os
import google.generativeai as genai

load_dotenv()
google_api_key = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=google_api_key)

def initialize_db_connection():
    # Connect to the SQL database 
    db = SQLDatabase.from_uri("sqlite:///data/CBSdatabase.db")
    return db

def get_schema_db(db):
    # Schema of the db
    schema = db.get_table_info()
    return schema

def load_gemini_model(tools=None):
    # Load the Gemini model
    model = genai.GenerativeModel("gemini-1.5-flash", tools=tools)
    return model


def create_prompt(table_ids, all_info_json):
    prompt = [
        f"""
            You are an expert in converting English questions to SQL query! 
            You can order the results by a relevant column to return the most interesting examples in the database.
            Never query for all the columns from a specific table, only ask for the relevant columns given the question.
            Also the sql code should not have ``` in beginning or end and sql word in output.
            You should always put the table identifiers between quotes ('TABLE_IDENTIFIER').

            DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.

            If the question does not seem related to the database, just return "I don't know" as the answer.
            
            For your generated SQL query, you must only use information in the following tables:
            """,
    ]
    
    for table_id in table_ids:
        table_info = all_info_json[table_id]
        
        prompt.append(f"Table identifier: {table_id}\nTable title: {table_info['title']}\nTable summary: {table_info['summary']}\nColumns: {table_info['columns']}\n")
    
    prompt.append("Given the following question, return a SQL query to answer the question:")
    
    print(prompt)
    
    # Join prompt sections into a single string
    formatted_prompt = [''.join(prompt)]
    
    print(formatted_prompt)

    return formatted_prompt


# def load_tables_data(table_ids, all_info_json):
#     tables_data = ["The following tables are likely related to the query:\n"]
#     for table_id in table_ids:
#         table_info = all_info_json[table_id]
        
#         tables_data.append(f"Table identifier: {table_id}\nTable title: {table_info['title']}\nTable summary: {table_info['summary']}\nColumns: {table_info['columns']}\n")
#     return ''.join(tables_data)

def load_tables_data(table_ids, all_info_json):
    # Extract only the necessary parts of the data related to the specified table IDs
    tables_data = {}
    for table_id in table_ids:
        table_info = all_info_json[table_id]
        tables_data[table_id] = {
            "title": table_info.get("title"),
            "summary": table_info.get("summary"),
            "columns": table_info.get("columns")
        }
    return tables_data

def get_tables_output_data(tables_data):
    # Get the output data for the tables
    output_data = []
    for table_id, table_info in tables_data.items():
        output_data.append(f"- {table_id}: {table_info['title']}")
    return '\n'.join(output_data)


def generate_sql_query(model, tables_data, user_question):
    # Generate SQL query
    sql_query = model.generate_content([tables_data, user_question])
    print("GENERATED SQL QUERY: ", sql_query.text)

    return sql_query.text

def run_query(db, sql_query):
    # Run sql query
    return db.run(sql_query)

def chain_query(model, sql_response, user_question):
    # Template
    template = f"""
                Based on the follwing question and the following answer that was obtained using a generated SQL query, write an intuitive answer:
                
                Initial question: {user_question}
                SQL response: {sql_response}
                """
    print(template)
    answer = model.generate_content([template, sql_response]).text

    return answer
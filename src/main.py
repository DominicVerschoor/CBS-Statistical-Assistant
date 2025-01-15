import gradio as gr
from gradio import ChatMessage
from utils import initialize_db_connection, load_tables_data, generate_sql_query, run_query, chain_query, get_tables_output_data
from embeddings import initialize_embedding_model, load_table_embeddings, find_best_matching_tables
import json
from function_calling import function_calling_tools, process_query_with_function_calls
import google.generativeai as genai


# DB connection
db = initialize_db_connection()

function_calling_model = genai.GenerativeModel(
                                model_name="gemini-1.5-flash",
                                system_instruction=f"""
                                                You are an expert in deciding which data from a database is relavant to answer a user question!
                                                We will provide you a list of tables and columns in the database and you have to decide which columns are relevant to answer the user question.
                                                To help you, there are some functions that you can use to obtain additional data such as the descriptions for each of the columns of a table.
                                                You should only use the functions for the tables and columns that are related to the user question.
                                                """,
                                tools=function_calling_tools
                            )

sql_model=genai.GenerativeModel(
                                model_name="gemini-1.5-flash",
                                system_instruction=f"""
                                                You are an expert in converting English questions about a database to SQL query!
                                                You should always answer in SQL code. 
                                                Never query for all the columns from a specific table, only ask for the relevant columns given the question.
                                                The sql code should not have ``` in beginning or end and sql word in output.
                                                You should always put the table identifiers between quotes ('TABLE_IDENTIFIER').
                                                DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
                                                You will be given a list of tables and columns in the database that are likely related to the question, use this information to generate the SQL query.
                                                """,
                                tools=None
                            )

answer_model=genai.GenerativeModel(
                                model_name="gemini-1.5-flash",
                                system_instruction=f"""
                                                You are an expert in converting outputs from a SQL query into an intuitive answers for a question that is asked by a user.
                                                Your task is to generate an answer that is relevant to the question asked by the user and format it in the requisted style (if applicable).
                                                The answer should be concise and relevant to the question asked and should only use the data that is provided to you by the output of the SQL query.
                                                """,
                                tools=None
                            )

# Initialize the embedding model to embed user questions
embedding_model = initialize_embedding_model()

# Load table and column embeddings
table_embeddings = load_table_embeddings()

all_info_json = json.load(open('data/all_info.json'))

def generate_response(user_question, history):
    # Find best matching tables
    best_matching_tables = find_best_matching_tables(user_question, table_embeddings, embedding_model, top_k=3)
    best_matching_table_ids = [table["table_id"] for table in best_matching_tables]
    
    tables_data = load_tables_data(best_matching_table_ids, all_info_json)
    
    print(tables_data)
    
    # Function calling to retrieve extra data
    process_query_with_function_calls(function_calling_model, user_question, json.dumps(tables_data))
    
    # Generate SQL query
    sql_query = generate_sql_query(model=sql_model, tables_data=json.dumps(tables_data), user_question=user_question)

    # Execute SQL query
    query_results = run_query(db=db, sql_query=sql_query)

    # Final answer
    answer = chain_query(model=answer_model, sql_response=query_results, user_question=user_question)
    
    response = [
        ChatMessage(
            role="assistant",
            content=answer,
        )
    ]
    
    response.append(
        ChatMessage(
            role="assistant",
            content=get_tables_output_data(tables_data),
            metadata={"title": "Related tables"}
        )
    )
    
    response.append(
        ChatMessage(
            role="assistant",
            content=sql_query,
            metadata={"title": "SQL query"}
        )
    )
    
    return response


gr.ChatInterface(
    fn=generate_response,
    type="messages",
    chatbot=gr.Chatbot(type="messages"),
    textbox=gr.Textbox(placeholder="Ask a question about the CBS data", container=False, scale=7),
    title="CBS Statistical Assistant",
    description="Ask a question about the CBS data",
    theme="ocean",
    examples=["How many tables are in the database?", "What is the average income of the people in the Netherlands?"],
).launch()
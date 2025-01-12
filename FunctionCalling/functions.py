import config
import json
import requests
import google.generativeai as genai
from google.generativeai.types import FunctionDeclaration, Tool, GenerateContentResponse
from typing import Dict, List


# Load JSON Table Data
with open('FunctionCalling\\all_info.json') as json_file:
    dataset = json.load(json_file)

# Configure Gemini
genai.configure(api_key=config.api_key)

# FunctionDeclarations
get_column_titles = FunctionDeclaration(
    name="get_column_titles",
    description="Fetches all column titles from the specified table.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "The ID of the table to extract column titles from.",
            }
        },
        "required": ["table_id"],
    },
)

get_column_descriptions = FunctionDeclaration(
    name="get_column_descriptions",
    description="Fetches column titles and their descriptions from the specified table.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "The ID of the table to extract column descriptions from.",
            }
        },
        "required": ["table_id"],
    },
)

get_table_summary = FunctionDeclaration(
    name="get_table_summary",
    description="Fetches the summary of the specified table.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "The ID of the table to extract the summary from.",
            }
        },
        "required": ["table_id"],
    },
)

get_specific_column_description = FunctionDeclaration(
    name="get_specific_column_description",
    description="Fetches the description of a specific column from the specified table.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "The ID of the table to fetch the column description from.",
            },
            "column_name": {
                "type": "string",
                "description": "The name of the column to fetch the description of.",
            },
        },
        "required": ["table_id", "column_name"],
    },
)

# Function Definitions !!!! NOT SURE IF ITS NECESSARY !!!!
def fetch_table_by_id(table_id: str):
    """Utility function to fetch a table by its ID."""
    for table in dataset:
        if table.get("table_id") == table_id:
            return table
    raise ValueError(f"Table with ID '{table_id}' not found.")


def handle_get_column_titles(table_id: str):
    table = fetch_table_by_id(table_id)
    return [
        col["TitleColumn"]
        for col in table.get("columns", [])
        if "TitleColumn" in col
    ]


def handle_get_column_descriptions(table_id: str):
    table = fetch_table_by_id(table_id)
    return [
        {"title": col["TitleColumn"], "description": col.get("Description")}
        for col in table.get("columns", [])
    ]


def handle_get_table_summary(table_id: str):
    table = fetch_table_by_id(table_id)
    return table.get("summary", "No summary available.")


def handle_get_specific_column_description(table_id: str, column_name: str):
    table = fetch_table_by_id(table_id)
    for col in table.get("columns", []):
        if col.get("TitleColumn", "").lower() == column_name.lower():
            return col.get("Description", "No description available.")
    return f"Column '{column_name}' not found in table '{table_id}'."


function_calling_tools = Tool(
    function_declarations=[
        get_column_titles,
        get_column_descriptions,
        get_table_summary,
        get_specific_column_description,
      ],
)

def extract_function_calls(response: GenerateContentResponse) -> List[Dict]:
  function_calls: List[Dict] = []
  if response.candidates[0].function_calls:
    for function_call in response.candidates[0].function_calls:
      function_call_dict = {'function_name': function_call.name}
      for key, value in function_call.args.items():
        if isinstance(value, dict):
          first_value_key = next(iter(value))
          arg_value = value[first_value_key]
        else:
          arg_value = value
        function_call_dict['arg_value'] = arg_value

      function_calls.append(function_call_dict)

  return function_calls

model = genai.GenerativeModel("gemini-1.5-flash", tools=function_calling_tools)
chat = model.start_chat()
question = "Can you provide the column titles of the table with id 03753eng?"
response = chat.send_message(question)

print(response)
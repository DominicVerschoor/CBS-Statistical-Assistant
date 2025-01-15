import json
from google.generativeai.types import FunctionDeclaration, Tool, GenerateContentResponse

# Load JSON Table Data
with open('data/all_info.json') as json_file:
    dataset = json.load(json_file)

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

# Function Definitions
def get_column_titles(table_id: str):
    table = dataset[table_id]
    return [
        col["TitleColumn"]
        for col in table.get("columns", [])
        if "TitleColumn" in col
    ]

def get_column_descriptions(table_id: str):
    table = dataset[table_id]
    return [
        {"title": col["TitleColumn"], "description": col.get("Description")}
        for col in table.get("columns", [])
    ]

def get_table_summary(table_id: str):
    table = dataset[table_id]
    return table.get("summary", "No summary available.")

def get_specific_column_description(table_id: str, column_name: str):
    table = dataset[table_id]
    for col in table.get("columns", []):
        if col.get("TitleColumn", "").lower() == column_name.lower():
            return col.get("Description", "No description available.")
    return f"Column '{column_name}' not found in table '{table_id}'."

def execute_function(function_call):
    function_name = function_call.name
    args = function_call.args
    
    if function_name == "get_column_titles":
        return get_column_titles(**args)
    elif function_name == "get_column_descriptions":
        return get_column_descriptions(**args)
    elif function_name == "get_table_summary":
        return get_table_summary(**args)
    elif function_name == "get_specific_column_description":
        return get_specific_column_description(**args)
    else:
        raise ValueError(f"Unknown function name: {function_name}")

function_calling_tools = Tool(
    function_declarations=[
        get_column_titles,
        get_column_descriptions,
        get_table_summary,
        get_specific_column_description,
    ],
)

def process_query_with_function_calls(model, question, tables_data):
    response = model.generate_content(contents=[tables_data, question])
    
    if response.candidates[0].content.parts:
        for part in response.candidates[0].content.parts:
            if "function_call" in part:  # Check if the part includes a function_call
                function_call = part.function_call

                # Execute and process the function call
                function_result = execute_function(function_call)
                result_text = json.dumps(function_result, indent=2)
                follow_up = f"Answer the question \"{question}\" using the following results:\n{result_text}"
                
                # Send the result back to the model
                print(function_call.name)
                response = model.generate_content(follow_up)
    
    return response.candidates[0].content.parts
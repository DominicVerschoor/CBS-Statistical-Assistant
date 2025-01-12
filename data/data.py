import cbsodata
import pandas as pd
import sqlite3
from tqdm import tqdm
from functools import wraps



# CONNECTION

DB_PATH = None

def connection(func):
    """Decorator to manage SQLite connection."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if DB_PATH is None:
            raise ValueError("Database path not set. Use 'set_db_path(path)' to set it.")
        conn = None
        try:
            # Open a connection
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Pass the connection or cursor to the function
            kwargs['conn'] = conn
            kwargs['cursor'] = cursor
            
            # Execute the function
            result = func(*args, **kwargs)
            
            # Commit changes if any
            conn.commit()
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            # Close the connection
            if conn:
                conn.close()
    return wrapper

def set_db_path(path):
    """Set the global database path."""
    global DB_PATH
    DB_PATH = path

def get_db_path():
    """Get the global database path."""
    return DB_PATH

# API FUNCTIONS

def check_language_count(tables):
    eng_count = 0
    ned_count = 0
    # iterate over the list of dictionaries
    for table in tables:
        language = table.get('Language', '')
        if  language.endswith('en'):
            eng_count += 1
        elif language.endswith('nl'):
            ned_count += 1
        else:
            print(table.get('Identifier', ''))
    print(f"Number of 'ENG': {eng_count}")
    print(f"Number of 'NED': {ned_count}")

def return_english_tables(tables):
    eng_tables = []
    for table in tables:
        language = table.get('Language', '')
        if  language.endswith('en'):
            identifier = table.get('Identifier', '')
            eng_tables.append(identifier)
    return eng_tables    

def check_col_names(table, metadata):
    # extracting the column names from the 'table' dataset
    table_columns = table.columns.tolist()

    # extracting the 'Key' values from the 'metadata' dataset
    key_values = metadata['Key'].dropna().tolist()  # Remove NaN values from the 'Key' column

    # finding matches between table columns and key values
    matches = [col for col in table_columns if col in key_values]

    # finding non-matching columns
    non_matches = [col for col in table_columns if col not in key_values]

    # Output results
    print("Matching columns:", matches, len(matches))
    print("Non-matching columns:", non_matches, len(non_matches))

# This function returns the dataframe of the metadata of the tables
def generate_metadata_tables_df(ids, max_count=None):
    metadata_tables_list = []  # List to store DataFrames
    if max_count is not None:
        ids = ids[:max_count]
    for id in ids:
        # Fetch metadata and drop 'ID' column
        new_entry = pd.DataFrame(cbsodata.get_meta(id, name='TableInfos'))
        metadata_tables_list.append(new_entry)  # Add the DataFrame to the list

    # Concatenate all DataFrames at once
    metadata_tables = pd.concat(metadata_tables_list, ignore_index=True).drop(columns=['ID'])
    return metadata_tables

# This function returns the dataframe of the metadata of the columns
def generate_metadata_columns_df(ids, max_count=None):
    metadata_columns_list = []  # List to store DataFrames
    if max_count is not None:
        ids = ids[:max_count]
    for id in ids:
        # Fetch metadata and drop 'ID' column
        new_entry = pd.DataFrame(cbsodata.get_meta(id, name='DataProperties'))
        new_entry['Identifier'] = id # Add the 'Identifier' column
        metadata_columns_list.append(new_entry)  # Add the DataFrame to the list

    # Concatenate all DataFrames at once
    metadata_columns = pd.concat(metadata_columns_list, ignore_index=True).drop(columns=['ID'])
    return metadata_columns



# SQLITE FUNCTIONS

@connection
def upload_tables(ids, size_limit = 2147483645, max_count = None, conn = None, cursor = None): # Size limit is 200 MB
    if max_count is not None:
        ids = ids[:max_count]
    for id in tqdm(ids, desc="Uploading tables", unit="table"):
        table = pd.DataFrame(cbsodata.get_data(id))
        if table.memory_usage().sum() > size_limit:
            print(f"Table {id} is too large to upload.")
            continue
        table.to_sql(id, conn, if_exists='replace', index=False)


# This function returns the names(ids) of the tables in the database
@connection
def get_tables_database(exclude_metadata_tables = True, conn = None, cursor = None):
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(query)

    # Fetch all table names
    table_names = [row[0] for row in cursor.fetchall()]
    if exclude_metadata_tables:
        table_names = [name for name in table_names if 'metadata' not in name]
    return table_names

# This function returns the metadata for the tables in the database. Description column can be included, but it contains a lot of tokens, most of which are not useful.
@connection
def get_all_tables_info(include_description = True, conn = None, cursor = None):
    if include_description:
        query = "SELECT Identifier, Title, Summary, Period, ShortDescription FROM metadata_tables;"
    else:
        query = "SELECT Identifier, Title, Summary, Period FROM metadata_tables;"
    tables_info = pd.read_sql_query(query, conn)
    return tables_info

# This function returns the metadata for the columns of the desired ids
@connection
def get_column_info(ids, conn = None, cursor = None):
    query = "SELECT * FROM metadata_columns WHERE Identifier IN ({});".format(','.join(['?']*len(ids)))
    column_info = pd.read_sql_query(query, conn, params=ids)
    return column_info

# This function retrieves the schema of the table
@connection
def get_table_schema(table_name, conn = None, cursor = None):
    query = "PRAGMA table_info('{}');".format(table_name)
    table_schema = pd.read_sql_query(query, conn)
    return table_schema

def get_database_schema1():
    table_names = get_tables_database()
    schema = {}
    for table_name in table_names:
        table_schema = get_table_schema(table_name)
        schema[table_name] = table_schema
    return schema

@connection
def get_database_schema2(conn = None, cursor = None):
    query = """SELECT name, type, sql
FROM sqlite_master
WHERE type IN ('table', 'view')
ORDER BY name;"""
    schema = pd.read_sql_query(query, conn)
    return schema

from data.data import *

def validate_queries(json_data, database_path):
  # set database path
  set_db_path(database_path)
  
  # initialize lists for valid and invalid entries
  valid_entries = []
  invalid_entries = []
  error_entries = []
  
  for entry in json_data:
    try:
      # execute query
      result = execute_query(entry['sql_query'])
      
      # separate entries 
      if result and len(result) > 0:
        valid_entries.append(entry)
      else:
        invalid_entries.append(entry)
    except Exception as e:
      print(f"error executing query for table {entry['table_id']}: {str(e)}")
      error_entries.append(entry)
      continue
      
  return valid_entries, invalid_entries, error_entries
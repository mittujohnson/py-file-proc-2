import pandas as pd
import io

def process_pipe_delimited_data_flexible(data_string: str, target_columns: list) -> pd.DataFrame:
    """
    Processes pipe-delimited data and populates a pandas DataFrame,
    handling cases where some target columns may be missing in a row.
    Missing columns will be filled with None (which pandas converts to NaN).

    Args:
        data_string (str): A string containing the pipe-delimited data.
                           Each line represents a record.
        target_columns (list): A list of all expected column names for the DataFrame.

    Returns:
        pd.DataFrame: A pandas DataFrame populated with the processed data.
    """
    processed_rows = []
    row_id_counter = 1

    lines = data_string.strip().split('\n')

    for line in lines:
        parts = line.split('|')
        
        # Initialize current_row_data with None for all target columns
        current_row_data = {col: None for col in target_columns}
        
        # Assign the sequential ID
        current_row_data['ID'] = row_id_counter
        row_id_counter += 1

        i = 0
        while i < len(parts):
            key = parts[i]
            
            if key == 'ID':
                i += 2 # Skip file's ID and its value, as we generate our own
            elif key == 'PROCESS_NAME':
                if i + 1 < len(parts):
                    # Only assign if 'PROCESS_NAME' is in our target columns
                    if 'PROCESS_NAME' in target_columns:
                        current_row_data['PROCESS_NAME'] = parts[i+1]
                i += 2
            elif key.startswith('DS'):
                ds_name = key
                if ds_name in target_columns: # Only process if it's a target column
                    if i + 2 < len(parts): # Check if both value and extra part exist
                        value_part = parts[i+1]
                        extra_part = parts[i+2]
                        current_row_data[ds_name] = f"{value_part}-{extra_part}"
                    elif i + 1 < len(parts): # If only value exists
                        current_row_data[ds_name] = parts[i+1]
                    # Else, it remains None as initialized
                i += 3 # Skip key, value, and extra part
            else:
                i += 1 # Unrecognized key, move to next part

        processed_rows.append(current_row_data)

    # Create the DataFrame from the list of dictionaries
    # Explicitly setting columns ensures order and that all target_columns are present
    df = pd.DataFrame(processed_rows, columns=target_columns)
    
    return df

# Your updated pipe-delimited data, including the line with missing DS100 and DS400
pipe_data_string_updated = """
ID|123|PROCESS_NAME|PROCESS-123|DS100|100-123|A123|DS200|200-123|B123|DS300|300-123|C123|DS400|400-123|D123|DS500|500-123|E123
ID|456|PROCESS_NAME|PROCESS-456|DS100|100-456|A456|DS200|200-456|B456|DS300|300-456|C456|DS400|400-456|D456|DS500|500-123|E456
ID|666|PROCESS_NAME|PROCESS-666|DS200|200-666|B456|DS300|300-666|C666|DS500|500-123|E666
"""

# Define the target columns for the DataFrame
target_columns_all = ['ID', 'PROCESS_NAME', 'DS100', 'DS200', 'DS300', 'DS400', 'DS500']

# Process the data and create the DataFrame
df_flexible = process_pipe_delimited_data_flexible(pipe_data_string_updated, target_columns_all)

# Print the resulting DataFrame
print("DataFrame with flexible column handling:")
print(df_flexible.to_string())
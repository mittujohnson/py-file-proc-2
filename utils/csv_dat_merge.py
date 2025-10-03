import csv
from collections import defaultdict
import os

def merge_csv_rows_by_id(input_filepath, output_filepath, id_column_name='id'):
    """
    Merges rows in a CSV file based on a common ID column.
    All other column values are concatenated if non-empty and unique.

    Args:
        input_filepath (str): The path to the input CSV file.
        output_filepath (str): The path to the output CSV file.
        id_column_name (str): The name of the column used as the grouping ID.
    """
    
    # --- Step 1: Read and Group Data ---
    
    merged_data = defaultdict(dict)
    column_names = []
    
    try:
        with open(input_filepath, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            column_names = reader.fieldnames
            
            # Determine the columns to merge (all except the ID column)
            merge_columns = [col for col in column_names if col != id_column_name]
            
            for row in reader:
                row_id = row.get(id_column_name)
                
                if not row_id:
                    print(f"Skipping row due to missing ID: {row}")
                    continue
                
                # Initialize sets for new IDs dynamically if they don't exist
                if row_id not in merged_data:
                    merged_data[row_id][id_column_name] = row_id
                    for col in merge_columns:
                        merged_data[row_id][col] = set()
                
                # Collect non-empty values for merging
                for col in merge_columns:
                    value = row.get(col)
                    if value and str(value).strip():
                        merged_data[row_id][col].add(str(value).strip())
                        
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_filepath}")
        return
    except Exception as e:
        print(f"Error during CSV reading: {e}")
        return

    # --- Step 2: Consolidate and Format ---
    
    final_output = []
    
    for id_key, fields in merged_data.items():
        final_row = {id_column_name: id_key}
        
        for col in merge_columns:
            # Sort and join the unique collected values with a space
            unique_values = sorted(list(fields.get(col, set())))
            final_row[col] = ' '.join(unique_values)
            
        final_output.append(final_row)
        
    # Sort the final output by ID (optional, but good for consistent results)
    final_output.sort(key=lambda x: str(x[id_column_name]))

    # --- Step 3: Write to Output CSV ---
    
    if not column_names:
        print("Error: Input file was empty or missing headers.")
        return

    try:
        with open(output_filepath, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=column_names)
            writer.writeheader()
            writer.writerows(final_output)
            
        print(f"\nSuccessfully merged data and wrote to {output_filepath}")
        print(f"Total merged rows: {len(final_output)}")
        
    except Exception as e:
        print(f"Error during CSV writing: {e}")


# --- Example Execution ---

# 1. Define file paths and input data
INPUT_FILE = 'input_data.csv'
OUTPUT_FILE = 'merged_output.csv'

# Sample data provided by the user
input_csv_data = """id,name,place,hobby
1,bobby,london,fishing
2,martin,,swimming
2,,ny,
3,jolly,,
3,,spain,
3,,abc,biking
4,mike,,
"""

# 2. Write the sample data to the input file
try:
    with open(INPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        f.write(input_csv_data)
    print(f"Created sample input file: {INPUT_FILE}")

    # 3. Call the generic merge function
    merge_csv_rows_by_id(INPUT_FILE, OUTPUT_FILE, id_column_name='id')
    
    # 4. Optional: Print the content of the output file
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            print("\nContent of Merged Output:")
            print(f.read())
            
except Exception as e:
    print(f"An error occurred during execution: {e}")

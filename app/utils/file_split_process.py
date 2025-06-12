import os
import pandas as pd
import psycopg2
from math import ceil

# --- Configuration ---
# IMPORTANT: Replace these with your actual PostgreSQL database credentials and table name.
DB_HOST = "your_db_host"
DB_NAME = "your_db_name"
DB_USER = "your_db_user"
DB_PASSWORD = "your_db_password"
TABLE_NAME = "your_table_name"  # The name of your PostgreSQL table

OUTPUT_FOLDER = "output_csv_files" # The folder where CSV files will be saved

# --- Database Connection Function ---
def get_db_connection():
    """
    Establishes and returns a PostgreSQL database connection.
    Returns None if the connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True # Set autocommit to True for non-transactional queries like COUNT(*)
        print("Successfully connected to the PostgreSQL database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

# --- Get Total Row Count Function ---
def get_total_row_count(conn, table_name):
    """
    Fetches the total number of rows in the specified PostgreSQL table.
    Returns -1 if an error occurs.
    """
    try:
        with conn.cursor() as cur:
            # Using f-string for query, ensure table_name is safe
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            total_rows = cur.fetchone()[0]
            print(f"Total rows in table '{table_name}': {total_rows}")
            return total_rows
    except Exception as e:
        print(f"Error getting total row count from table '{table_name}': {e}")
        return -1

# --- Data Chunk Fetching Function ---
def fetch_data_chunk(conn, table_name, limit, offset):
    """
    Fetches a chunk of data from the specified PostgreSQL table using LIMIT and OFFSET.
    
    Args:
        conn (psycopg2.connection): The database connection object.
        table_name (str): The name of the PostgreSQL table.
        limit (int): The maximum number of rows to retrieve.
        offset (int): The number of rows to skip before starting to return rows.
        
    Returns:
        pd.DataFrame: A Pandas DataFrame containing the fetched chunk of data.
                      Returns an empty DataFrame if no data is found or an error occurs.
    """
    try:
        # Using f-string for query, ensure table_name is safe
        query = f"SELECT * FROM {table_name} ORDER BY 1 OFFSET {offset} LIMIT {limit};"
        # Using ORDER BY 1 (first column) is a common practice to ensure consistent order
        # when using OFFSET/LIMIT. Without an ORDER BY, the order of rows is not guaranteed.
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error fetching data chunk from table '{table_name}' (OFFSET {offset}, LIMIT {limit}): {e}")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Data Splitting and Saving Function ---
def split_and_save_to_csv(conn, total_rows, num_files, output_folder, table_name):
    """
    Splits the data fetched from the table into a specified number of parts
    and saves each part to a CSV file.
    
    This function first creates the output folder if it doesn't exist.
    It then calculates how many rows go into each file.
    For each part, it checks if the corresponding CSV file already exists in the output folder.
    If the file exists, it skips generation; otherwise, it fetches the subset of data
    using LIMIT and OFFSET and saves it to a new CSV.
    
    Args:
        conn (psycopg2.connection): The database connection object.
        total_rows (int): The total number of rows in the table.
        num_files (int): The desired number of output CSV files.
        output_folder (str): The name of the folder to save the CSV files.
        table_name (str): The name of the PostgreSQL table.
    """
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    if not os.path.exists(output_folder):
        print(f"Created output folder: {output_folder}")

    if total_rows == 0:
        print("No data to split. The table is empty.")
        return

    # Calculate approximate rows per file. ceil ensures all rows are included.
    rows_per_file = ceil(total_rows / num_files)
    
    print(f"\n--- Splitting Information ---")
    print(f"Total rows in table: {total_rows}")
    print(f"Desired number of files: {num_files}")
    print(f"Approximately {rows_per_file} rows per file.")
    print(f"-----------------------------")

    current_offset = 0
    for i in range(num_files):
        # Determine the limit for the current chunk
        # It's either the calculated rows_per_file or the remaining rows
        current_limit = rows_per_file
        
        # Construct the full path for the output CSV file
        file_name = os.path.join(output_folder, f"part_{i+1}.csv")

        # Check if the file already exists
        if os.path.exists(file_name):
            print(f"Skipping '{file_name}' as it already exists.")
            current_offset += current_limit # Still update offset for next iteration
        else:
            # Fetch the subset of the DataFrame using LIMIT and OFFSET
            print(f"Fetching data for '{file_name}' (LIMIT {current_limit}, OFFSET {current_offset})...")
            subset_df = fetch_data_chunk(conn, table_name, current_limit, current_offset)
            
            if not subset_df.empty:
                # Save the subset to a CSV file
                # index=False prevents Pandas from writing the DataFrame index as a column
                subset_df.to_csv(file_name, index=False)
                print(f"Generated '{file_name}' with {len(subset_df)} rows.")
            else:
                print(f"No data fetched for '{file_name}' (possible end of table or error).")
            
            # Update the offset for the next chunk
            current_offset += len(subset_df) # Use actual fetched length in case it's less than current_limit

        # If we have processed all rows, break early
        if current_offset >= total_rows:
            break

# --- Main Program Logic ---
def main():
    """
    Main function to orchestrate the database connection, total row count fetching,
    chunked data fetching, and CSV file generation process.
    """
    print("Starting the CSV file generation process from PostgreSQL table...")

    # 1. Get database connection
    conn = get_db_connection()
    if conn is None:
        print("Failed to connect to the database. Please check your credentials and database status. Exiting.")
        return

    try:
        # 2. Get the total number of rows in the table
        total_rows = get_total_row_count(conn, TABLE_NAME)
        if total_rows == -1:
            print(f"Failed to get total row count from table '{TABLE_NAME}'. Exiting.")
            return
        elif total_rows == 0:
            print(f"Table '{TABLE_NAME}' is empty. No CSV files will be generated. Exiting.")
            return

        # 3. Get user input for the number of files
        num_files_input = input("Enter the number of CSV files to generate (default is 15): ")
        try:
            # Convert input to integer, if empty, use default 15
            num_files = int(num_files_input) if num_files_input.strip() else 15
            if num_files <= 0:
                print("Invalid input: Number of files must be a positive integer. Using default (15).")
                num_files = 15
            elif num_files > total_rows:
                # If more files requested than rows, create one file per row
                print(f"Warning: Requested {num_files} files but table has only {total_rows} rows. "
                      f"Setting number of files to {total_rows} (one row per file).")
                num_files = total_rows
        except ValueError:
            print("Invalid input: Not a valid number. Using default number of files (15).")
            num_files = 15

        # 4. Split data and save to CSV files
        split_and_save_to_csv(conn, total_rows, num_files, OUTPUT_FOLDER, TABLE_NAME)

    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
    print("CSV file generation process completed.")

# --- Run the Main Function ---
if __name__ == "__main__":
    main()
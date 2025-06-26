import boto3
import csv
import uuid
from io import StringIO
from pyhive import hive
from thrift.transport import TSocket
import psycopg2 # Import for PostgreSQL
from psycopg2 import Error as PgError # Import for PostgreSQL error handling

# --- S3 Utility Functions ---

def list_csv_files(bucket_name, prefix=''):
    """
    Lists CSV files in an S3 bucket with a given prefix.
    It excludes files that already have the '.updated' suffix.
    
    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The S3 prefix (folder path) to search within.
                      Defaults to an empty string to search the entire bucket.
    
    Returns:
        list: A list of S3 object keys (file paths) for CSV files found.
    """
    s3 = boto3.client('s3')
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    # Iterate through pages of objects to handle large number of files
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                # Check if the object is a CSV and not already marked as updated
                if obj['Key'].endswith('.csv') and not obj['Key'].endswith('.updated'):
                    files.append(obj['Key'])
    return files

def read_csv_from_s3(bucket_name, key):
    """
    Reads a CSV file from S3 and returns its content as a list of dictionaries.
    Each dictionary represents a row, with column headers as keys.
    
    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The S3 object key (file path) of the CSV file.
        
    Returns:
        list: A list of dictionaries, where each dictionary is a row from the CSV.
    """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        # Decode the byte stream to a UTF-8 string
        csv_string = obj['Body'].read().decode('utf-8')
        # Use StringIO to treat the string as a file for csv.DictReader
        return list(csv.DictReader(StringIO(csv_string)))
    except Exception as e:
        print(f"Error reading s3://{bucket_name}/{key}: {e}")
        return []

def rename_s3_object(bucket_name, old_key, new_key):
    """
    Renames an S3 object by copying it to a new key and then deleting the old object.
    
    Args:
        bucket_name (str): The name of the S3 bucket.
        old_key (str): The current S3 object key.
        new_key (str): The desired new S3 object key.
        
    Returns:
        bool: True if the rename was successful, False otherwise.
    """
    s3 = boto3.client('s3')
    try:
        # Copy the object to the new key
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': old_key}, Key=new_key)
        # Delete the old object
        s3.delete_object(Bucket=bucket_name, Key=old_key)
        print(f"Successfully renamed s3://{bucket_name}/{old_key} to s3://{bucket_name}/{new_key}")
        return True
    except Exception as e:
        print(f"Error renaming s3://{bucket_name}/{old_key} to s3://{bucket_name}/{new_key}: {e}")
        return False

# --- Hive Utility Function ---

def update_hive_table_with_staging(hive_connection, main_table_name, staging_table_name, csv_data, common_key_column):
    """
    Updates a Hive table using a temporary staging table and INSERT OVERWRITE.
    This is the recommended approach for batch updates in Hive, especially for
    non-transactional tables or those backed by simple file formats (like CSV).

    The process is:
    1. Create a temporary staging table with schema inferred from CSV data.
    2. Insert the CSV data into this temporary staging table.
    3. Perform an `INSERT OVERWRITE` on the main table. This statement joins
       the main table with the staging table and effectively recreates the main
       table's content, merging new data from the CSV while retaining old data
       not present in the CSV.

    Args:
        hive_connection (pyhive.hive.Connection): An active Hive connection object.
        main_table_name (str): The name of the main Hive table to be updated.
        staging_table_name (str): A unique name for the temporary staging table.
        csv_data (list): A list of dictionaries, representing the CSV data to insert.
        common_key_column (str): The name of the common key column used for joining
                                 between the CSV data and the Hive table.

    Returns:
        bool: True if the Hive update process completed successfully, False otherwise.
    """
    cursor = hive_connection.cursor()

    if not csv_data:
        print("No data provided to update Hive table.")
        return False

    # Get column names from the first row of CSV data
    csv_columns = list(csv_data[0].keys())
    if common_key_column not in csv_columns:
        print(f"Error: Common key column '{common_key_column}' not found in CSV data.")
        return False

    update_successful = False
    try:
        # 1. Create a temporary staging table
        # We assume all CSV columns can be treated as STRING in Hive for simplicity.
        # Adjust data types if specific casting is required for your Hive table.
        create_staging_table_query = f"CREATE TEMPORARY TABLE {staging_table_name} ("
        create_staging_table_query += ", ".join([f"`{col}` STRING" for col in csv_columns])
        create_staging_table_query += ") STORED AS TEXTFILE" # TEXTFILE is easy to insert into
        
        print(f"Creating staging table: {create_staging_table_query}")
        cursor.execute(create_staging_table_query)
        print(f"Staging table '{staging_table_name}' created successfully.")

        # 2. Insert data into the staging table
        # For very large CSVs, consider writing to a temporary S3 location
        # and using `LOAD DATA INPATH` instead of direct `INSERT VALUES`.
        insert_values_list = []
        for row in csv_data:
            # Prepare values for SQL INSERT statement, escaping single quotes
            escaped_values = []
            for col in csv_columns:
                value = str(row.get(col, '')).replace("'", "''") # Escape single quotes
                escaped_values.append(f"'{value}'")
            insert_values_list.append(f"({', '.join(escaped_values)})")

        if insert_values_list:
            # Batch inserts into the staging table. Max 1000 rows per insert to avoid query length limits.
            batch_size = 500
            for i in range(0, len(insert_values_list), batch_size):
                batch_insert_values = insert_values_list[i:i + batch_size]
                insert_staging_data_query = f"INSERT INTO TABLE {staging_table_name} VALUES {', '.join(batch_insert_values)}"
                print(f"Inserting batch {i//batch_size + 1} into staging table...")
                cursor.execute(insert_staging_data_query)
            print(f"All {len(csv_data)} rows inserted into staging table successfully.")
        else:
            print("No data to insert into staging table. Skipping main table update.")
            return False

        # 3. Perform INSERT OVERWRITE to merge data into the main table
        # This query performs a FULL OUTER JOIN to merge existing data with new data.
        # It prioritizes new data from the staging table where common_key_column matches.
        #
        # IMPORTANT: INSERT OVERWRITE will DELETE ALL DATA in the target table (or partition)
        # and then insert the results of the SELECT query. Ensure this is desired behavior.
        #
        # First, retrieve the schema of the main table to ensure correct column selection
        cursor.execute(f"DESCRIBE {main_table_name}")
        main_table_schema = [col[0] for col in cursor.fetchall()]

        select_clauses = []
        for col in main_table_schema:
            if col == common_key_column:
                # Common key is taken from either main table or staging table (they should match)
                select_clauses.append(f"COALESCE(st.`{col}`, mt.`{col}`) AS `{col}`")
            elif col in csv_columns:
                # If column exists in CSV, prioritize data from staging table (new data)
                select_clauses.append(f"COALESCE(st.`{col}`, mt.`{col}`) AS `{col}`")
            else:
                # If column only exists in the main table, keep its existing value
                select_clauses.append(f"mt.`{col}` AS `{col}`")
        
        if not select_clauses:
            print(f"Error: Could not determine select clauses for INSERT OVERWRITE. Check main table schema.")
            return False

        insert_overwrite_query = f"""
        INSERT OVERWRITE TABLE {main_table_name}
        SELECT
            {', '.join(select_clauses)}
        FROM
            {main_table_name} mt
        FULL OUTER JOIN
            {staging_table_name} st
        ON
            mt.`{common_key_column}` = st.`{common_key_column}`;
        """
        
        print(f"Executing INSERT OVERWRITE into main table '{main_table_name}'...")
        cursor.execute(insert_overwrite_query)
        print("Hive table updated successfully using INSERT OVERWRITE staging approach.")
        update_successful = True

    except Exception as e:
        print(f"Error during Hive update process for {main_table_name}: {e}")
        update_successful = False
    finally:
        # Always attempt to clean up the temporary staging table
        try:
            print(f"Attempting to drop staging table '{staging_table_name}'.")
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name}")
            print(f"Staging table '{staging_table_name}' dropped successfully.")
        except Exception as e:
            print(f"Error dropping staging table '{staging_table_name}': {e}")
    
    return update_successful

# --- PostgreSQL Utility Functions ---

def connect_to_postgres(host, port, dbname, user, password):
    """
    Establishes a connection to a PostgreSQL database.
    
    Args:
        host (str): PostgreSQL host.
        port (int): PostgreSQL port.
        dbname (str): Database name.
        user (str): Username.
        password (str): Password.
        
    Returns:
        psycopg2.connection: A PostgreSQL connection object, or None on error.
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print("Successfully connected to PostgreSQL.")
        return conn
    except PgError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def upsert_data_to_postgres(pg_connection, pg_table_name, csv_data, common_key_column):
    """
    Performs an UPSERT (INSERT or UPDATE) operation on a PostgreSQL table.
    This function uses INSERT ... ON CONFLICT (common_key_column) DO UPDATE SET ...
    It requires the common_key_column to have a UNIQUE constraint or be a PRIMARY KEY
    in the target PostgreSQL table for efficient updates.

    Args:
        pg_connection (psycopg2.connection): An active PostgreSQL connection object.
        pg_table_name (str): The name of the PostgreSQL table to be updated.
        csv_data (list): A list of dictionaries, representing the CSV data.
        common_key_column (str): The name of the common key column used for UPSERT.

    Returns:
        bool: True if the UPSERT was successful, False otherwise.
    """
    cursor = pg_connection.cursor()
    
    if not csv_data:
        print("No data provided to update PostgreSQL table.")
        return False

    csv_columns = list(csv_data[0].keys())
    if common_key_column not in csv_columns:
        print(f"Error: Common key column '{common_key_column}' not found in CSV data for PostgreSQL UPSERT.")
        return False

    try:
        # Prepare column names for INSERT and ON CONFLICT DO UPDATE SET
        # Quoting column names to handle potential reserved keywords or special characters
        insert_columns = ", ".join([f'"{col}"' for col in csv_columns])
        
        # Prepare the ON CONFLICT DO UPDATE SET part
        # We update all columns present in CSV data except the common key column used for conflict.
        update_set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in csv_columns if col != common_key_column]
        update_set_clause = ", ".join(update_set_clauses)

        # Placeholder for values (e.g., %s, %s, %s)
        values_placeholders = ", ".join(['%s'] * len(csv_columns))

        # Base UPSERT query string
        # Ensure the common_key_column is a PRIMARY KEY or has a UNIQUE constraint in PostgreSQL
        upsert_query = f"""
        INSERT INTO "{pg_table_name}" ({insert_columns})
        VALUES ({values_placeholders})
        ON CONFLICT ("{common_key_column}") DO UPDATE SET
            {update_set_clause};
        """

        # Prepare data for batch execution
        # Convert list of dictionaries to list of tuples, matching column order in csv_columns
        data_to_insert = []
        for row in csv_data:
            data_to_insert.append(tuple(row.get(col, None) for col in csv_columns)) # Use None for missing values

        # Execute the UPSERT query for each row or in batches using executemany for efficiency
        cursor.executemany(upsert_query, data_to_insert)
        pg_connection.commit() # Commit the transaction
        print(f"Successfully UPSERTed {len(csv_data)} rows into PostgreSQL table '{pg_table_name}'.")
        return True

    except PgError as e:
        pg_connection.rollback() # Rollback on error
        print(f"PostgreSQL Error during UPSERT for table '{pg_table_name}': {e}")
        return False
    except Exception as e:
        pg_connection.rollback() # Rollback on other errors
        print(f"An unexpected error occurred during PostgreSQL UPSERT for table '{pg_table_name}': {e}")
        return False
    finally:
        cursor.close()

# --- Main Application Logic ---

def main():
    # --- Configuration Variables ---
    # !!! IMPORTANT: Replace these placeholder values with your actual S3, Hive, and PostgreSQL details !!!
    S3_BUCKET_NAME = 'your-s3-bucket-name'             
    S3_PREFIX = 'csv_data_to_process/'                 # E.g., 'my_folder/sub_folder/' or '' for root

    HIVE_HOST = 'localhost'                            # E.g., 'your-hiveserver2-host.example.com'
    HIVE_PORT = 10000                                  # Default HiveServer2 port
    HIVE_DATABASE = 'default'                          # E.g., 'your_database_name'
    HIVE_TABLE_NAME = 'your_hive_table'                # The Hive table to be updated
    HIVE_COMMON_KEY_COLUMN = 'id'                      # The column name used to match records (e.g., 'product_id', 'user_uuid')

    PG_HOST = 'localhost'                              # E.g., 'your-postgres-host.example.com'
    PG_PORT = 5432                                     # Default PostgreSQL port
    PG_DATABASE = 'your_pg_database'                   # E.g., 'your_application_db'
    PG_USER = 'your_pg_user'                           # PostgreSQL username
    PG_PASSWORD = 'your_pg_password'                   # PostgreSQL password
    PG_TABLE_NAME = 'your_pg_table'                    # The PostgreSQL table to be updated
    PG_COMMON_KEY_COLUMN = 'id'                        # The column name used to match records (e.g., 'product_id', 'user_uuid')
                                                       # This MUST be a PRIMARY KEY or UNIQUE constraint in your PG table!

    print("--- Starting S3 to Hive and PostgreSQL Data Synchronization Application ---")
    print(f"Configured S3 Bucket: {S3_BUCKET_NAME}, Prefix: '{S3_PREFIX}'")
    print(f"Configured Hive Server: {HIVE_HOST}:{HIVE_PORT}/{HIVE_DATABASE}, Table: {HIVE_TABLE_NAME}")
    print(f"Configured PostgreSQL Server: {PG_HOST}:{PG_PORT}/{PG_DATABASE}, Table: {PG_TABLE_NAME}")
    print(f"Using common key column: '{HIVE_COMMON_KEY_COLUMN}' for Hive updates and '{PG_COMMON_KEY_COLUMN}' for PostgreSQL updates.")

    # 1. List new CSV files in S3
    csv_files = list_csv_files(S3_BUCKET_NAME, S3_PREFIX)

    if not csv_files:
        print("No new CSV files found to process (or all are already marked as '.updated'). Exiting.")
        return

    print(f"Found {len(csv_files)} CSV files to process: {csv_files}")

    # Process each CSV file one by one
    for csv_file_key in csv_files:
        print(f"\n--- Processing file: s3://{S3_BUCKET_NAME}/{csv_file_key} ---")
        
        hive_connection = None # Initialize connection variable
        pg_connection = None   # Initialize connection variable

        try:
            # 2. Read CSV data from S3
            csv_data = read_csv_from_s3(S3_BUCKET_NAME, csv_file_key)
            if not csv_data:
                print(f"Skipping '{csv_file_key}' as it is empty or could not be read.")
                continue
            
            print(f"Successfully read {len(csv_data)} rows from '{csv_file_key}'.")

            # Initialize update status flags
            hive_update_successful = False
            pg_update_successful = False

            # --- Hive Update ---
            try:
                # 'auth='NOSASL' is for no authentication. Adjust based on your HiveServer2 setup (e.g., 'LDAP', 'KERBEROS').
                hive_connection = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, database=HIVE_DATABASE, auth='NOSASL')
                print(f"Successfully connected to HiveServer2 at {HIVE_HOST}:{HIVE_PORT}/{HIVE_DATABASE}.")

                # Generate a unique staging table name for this file to prevent conflicts
                staging_table_name = f"temp_csv_update_stage_{uuid.uuid4().hex[:8]}" 

                # Update Hive table using the staging approach
                hive_update_successful = update_hive_table_with_staging(
                    hive_connection, 
                    HIVE_TABLE_NAME, 
                    staging_table_name, 
                    csv_data, 
                    HIVE_COMMON_KEY_COLUMN
                )
            except TSocket.TTransportException as e:
                print(f"Error: Could not connect to HiveServer2: {e}. Please ensure HiveServer2 is running and host/port are correct.")
            except Exception as e:
                print(f"An error occurred during Hive operations for '{csv_file_key}': {e}")
            finally:
                if hive_connection:
                    try:
                        hive_connection.close()
                        print("Closed Hive connection.")
                    except Exception as e:
                        print(f"Error closing Hive connection: {e}")

            # --- PostgreSQL Update (only if Hive update was successful) ---
            if hive_update_successful:
                try:
                    pg_connection = connect_to_postgres(
                        PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
                    )
                    if pg_connection:
                        pg_update_successful = upsert_data_to_postgres(
                            pg_connection,
                            PG_TABLE_NAME,
                            csv_data,
                            PG_COMMON_KEY_COLUMN
                        )
                    else:
                        print(f"Skipping PostgreSQL update for '{csv_file_key}' due to connection failure.")
                except Exception as e:
                    print(f"An error occurred during PostgreSQL operations for '{csv_file_key}': {e}")
                finally:
                    if pg_connection:
                        try:
                            pg_connection.close()
                            print("Closed PostgreSQL connection.")
                        except Exception as e:
                            print(f"Error closing PostgreSQL connection: {e}")
            else:
                print(f"Skipping PostgreSQL update for '{csv_file_key}' because Hive update failed.")

            # 5. Rename S3 file only if BOTH Hive and PostgreSQL updates were successful
            if hive_update_successful and pg_update_successful:
                new_csv_file_key = csv_file_key + ".updated"
                if rename_s3_object(S3_BUCKET_NAME, csv_file_key, new_csv_file_key):
                    print(f"Successfully processed and marked '{csv_file_key}' as updated.")
                else:
                    print(f"Failed to rename '{csv_file_key}'. Manual intervention may be needed to mark it as processed.")
            else:
                print(f"Updates for '{csv_file_key}' were NOT fully successful (Hive success: {hive_update_successful}, PostgreSQL success: {pg_update_successful}). File not renamed.")

        except Exception as e:
            print(f"An unexpected error occurred while processing S3 file '{csv_file_key}': {e}")

    print("\n--- S3 to Hive and PostgreSQL Data Synchronization Application Finished ---")

if __name__ == "__main__":
    main()
# This is the main entry point for the application.
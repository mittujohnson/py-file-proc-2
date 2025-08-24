import sys
import subprocess
from pyhive import hive

# --- User-defined variables ---
HIVE_HOST = 'your_hive_host'  # e.g., 'localhost' or an IP address
HIVE_PORT = 10000            # Default HiveServer2 port
HIVE_USER = 'your_username'  # Your Hadoop username

# S3 and HDFS paths for the data copy step
S3_SOURCE_PATH = 's3a://your-s3-bucket/path/to/data/'
HDFS_DESTINATION_PATH = '/user/your_username/sales_data_from_s3/'

# --- Main functions ---

def execute_hive_query(cursor, query, step_name):
    """
    Executes a HiveQL query with robust error handling.

    Args:
        cursor: A PyHive cursor object.
        query (str): The HiveQL query to execute.
        step_name (str): A descriptive name for the current step.
    
    Raises:
        Exception: If the query execution fails.
    """
    print(f"--- Starting: {step_name} ---")
    try:
        # Execute the query
        cursor.execute(query)
        print(f"--- Success: {step_name} ---")
    except hive.exc.Error as e:
        print(f"--- FAILED: {step_name} ---")
        print(f"Error during query execution: {e}")
        # Reraise the exception to be caught by the main try-except block
        raise Exception(f"Hive query failed at step: {step_name}")

def run_shell_command(command, step_name):
    """
    Executes a shell command and checks its exit status.

    Args:
        command (str): The shell command to execute.
        step_name (str): A descriptive name for the current step.
    
    Raises:
        Exception: If the command returns a non-zero exit code.
    """
    print(f"--- Starting: {step_name} ---")
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        print(result.stdout)
        print(f"--- Success: {step_name} ---")
    except subprocess.CalledProcessError as e:
        print(f"--- FAILED: {step_name} ---")
        print(f"Command failed with exit code {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise Exception(f"Shell command failed at step: {step_name}")

def run_hive_etl():
    """
    Main function to drive the Hive ETL process.
    """
    conn = None
    try:
        # Step 1: Use distcp to copy data from S3 to HDFS
        distcp_command = f"hadoop distcp {S3_SOURCE_PATH} {HDFS_DESTINATION_PATH}"
        run_shell_command(distcp_command, "Copy Data from S3 to HDFS using distcp")

        # Step 2: Connect to HiveServer2
        print("Connecting to HiveServer2...")
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER)
        cursor = conn.cursor()
        print("Successfully connected to Hive.")
        
        # Step 3: Define the first query: Create the temporary table (CTAS)
        create_temp_table_query = """
        CREATE TEMPORARY TABLE temp_filtered_sales AS
        SELECT
            transaction_id,
            product_id,
            sale_amount
        FROM sales_data
        WHERE
            region = 'North America' AND sale_date >= '2024-01-01'
        """
        execute_hive_query(cursor, create_temp_table_query, "Create Temporary Table")

        # Step 4: Define the second query: Join the temp table and create the final output table
        create_output_table_query = """
        CREATE TABLE final_processed_sales AS
        SELECT
            t1.transaction_id,
            t1.sale_amount,
            t2.product_name,
            t2.category
        FROM temp_filtered_sales t1
        JOIN product_info t2 ON t1.product_id = t2.product_id
        """
        execute_hive_query(cursor, create_output_table_query, "Join and Create Output Table")
        
        print("\nProgram completed successfully!")
        
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        print("Program failed. Exiting with status 1.")
        sys.exit(1)
        
    finally:
        # Step 5: Ensure the connection is closed
        if conn:
            conn.close()
            print("Hive connection closed.")

if __name__ == "__main__":
    run_hive_etl()

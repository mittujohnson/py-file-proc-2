import sys
import subprocess
from pyhive import hive
from datetime import datetime
from config import HIVE_HOST, HIVE_PORT, HIVE_USER, S3_BASE_PATH, HDFS_BASE_PATH, DEFAULT_START_DATE

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
        cursor.execute(query)
        print(f"--- Success: {step_name} ---")
    except hive.exc.Error as e:
        print(f"--- FAILED: {step_name} ---")
        print(f"Error during query execution: {e}")
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

def get_latest_partition_date(cursor):
    """
    Finds the latest partition date in the final output table.
    """
    # This query must be idempotent and must not fail on first run if table is empty.
    # We create the table before querying it to ensure it exists.
    create_output_table_query = """
    CREATE TABLE IF NOT EXISTS final_processed_sales (
        transaction_id STRING,
        sale_amount DOUBLE,
        product_name STRING,
        category STRING
    )
    PARTITIONED BY (sale_date DATE)
    STORED AS PARQUET;
    """
    execute_hive_query(cursor, create_output_table_query, "Create Final Output Table (if not exists)")

    query = "SELECT MAX(sale_date) FROM final_processed_sales"
    execute_hive_query(cursor, query, "Get Latest Partition Date")
    result = cursor.fetchall()
    
    if result and result[0][0]:
        return datetime.strptime(str(result[0][0]), '%Y-%m-%d').date()
    else:
        # If no partitions exist, return the default start date from config
        return datetime.strptime(DEFAULT_START_DATE, '%Y-%m-%d').date()

def process_single_day(cursor, target_date):
    """
    Orchestrates the entire ETL process for a single day.
    """
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"\n--- Processing data for date: {date_str} ---")

    # Step 1: Use distcp to copy data from S3 to HDFS for the specific date
    s3_source_path_daily = f"{S3_BASE_PATH}{date_str}/"
    hdfs_destination_path_daily = f"{HDFS_BASE_PATH}{date_str}/"
    distcp_command = f"hadoop distcp {s3_source_path_daily} {hdfs_destination_path_daily}"
    run_shell_command(distcp_command, f"Copy Data for {date_str}")

    # Step 2: Define the first query: Create the temporary table (CTAS)
    create_temp_table_query = f"""
    CREATE TEMPORARY TABLE temp_filtered_sales AS
    SELECT
        transaction_id,
        product_id,
        sale_amount
    FROM sales_data
    WHERE
        region = 'North America' AND sale_date = '{date_str}'
    """
    execute_hive_query(cursor, create_temp_table_query, f"Create Temporary Table for {date_str}")

    # Step 3: Enable dynamic partitioning and insert data
    execute_hive_query(cursor, "SET hive.exec.dynamic.partition=true", "Enable Dynamic Partitioning")
    execute_hive_query(cursor, "SET hive.exec.dynamic.partition.mode=nonstrict", "Set Dynamic Partition Mode")

    insert_output_table_query = f"""
    INSERT OVERWRITE TABLE final_processed_sales PARTITION(sale_date='{date_str}')
    SELECT
        t1.transaction_id,
        t1.sale_amount,
        t2.product_name,
        t2.category
    FROM temp_filtered_sales t1
    JOIN product_info t2 ON t1.product_id = t2.product_id
    """
    execute_hive_query(cursor, insert_output_table_query, f"Insert data into partitioned table for {date_str}")


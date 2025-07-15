import boto3
import pandas as pd
import io
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_csv_from_s3_to_hive(
    s3_bucket_name: str,
    s3_object_key: str,
    hive_table_name: str,
    hive_database_name: str = "default",
    hive_schema: dict = None, # Optional: {'column1': 'STRING', 'column2': 'INT'}
    delimiter: str = ',',
    header: bool = True,
    hdfs_target_path: str = None, # Required if using LOAD DATA INPATH
    hive_connection_details: dict = None # For PyHive, e.g., {'host': 'localhost', 'port': 10000, 'username': 'hive'}
):
    """
    Reads a CSV file from S3 and prepares it for loading into a Hive table.

    This function outlines the process. For actual execution, you would typically
    upload the file to HDFS and then run HiveQL commands using a Hive client
    (e.g., beeline, PyHive, or Spark).

    Args:
        s3_bucket_name (str): The name of the S3 bucket.
        s3_object_key (str): The key (path) of the CSV file in the S3 bucket.
        hive_table_name (str): The name of the Hive table to load data into.
        hive_database_name (str): The name of the Hive database (default: 'default').
        hive_schema (dict, optional): A dictionary defining the Hive table schema
                                      e.g., {'col_name1': 'STRING', 'col_name2': 'INT'}.
                                      If None, schema inference is attempted (basic).
        delimiter (str): The delimiter used in the CSV file (default: ',').
        header (bool): True if the CSV file has a header row, False otherwise.
        hdfs_target_path (str, optional): The HDFS path where the CSV file will be
                                          uploaded before loading into Hive.
                                          E.g., '/user/hive/warehouse/my_data/input.csv'.
                                          This is crucial for `LOAD DATA INPATH`.
        hive_connection_details (dict, optional): Dictionary containing connection
                                                 details for PyHive (e.g., host, port, username).
                                                 If provided, PyHive will be used to execute HiveQL.
    """
    logging.info(f"Starting process to load '{s3_object_key}' from S3 to Hive table '{hive_table_name}'...")

    # --- Step 1: Read CSV file from S3 ---
    s3 = boto3.client('s3')
    try:
        logging.info(f"Downloading '{s3_object_key}' from S3 bucket '{s3_bucket_name}'...")
        obj = s3.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
        csv_data = obj['Body'].read().decode('utf-8')
        logging.info("CSV data downloaded successfully.")
    except Exception as e:
        logging.error(f"Error reading CSV from S3: {e}")
        return

    # Use pandas to parse the CSV data
    try:
        df = pd.read_csv(io.StringIO(csv_data), sep=delimiter, header=0 if header else None)
        logging.info(f"CSV data loaded into pandas DataFrame. Shape: {df.shape}")
        logging.info(f"DataFrame columns: {df.columns.tolist()}")
    except Exception as e:
        logging.error(f"Error parsing CSV data with pandas: {e}")
        return

    # --- Step 2: Prepare Hive Schema (if not provided) ---
    if hive_schema is None:
        logging.info("Attempting to infer Hive schema from DataFrame dtypes...")
        hive_schema = {}
        for col, dtype in df.dtypes.items():
            # Basic type mapping, extend as needed
            if pd.api.types.is_integer_dtype(dtype):
                hive_schema[col] = 'BIGINT'
            elif pd.api.types.is_float_dtype(dtype):
                hive_schema[col] = 'DOUBLE'
            elif pd.api.types.is_bool_dtype(dtype):
                hive_schema[col] = 'BOOLEAN'
            else: # Default to STRING for objects, dates, etc.
                hive_schema[col] = 'STRING'
        logging.info(f"Inferred Hive schema: {hive_schema}")

    # Construct CREATE TABLE statement
    columns_ddl = ", ".join([f"`{col}` {col_type}" for col, col_type in hive_schema.items()])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{hive_database_name}`.`{hive_table_name}` (
        {columns_ddl}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '{delimiter}'
    STORED AS TEXTFILE;
    """
    logging.info(f"\nHive CREATE TABLE SQL:\n{create_table_sql}")

    # --- Step 3: Load Data into Hive ---
    # This part depends on your Hive setup.
    # Option A: Load data from HDFS (most common for managed tables)
    if hdfs_target_path:
        logging.info("Proceeding with HDFS upload and LOAD DATA INPATH approach.")
        # In a real scenario, you would upload the 'csv_data' (or the original file)
        # to the 'hdfs_target_path' using a library like hdfs (pip install hdfs)
        # or by calling an external HDFS client.

        # Example (conceptual) of uploading to HDFS:
        # from hdfs import InsecureClient
        # client = InsecureClient('http://namenode_host:50070', user='hdfs_user')
        # with client.write(hdfs_target_path, encoding='utf-8') as writer:
        #     writer.write(csv_data)
        # logging.info(f"CSV data conceptually uploaded to HDFS at: {hdfs_target_path}")

        load_data_sql = f"""
        LOAD DATA INPATH '{hdfs_target_path}' INTO TABLE `{hive_database_name}`.`{hive_table_name}`;
        """
        logging.info(f"\nHive LOAD DATA SQL (to be executed manually or via client):\n{load_data_sql}")
        logging.warning("Please ensure the CSV file is uploaded to the specified HDFS_TARGET_PATH "
                        "before executing the LOAD DATA INPATH command.")

    # Option B: Using PyHive to execute SQL (requires HiveServer2 running)
    if hive_connection_details:
        try:
            from pyhive import hive
            logging.info(f"Attempting to connect to HiveServer2 at {hive_connection_details.get('host')}:{hive_connection_details.get('port')}...")
            conn = hive.Connection(
                host=hive_connection_details['host'],
                port=hive_connection_details.get('port', 10000),
                username=hive_connection_details.get('username')
            )
            cursor = conn.cursor()

            # Execute CREATE TABLE
            logging.info("Executing CREATE TABLE IF NOT EXISTS...")
            cursor.execute(create_table_sql)
            logging.info("CREATE TABLE command executed.")

            # For PyHive, direct insertion of large datasets is not efficient.
            # It's better to use Spark or the HDFS + LOAD DATA INPATH method.
            # If the dataset is small, you could insert row by row (highly inefficient)
            # or use INSERT INTO ... VALUES (not ideal for CSVs).
            # The most robust way with PyHive is to ensure the file is in HDFS
            # or an S3 location accessible by Hive and then run LOAD DATA or ALTER TABLE.

            # If you must insert via PyHive for small files, you'd iterate the DataFrame:
            # logging.warning("Direct row-by-row insertion via PyHive is inefficient for large datasets.")
            # insert_sql_template = f"INSERT INTO TABLE `{hive_database_name}`.`{hive_table_name}` VALUES ({', '.join(['%s'] * len(hive_schema))})"
            # for index, row in df.iterrows():
            #     values = tuple(row.values)
            #     cursor.execute(insert_sql_template, values)
            # conn.commit()
            # logging.info("Data conceptually inserted via PyHive (for small datasets).")

            logging.info("PyHive connection established. For large datasets, consider "
                         "uploading to HDFS/S3 and using LOAD DATA INPATH or Spark.")

            cursor.close()
            conn.close()
            logging.info("Hive connection closed.")

        except ImportError:
            logging.error("PyHive library not found. Please install it: pip install pyhive[hive]")
        except Exception as e:
            logging.error(f"Error connecting to or interacting with Hive via PyHive: {e}")

    # Option C: External Table pointing to S3
    logging.info("\n--- Alternative: External Table pointing directly to S3 ---")
    logging.info("If your Hive table is an EXTERNAL table, you just need to ensure the CSV "
                 "file is placed in the correct S3 location that the table points to.")
    external_table_sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `{hive_database_name}`.`{hive_table_name}_external` (
        {columns_ddl}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '{delimiter}'
    STORED AS TEXTFILE
    LOCATION 's3://{s3_bucket_name}/path/to/hive/external/data/'; -- IMPORTANT: This path must match where your CSVs are
    """
    logging.info(f"\nExample External Table SQL:\n{external_table_sql}")
    logging.info("After placing the CSV in the S3 location, run: MSCK REPAIR TABLE "
                 f"`{hive_database_name}`.`{hive_table_name}_external`;")

    logging.info("Process completed. Please execute the generated HiveQL commands manually or via your preferred client.")

# --- Example Usage ---
if __name__ == "__main__":
    # --- Configuration ---
    # Replace with your actual S3 and Hive details
    S3_BUCKET = "your-s3-bucket-name"
    S3_KEY = "path/to/your/data.csv" # e.g., "data/sales/2023/sales_q1.csv"
    HIVE_TABLE = "my_csv_data"
    HIVE_DATABASE = "my_database" # Or "default"

    # Optional: Define your schema explicitly. If not provided, it will be inferred.
    # Make sure column names match your CSV header (if header=True) or are in order.
    CUSTOM_HIVE_SCHEMA = {
        "id": "BIGINT",
        "name": "STRING",
        "value": "DOUBLE",
        "timestamp": "STRING" # Hive has TIMESTAMP type, but often stored as string
    }

    CSV_DELIMITER = ","
    CSV_HAS_HEADER = True

    # Required for LOAD DATA INPATH: The HDFS path where you'll stage the CSV
    HDFS_STAGING_PATH = f"/user/hive/warehouse/{HIVE_DATABASE}.db/{HIVE_TABLE}/input_data.csv"
    # Note: For actual HDFS upload, you'd need the 'hdfs' Python library
    # and HDFS client configured on the machine running this script.

    # Optional: PyHive connection details (if you want to execute SQL directly from Python)
    # Make sure HiveServer2 is running and accessible from where this script runs.
    PYHIVE_CONNECTION = {
        'host': 'localhost', # Replace with your HiveServer2 host
        'port': 10000,       # Default HiveServer2 port
        'username': 'hiveuser' # Replace with your Hive user
    }

    # --- Run the function ---
    load_csv_from_s3_to_hive(
        s3_bucket_name=S3_BUCKET,
        s3_object_key=S3_KEY,
        hive_table_name=HIVE_TABLE,
        hive_database_name=HIVE_DATABASE,
        hive_schema=CUSTOM_HIVE_SCHEMA,
        delimiter=CSV_DELIMITER,
        header=CSV_HAS_HEADER,
        hdfs_target_path=HDFS_STAGING_PATH,
        # hive_connection_details=PYHIVE_CONNECTION # Uncomment to enable PyHive execution
    )

    # --- Mock S3 Data for Testing (if you don't have a real S3 file) ---
    # To test the S3 download part without a real S3 bucket, you can mock boto3:
    # import unittest.mock
    # with unittest.mock.patch('boto3.client') as mock_boto_client:
    #     mock_s3_client = unittest.mock.Mock()
    #     mock_boto_client.return_value = mock_s3_client
    #     mock_s3_client.get_object.return_value = {
    #         'Body': io.BytesIO(b"id,name,value,timestamp\n1,Alice,10.5,2023-01-01\n2,Bob,20.0,2023-01-02")
    #     }
    #     print("\n--- Running with Mock S3 Data ---")
    #     load_csv_from_s3_to_hive(
    #         s3_bucket_name="mock-bucket",
    #         s3_object_key="mock-data.csv",
    #         hive_table_name="mock_table",
    #         hive_database_name="mock_db",
    #         hive_schema=None, # Let it infer
    #         hdfs_target_path="/tmp/mock_data.csv"
    #     )

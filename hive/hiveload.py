import os
from hdfs import InsecureClient
from pyhive import hive # Or from impyla import dbapi as hive

# --- Configuration ---
CSV_FILE_PATH = 'your_existing_data.csv'  # Local path to your CSV file
HDFS_TEMP_PATH = '/user/temp_uploads/your_data_to_load.csv' # Temporary HDFS path for the CSV
HIVE_TABLE_NAME = 'your_existing_hive_table'
HIVE_DATABASE = 'default' # Or your specific Hive database

# HDFS connection details
HDFS_NAMENODE_URL = 'http://localhost:9870' # Replace with your NameNode URL and port

# Hive connection details
HIVE_HOST = 'localhost' # Replace with your HiveServer2 host
HIVE_PORT = 10000 # Replace with your HiveServer2 port
HIVE_USER = 'your_username' # Your user for HiveServer2, if authentication is enabled

# --- 1. Upload CSV to HDFS ---
print(f"\n--- Uploading {CSV_FILE_PATH} to HDFS ({HDFS_TEMP_PATH}) ---")
try:
    client = InsecureClient(HDFS_NAMENODE_URL, user=HIVE_USER)

    # Ensure the HDFS directory for temporary uploads exists
    hdfs_dir = os.path.dirname(HDFS_TEMP_PATH)
    client.makedirs(hdfs_dir, permission=755)
    print(f"HDFS directory {hdfs_dir} ensured.")

    # Upload the file. Use overwrite=True if the file might already exist
    client.upload(HDFS_TEMP_PATH, CSV_FILE_PATH, overwrite=True)
    print(f"Successfully uploaded {CSV_FILE_PATH} to HDFS at {HDFS_TEMP_PATH}")

except Exception as e:
    print(f"Error uploading to HDFS: {e}")
    print("Please ensure HDFS NameNode URL and permissions are correct.")
    exit(1)

# --- 2. Load Data into Existing Hive Table ---
print(f"\n--- Loading data from HDFS to Hive table '{HIVE_TABLE_NAME}' ---")
try:
    conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, database=HIVE_DATABASE)
    cursor = conn.cursor()

    # The LOAD DATA INPATH command
    # - LOCAL is omitted because the source is already in HDFS.
    # - OVERWRITE: If you want to replace all existing data in the table with this new data.
    #              Remove `OVERWRITE` if you want to append data.
    load_data_query = f"""
    LOAD DATA INPATH '{HDFS_TEMP_PATH}' OVERWRITE INTO TABLE {HIVE_TABLE_NAME};
    """
    # If you want to append instead of overwrite, use:
    # load_data_query = f"""
    # LOAD DATA INPATH '{HDFS_TEMP_PATH}' INTO TABLE {HIVE_TABLE_NAME};
    # """

    print("Executing Hive LOAD DATA INPATH query...")
    print(load_data_query)
    cursor.execute(load_data_query)
    print(f"Successfully loaded data from {HDFS_TEMP_PATH} into Hive table '{HIVE_TABLE_NAME}'.")

    # Optional: Verify data by selecting top few rows
    print(f"\n--- Verifying data in {HIVE_TABLE_NAME} (first 5 rows) ---")
    cursor.execute(f"SELECT * FROM {HIVE_TABLE_NAME} LIMIT 5")
    results = cursor.fetchall()
    if results:
        for row in results:
            print(row)
    else:
        print("No data found or table is empty after load. Check your CSV, HDFS path, and table schema.")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error connecting to Hive or executing LOAD DATA: {e}")
    print("Please ensure HiveServer2 is running, and connection details are correct.")
    exit(1)

print("\n--- Script Finished ---")
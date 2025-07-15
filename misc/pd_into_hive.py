import pandas as pd
from pyhive import hive
import os
import s3fs # Required for Pandas to write directly to S3
import boto3 # Underlying AWS SDK for Python

def insert_pandas_df_into_hive_via_s3(
    pandas_df,
    hive_table_name,
    s3_bucket,
    s3_prefix,
    hive_database="default",
    file_format="parquet", # Recommended: 'parquet' or 'orc' for performance
    delimiter=",",         # Only relevant for CSV files
    hive_host="your_hive_server_host", # e.g., "localhost" or an IP address
    hive_port=10000,       # Default HiveServer2 port
    aws_access_key_id=None, # Optional: if not using env vars or IAM roles
    aws_secret_access_key=None # Optional: if not using env vars or IAM roles
):
    """
    Inserts a Pandas DataFrame into a Hive table by:
    1. Saving the DataFrame to an S3 location.
    2. Creating/updating an external Hive table pointing to that S3 location.

    Args:
        pandas_df (pd.DataFrame): The Pandas DataFrame to insert.
        hive_table_name (str): The name of the Hive table.
        s3_bucket (str): The S3 bucket name.
        s3_prefix (str): The S3 prefix (folder path) within the bucket where the data will be stored.
                         e.g., "data/my_app/my_table/"
        hive_database (str): The Hive database name (default: "default").
        file_format (str): The format to save the file in S3 ('csv' or 'parquet').
                           'parquet' is highly recommended for Hive.
        delimiter (str): Delimiter for CSV files (ignored for Parquet).
        hive_host (str): Hostname or IP of the HiveServer2.
        hive_port (int): Port of the HiveServer2.
        aws_access_key_id (str, optional): AWS Access Key ID.
                                           If None, boto3 will look for credentials
                                           in environment variables, ~/.aws/credentials,
                                           or IAM roles.
        aws_secret_access_key (str, optional): AWS Secret Access Key.
                                               If None, boto3 will look for credentials
                                               as above.
    """
    # Construct the full S3 path for the data
    s3_data_file_path = f"s3://{s3_bucket}/{s3_prefix}{hive_table_name}.{file_format}"
    s3_table_location_path = f"s3a://{s3_bucket}/{s3_prefix}" # Hive uses s3a:// scheme

    try:
        # 1. Configure S3FS for Pandas (if using explicit credentials)
        # If using environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # or IAM roles, you don't need to pass credentials explicitly to s3fs.
        # This block is for explicit key/secret.
        if aws_access_key_id and aws_secret_access_key:
            # Create an S3 filesystem object with explicit credentials
            s3 = s3fs.S3FileSystem(
                key=aws_access_key_id,
                secret=aws_secret_access_key
            )
            # Make this the default for pandas operations that use s3fs
            pd.options.io.parquet.engine = 'pyarrow' # or 'fastparquet'
            pd.options.io.parquet.s3 = s3
            pd.options.io.csv.s3 = s3
            print("S3FS configured with explicit AWS credentials.")
        else:
            # Rely on default boto3 credential chain (env vars, config files, IAM roles)
            print("S3FS relying on default AWS credential chain (env vars, config, IAM roles).")
            s3 = s3fs.S3FileSystem() # Initialize without explicit credentials

        # 2. Save Pandas DataFrame to S3
        print(f"Saving Pandas DataFrame to S3: {s3_data_file_path}")
        if file_format == "csv":
            pandas_df.to_csv(s3_data_file_path, index=False, sep=delimiter, encoding='utf-8')
        elif file_format == "parquet":
            pandas_df.to_parquet(s3_data_file_path, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}. Use 'csv' or 'parquet'.")
        print("Pandas DataFrame saved to S3 successfully.")

        # 3. Connect to Hive and execute commands
        print(f"Connecting to Hive at {hive_host}:{hive_port}, database: {hive_database}")
        # For production, consider using a more robust authentication method than NOSASL
        conn = hive.Connection(host=hive_host, port=hive_port, database=hive_database, auth="NOSASL")
        cursor = conn.cursor()
        print("Connected to Hive.")

        # 4. Create or Alter an External Hive Table pointing to the S3 location
        # This SQL dynamically generates the schema based on the Pandas DataFrame dtypes.
        # It's crucial that the Hive table schema matches the data in S3.
        hive_columns = []
        for col in pandas_df.columns:
            hive_type = map_pandas_dtype_to_hive(str(pandas_df[col].dtype))
            hive_columns.append(f"`{col}` {hive_type}") # Use backticks for column names with special chars

        columns_sql = ", ".join(hive_columns)

        create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS `{hive_database}`.`{hive_table_name}` (
            {columns_sql}
        )
        """
        if file_format == "csv":
            create_table_sql += f"""
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '{delimiter}'
            STORED AS TEXTFILE
            LOCATION '{s3_table_location_path}';
            """
        elif file_format == "parquet":
            create_table_sql += f"""
            STORED AS PARQUET
            LOCATION '{s3_table_location_path}';
            """
        else:
            raise ValueError(f"Unsupported file format for Hive table creation: {file_format}")

        print(f"Executing CREATE/ALTER TABLE SQL:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        print(f"Hive table '{hive_database}.{hive_table_name}' created or updated successfully, pointing to S3.")

        # If you add new files to the same S3_PREFIX, Hive should automatically pick them up
        # for external tables. For partitioned tables, you might need:
        # cursor.execute(f"MSCK REPAIR TABLE `{hive_database}`.`{hive_table_name}`;")
        # print("MSCK REPAIR TABLE executed (if applicable).")

    except ImportError as ie:
        print(f"Missing required library: {ie}. Please install it (e.g., pip install {str(ie).split(' ')[-1]}).")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
            print("Hive connection closed.")

def map_pandas_dtype_to_hive(pandas_dtype):
    """Maps Pandas data types to Hive data types."""
    if 'int' in pandas_dtype:
        return 'BIGINT' # Use BIGINT for broader integer range
    elif 'float' in pandas_dtype:
        return 'DOUBLE'
    elif 'bool' in pandas_dtype:
        return 'BOOLEAN'
    elif 'datetime' in pandas_dtype:
        return 'TIMESTAMP'
    elif 'object' in pandas_dtype:
        return 'STRING' # Objects are typically strings in Pandas
    else:
        return 'STRING' # Default for any other unmapped types

# --- Example Usage ---
if __name__ == "__main__":
    # Ensure you have these libraries installed:
    # pip install pandas pyhive[hive] s3fs boto3

    # Create a sample Pandas DataFrame
    data = {'product_id': [101, 102, 103, 104],
            'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor'],
            'price': [1200.50, 25.00, 75.99, 300.00],
            'in_stock': [True, True, False, True],
            'last_updated': pd.to_datetime(['2024-07-01', '2024-07-01', '2024-06-28', '2024-07-02'])}
    df = pd.DataFrame(data)

    # --- Configuration ---
    # IMPORTANT: Replace with your actual S3 bucket and desired prefix
    my_s3_bucket = "your-s3-bucket-name"
    my_s3_prefix = "data/products/" # This will be the "folder" in S3

    # IMPORTANT: Replace with your actual HiveServer2 host and port
    my_hive_host = "localhost" # Or your HiveServer2 IP/hostname
    my_hive_port = 10000

    my_hive_table = "products_data_s3"
    my_hive_database = "default" # Or your specific Hive database

    # Optional: AWS credentials if not using environment variables or IAM roles
    # DO NOT hardcode credentials in production code!
    # aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    # aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_access_key = None # Set to your actual key if needed, otherwise leave None
    aws_secret_key = None # Set to your actual secret if needed, otherwise leave None

    # Call the function
    insert_pandas_df_into_hive_via_s3(
        pandas_df=df,
        hive_table_name=my_hive_table,
        s3_bucket=my_s3_bucket,
        s3_prefix=my_s3_prefix,
        hive_database=my_hive_database,
        file_format="parquet", # Highly recommended
        hive_host=my_hive_host,
        hive_port=my_hive_port,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    print("\n--- Verification Steps ---")
    print(f"1. Check your S3 bucket: s3://{my_s3_bucket}/{my_s3_prefix}{my_hive_table}.parquet")
    print(f"2. In Hive CLI or Beeline, run:")
    print(f"   USE {my_hive_database};")
    print(f"   DESCRIBE FORMATTED {my_hive_table};")
    print(f"   SELECT * FROM {my_hive_table} LIMIT 5;")
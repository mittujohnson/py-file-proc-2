from pyhive import hive
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.TApplicationException import TApplicationException

def create_hive_table_from_list(
    column_definitions,
    table_name,
    hive_host,
    hive_port=10000,
    hive_database="default",
    if_not_exists=True,
    row_format_delimited_by=None,
    stored_as="ORC",
    location=None,
    table_comment=None,
    auth_method="NONE",  # Options: "NONE", "LDAP", "KERBEROS"
    username=None,
    password=None,
    kerberos_service_name="hive"
):
    """
    Creates a Hive table based on a list of column definitions.

    Args:
        column_definitions (list of tuples): A list where each tuple represents a column
                                             and contains (column_name, data_type).
                                             Example: [('id', 'INT'), ('name', 'STRING'), ('age', 'INT')]
        table_name (str): The name of the Hive table to create.
        hive_host (str): The hostname or IP address of the HiveServer2.
        hive_port (int): The port of the HiveServer2 (default: 10000).
        hive_database (str): The database where the table will be created (default: "default").
        if_not_exists (bool): If True, adds 'IF NOT EXISTS' to the CREATE TABLE statement.
                              (default: True).
        row_format_delimited_by (str): The field delimiter for ROW FORMAT DELIMITED.
                                       Example: ',' for CSV. If None, ROW FORMAT DELIMITED is not used.
        stored_as (str): The file format for the table (e.g., "TEXTFILE", "ORC", "PARQUET").
                         (default: "ORC").
        location (str): The HDFS path for the table data. If None, Hive uses its default location.
        table_comment (str): A comment for the table.
        auth_method (str): Authentication method for HiveServer2.
                           Options: "NONE", "LDAP", "KERBEROS".
        username (str, optional): Username for LDAP or other authentication methods.
        password (str, optional): Password for LDAP or other authentication methods.
        kerberos_service_name (str): Kerberos service name for 'KERBEROS' authentication.
    """
    column_str_parts = []
    for col_name, data_type in column_definitions:
        column_str_parts.append(f"{col_name} {data_type}")
    columns_clause = ", ".join(column_str_parts)

    create_table_sql = f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{hive_database}.{table_name} ({columns_clause})"

    if table_comment:
        create_table_sql += f" COMMENT '{table_comment}'"

    if row_format_delimited_by:
        create_table_sql += f" ROW FORMAT DELIMITED FIELDS TERMINATED BY '{row_format_delimited_by}'"

    create_table_sql += f" STORED AS {stored_as}"

    if location:
        create_table_sql += f" LOCATION '{location}'"

    print(f"Generated HiveQL:\n{create_table_sql}\n")

    conn = None
    cursor = None
    try:
        conn = hive.Connection(
            host=hive_host,
            port=hive_port,
            database=hive_database,
            auth=auth_method,
            username=username,
            password=password,
            kerberos_service_name=kerberos_service_name
        )
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print(f"Table '{table_name}' created successfully in database '{hive_database}'.")

    except TApplicationException as e:
        print(f"Hive operation failed: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# --- Example Usage ---

# 1. Define your column names and data types
#    This is a Python list of tuples, where each tuple is (column_name, data_type)
my_column_list = [
    ('user_id', 'BIGINT'),
    ('user_name', 'STRING'),
    ('email', 'STRING'),
    ('registration_date', 'DATE'),
    ('last_login_ts', 'TIMESTAMP'),
    ('is_active', 'BOOLEAN'),
    ('balance', 'DECIMAL(10,2)')
]

# 2. Hive Connection Parameters
HIVE_SERVER_HOST = 'localhost'  # Replace with your HiveServer2 host
HIVE_SERVER_PORT = 10000        # Default HiveServer2 port
HIVE_DATABASE = 'default'       # Replace if you want a specific database
HIVE_AUTH_METHOD = 'NONE'      # Change as per your Hive setup ('NONE', 'LDAP', 'KERBEROS')
HIVE_USERNAME = None            # Set if using LDAP/username auth
HIVE_PASSWORD = None            # Set if using LDAP/password auth
HIVE_KERBEROS_SERVICE_NAME = 'hive' # Set if using Kerberos

# 3. Table specific parameters
TABLE_NAME = 'my_users_table'
TABLE_COMMENT = 'This table stores user information from Python.'
ROW_DELIMITER = ','  # For CSV-like data
STORAGE_FORMAT = 'TEXTFILE' # Or 'ORC', 'PARQUET', etc.
HDFS_LOCATION = '/user/hive/warehouse/my_custom_location/my_users_table' # Optional: specify HDFS path

# Call the function to create the table
create_hive_table_from_list(
    column_definitions=my_column_list,
    table_name=TABLE_NAME,
    hive_host=HIVE_SERVER_HOST,
    hive_port=HIVE_SERVER_PORT,
    hive_database=HIVE_DATABASE,
    if_not_exists=True,
    row_format_delimited_by=ROW_DELIMITER,
    stored_as=STORAGE_FORMAT,
    location=HDFS_LOCATION,
    table_comment=TABLE_COMMENT,
    auth_method=HIVE_AUTH_METHOD,
    username=HIVE_USERNAME,
    password=HIVE_PASSWORD,
    kerberos_service_name=HIVE_KERBEROS_SERVICE_NAME
)

# Example for an ORC table without a delimiter
print("\n--- Creating an ORC table example ---")
orc_column_list = [
    ('product_id', 'INT'),
    ('product_name', 'STRING'),
    ('price', 'DOUBLE')
]
create_hive_table_from_list(
    column_definitions=orc_column_list,
    table_name='products_orc',
    hive_host=HIVE_SERVER_HOST,
    hive_port=HIVE_SERVER_PORT,
    hive_database=HIVE_DATABASE,
    stored_as='ORC',
    table_comment='Product data stored as ORC'
)

import json
import re
import argparse
import sys
import mysql.connector # You may need to change this import based on your database type

# Sample configuration file with multiple database connections
# Replace these values with your actual database and script details
SAMPLE_CONFIG = """
{
    "db_connections": {
        "mysql_dev": {
            "db_type": "mysql",
            "host": "localhost",
            "user": "root",
            "password": "your_password",
            "database": "test_dev_db"
        },
        "mysql_prod": {
            "db_type": "mysql",
            "host": "localhost",
            "user": "prod_user",
            "password": "prod_password",
            "database": "test_prod_db"
        },
        "postgresql_dev": {
            "db_type": "postgresql",
            "host": "localhost",
            "user": "pg_dev_user",
            "password": "pg_dev_password",
            "database": "pg_dev_db"
        }
    }
}
"""

# Sample SQL script file
# Each statement is separated by a semicolon (;)
# Statements can have a label using a comment with a specific format: -- @label: my_label
# The script will stop on the first error.
SAMPLE_SQL_SCRIPT = """
-- @label: Create users table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE
);

-- @label: Insert a new user
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');

-- @label: Select all users
SELECT * FROM users;

-- @label: This statement will cause an error (demonstration)
-- This line is intentionally wrong to show the error handling
-- SELECT name, email FROM non_existent_table;
"""

def load_config(file_path="config.json"):
    """
    Loads the database configuration from a JSON file.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{file_path}' not found.")
        print("Using sample configuration instead.")
        return json.loads(SAMPLE_CONFIG)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in configuration file '{file_path}'.")
        return None

def load_sql_script(file_path):
    """
    Loads and parses the SQL script from a file, splitting it into individual statements.
    Each statement can have an optional label.
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: SQL script file '{file_path}' not found.")
        print("Using sample SQL script instead.")
        content = SAMPLE_SQL_SCRIPT

    # Split the script by semicolon, keeping newlines to handle multiline statements
    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]

    parsed_statements = []
    current_label = "Unlabeled Statement"

    for stmt in statements:
        # Check for a label comment
        label_match = re.match(r"--\s*@label:\s*(.*)", stmt)
        if label_match:
            current_label = label_match.group(1).strip()
            # Remove the label line from the statement itself
            sql_code = re.sub(r"--\s*@label:.*", "", stmt, count=1).strip()
        else:
            sql_code = stmt.strip()
        
        if sql_code:
            parsed_statements.append({'label': current_label, 'sql': sql_code})
            current_label = "Unlabeled Statement" # Reset for the next statement

    return parsed_statements

def handle_show_partitions(cursor):
    """
    Custom function to handle the 'SHOW PARTITIONS' statement.
    It fetches the results, gets the latest partition, and exits if none are found.
    """
    results = cursor.fetchall()
    if not results:
        print("No partitions found. Exiting program.")
        sys.exit(0)
    
    # Assuming the partition name is the first column and can be sorted to find the latest
    latest_partition = sorted(results, key=lambda x: x[0])[-1]
    print(f"Latest partition found: {latest_partition[0]}")
    return latest_partition[0]

def run_script(db_type, script_file):
    """
    Connects to the database and executes the SQL script.
    """
    config = load_config()
    if not config:
        return

    # Get connection details for the specified database type
    if db_type not in config.get('db_connections', {}):
        print(f"Error: Database type '{db_type}' not found in the configuration file.")
        return
        
    db_config_details = config['db_connections'][db_type]
    db_type_name = db_config_details['db_type']
    
    # Remove db_type from the dictionary to avoid passing it to the connector
    db_config_params = {k: v for k, v in db_config_details.items() if k != 'db_type'}

    # Establish database connection based on the type
    if db_type_name == 'mysql':
        try:
            conn = mysql.connector.connect(**db_config_params)
            cursor = conn.cursor()
        except mysql.connector.Error as err:
            print(f"Error: Could not connect to MySQL database '{db_type}'.")
            print(err)
            return
    elif db_type_name == 'postgresql':
        try:
            import psycopg2
            conn = psycopg2.connect(**db_config_params)
            cursor = conn.cursor()
        except ImportError:
            print("Error: PostgreSQL library 'psycopg2' not found. Please install it (pip install psycopg2-binary).")
            return
        except Exception as err:
            print(f"Error: Could not connect to PostgreSQL database '{db_type}'.")
            print(err)
            return
    else:
        print(f"Error: Unsupported database type '{db_type_name}'.")
        return

    statements = load_sql_script(script_file)

    if not statements:
        print("No SQL statements found to execute.")
        return

    print("Starting script execution...")
    
    try:
        for stmt in statements:
            label = stmt['label']
            sql = stmt['sql']
            
            print(f"\n--- Running step: '{label}' ---")
            print(f"SQL: {sql}")

            # Check for specific command to handle it differently
            is_show_partitions = sql.strip().upper().startswith('SHOW PARTITIONS')
            is_select = sql.strip().upper().startswith('SELECT')

            cursor.execute(sql)
            
            if is_show_partitions:
                # Custom handler for SHOW PARTITIONS
                latest_partition = handle_show_partitions(cursor)
                print(f"Successfully processed 'SHOW PARTITIONS'. Latest partition: {latest_partition}")

            elif is_select:
                results = cursor.fetchall()
                if results:
                    print("Result:")
                    for row in results:
                        print(row)
                else:
                    print("No results returned.")
            else:
                # For non-SELECT statements, commit the transaction
                conn.commit()
                print("Statement executed successfully.")
    
    except Exception as e:
        print(f"\n--- Script execution failed on step: '{label}' ---")
        print(f"Error: {e}")
        # Rollback any pending transactions on failure
        if conn and conn.is_connected():
            conn.rollback()
    
    finally:
        # Close the connection
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a SQL script against a specified database.")
    parser.add_argument("db_type", help="The database type to connect to (e.g., 'mysql_dev', 'postgresql_dev').")
    parser.add_argument("script_file", help="The path to the SQL script file to execute.")
    
    args = parser.parse_args()
    
    run_script(args.db_type, args.script_file)
